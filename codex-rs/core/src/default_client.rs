use crate::spawn::CODEX_SANDBOX_ENV_VAR;
use bytes::Bytes;
use http::Error as HttpError;
use http_body_util::BodyExt;
use reqwest::Body;
use reqwest::IntoUrl;
use reqwest::Method;
use reqwest::Response;
use reqwest::header::HeaderName;
use reqwest::header::HeaderValue;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Error as IoError;
use std::io::Write;
use std::path::Path;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::task;

const REQUEST_LOG_PATH: &str = "/tmp/codex-http-requests.log";
const RESPONSE_LOG_PATH: &str = "/tmp/codex-http-responses.log";

/// Set this to add a suffix to the User-Agent string.
///
/// It is not ideal that we're using a global singleton for this.
/// This is primarily designed to differentiate MCP clients from each other.
/// Because there can only be one MCP server per process, it should be safe for this to be a global static.
/// However, future users of this should use this with caution as a result.
/// In addition, we want to be confident that this value is used for ALL clients and doing that requires a
/// lot of wiring and it's easy to miss code paths by doing so.
/// See https://github.com/openai/codex/pull/3388/files for an example of what that would look like.
/// Finally, we want to make sure this is set for ALL mcp clients without needing to know a special env var
/// or having to set data that they already specified in the mcp initialize request somewhere else.
///
/// A space is automatically added between the suffix and the rest of the User-Agent string.
/// The full user agent string is returned from the mcp initialize response.
/// Parenthesis will be added by Codex. This should only specify what goes inside of the parenthesis.
pub static USER_AGENT_SUFFIX: LazyLock<Mutex<Option<String>>> = LazyLock::new(|| Mutex::new(None));
pub const DEFAULT_ORIGINATOR: &str = "codex_cli_rs";
pub const CODEX_INTERNAL_ORIGINATOR_OVERRIDE_ENV_VAR: &str = "CODEX_INTERNAL_ORIGINATOR_OVERRIDE";
static HTTP_LOGGING_ENABLED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug)]
pub struct CodexHttpClient {
    inner: reqwest::Client,
}

pub fn set_http_logging_enabled(enabled: bool) {
    HTTP_LOGGING_ENABLED.store(enabled, Ordering::Relaxed);
}

fn http_logging_enabled() -> bool {
    HTTP_LOGGING_ENABLED.load(Ordering::Relaxed)
}

impl CodexHttpClient {
    fn new(inner: reqwest::Client) -> Self {
        Self { inner }
    }

    pub fn get<U>(&self, url: U) -> CodexRequestBuilder
    where
        U: IntoUrl,
    {
        self.request(Method::GET, url)
    }

    pub fn post<U>(&self, url: U) -> CodexRequestBuilder
    where
        U: IntoUrl,
    {
        self.request(Method::POST, url)
    }

    pub fn request<U>(&self, method: Method, url: U) -> CodexRequestBuilder
    where
        U: IntoUrl,
    {
        let url_str = url.as_str().to_string();
        CodexRequestBuilder::new(
            self.inner.clone(),
            self.inner.request(method.clone(), url),
            method,
            url_str,
        )
    }
}

#[must_use = "requests are not sent unless `send` is awaited"]
#[derive(Debug)]
pub struct CodexRequestBuilder {
    client: reqwest::Client,
    builder: reqwest::RequestBuilder,
    method: Method,
    url: String,
}

impl CodexRequestBuilder {
    fn new(
        client: reqwest::Client,
        builder: reqwest::RequestBuilder,
        method: Method,
        url: String,
    ) -> Self {
        Self {
            client,
            builder,
            method,
            url,
        }
    }

    fn map(self, f: impl FnOnce(reqwest::RequestBuilder) -> reqwest::RequestBuilder) -> Self {
        Self {
            builder: f(self.builder),
            client: self.client,
            method: self.method,
            url: self.url,
        }
    }

    pub fn header<K, V>(self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<HttpError>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<HttpError>,
    {
        self.map(|builder| builder.header(key, value))
    }

    pub fn bearer_auth<T>(self, token: T) -> Self
    where
        T: Display,
    {
        self.map(|builder| builder.bearer_auth(token))
    }

    pub fn json<T>(self, value: &T) -> Self
    where
        T: ?Sized + Serialize,
    {
        self.map(|builder| builder.json(value))
    }

    pub async fn send(self) -> Result<Response, reqwest::Error> {
        let request = self.builder.build()?;

        if http_logging_enabled()
            && let Err(error) = log_request(&request).await
        {
            tracing::warn!(
                method = %self.method,
                url = %self.url,
                error = ?error,
                "Failed to log HTTP request",
            );
        }

        match self.client.execute(request).await {
            Ok(response) => {
                let response = if http_logging_enabled() {
                    match log_response(response).await {
                        Ok(res) => res,
                        Err(error) => {
                            tracing::warn!(
                                method = %self.method,
                                url = %self.url,
                                error = %error,
                                "Failed to log HTTP response",
                            );
                            return Err(error);
                        }
                    }
                } else {
                    response
                };

                let request_ids = Self::extract_request_ids(&response);
                tracing::debug!(
                    method = %self.method,
                    url = %self.url,
                    status = %response.status(),
                    request_ids = ?request_ids,
                    version = ?response.version(),
                    "Request completed",
                );

                Ok(response)
            }
            Err(error) => {
                if http_logging_enabled()
                    && let Err(log_error) =
                        log_response_error(&self.method, &self.url, &error).await
                {
                    tracing::warn!(
                        method = %self.method,
                        url = %self.url,
                        error = ?log_error,
                        "Failed to log HTTP response error",
                    );
                }

                let status = error.status();
                tracing::debug!(
                    method = %self.method,
                    url = %self.url,
                    status = status.map(|s| s.as_u16()),
                    error = %error,
                    "Request failed",
                );
                Err(error)
            }
        }
    }

    fn extract_request_ids(response: &Response) -> HashMap<String, String> {
        ["cf-ray", "x-request-id", "x-oai-request-id"]
            .iter()
            .filter_map(|&name| {
                let header_name = HeaderName::from_static(name);
                let value = response.headers().get(header_name)?;
                let value = value.to_str().ok()?.to_owned();
                Some((name.to_owned(), value))
            })
            .collect()
    }
}

async fn log_request(request: &reqwest::Request) -> std::io::Result<()> {
    let body = request
        .body()
        .and_then(Body::as_bytes)
        .map(format_body_bytes)
        .unwrap_or_else(|| "<body not captured (streaming or multipart)>".to_string());

    let entry = format!(
        "[{timestamp}] {method} {url}\nHeaders:\n{headers}Body:\n{body}\n\n",
        timestamp = log_timestamp(),
        method = request.method(),
        url = request.url(),
        headers = format_headers(request.headers()),
        body = body,
    );

    append_log(Path::new(REQUEST_LOG_PATH), entry).await
}

async fn log_response(mut response: Response) -> Result<Response, reqwest::Error> {
    let url = response.url().clone();
    let status = response.status();
    let version = response.version();
    let headers = response.headers().clone();

    let http_response: http::Response<Body> = response.into();
    let (parts, body) = http_response.into_parts();

    let collected = match body.collect().await {
        Ok(collected) => collected,
        Err(error) => {
            tracing::warn!(url = %url, status = %status, error = %error, "Failed to collect HTTP response body for logging");
            let rebuilt = http::Response::from_parts(parts, Body::from(Bytes::new()));
            return Ok(Response::from(rebuilt));
        }
    };

    let body_bytes = collected.to_bytes();
    let rebuilt = http::Response::from_parts(parts, Body::from(body_bytes.clone()));
    response = Response::from(rebuilt);

    let entry = format!(
        "[{timestamp}] {status} {url} (HTTP/{version:?})\nHeaders:\n{headers}Body:\n{body}\n\n",
        timestamp = log_timestamp(),
        status = status,
        url = url,
        version = version,
        headers = format_headers(&headers),
        body = format_body_bytes(&body_bytes),
    );

    if let Err(error) = append_log(Path::new(RESPONSE_LOG_PATH), entry).await {
        tracing::warn!(url = %url, status = %status, error = ?error, "Failed to log HTTP response");
    }

    Ok(response)
}

async fn log_response_error(
    method: &Method,
    url: &str,
    error: &reqwest::Error,
) -> std::io::Result<()> {
    let entry = format!(
        "[{timestamp}] {method} {url}\nError: {error}\n\n",
        timestamp = log_timestamp(),
        method = method,
        url = url,
        error = error,
    );

    append_log(Path::new(RESPONSE_LOG_PATH), entry).await
}

fn log_timestamp() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

fn format_headers(headers: &reqwest::header::HeaderMap) -> String {
    if headers.is_empty() {
        return "  <none>\n".to_string();
    }

    headers
        .iter()
        .map(|(name, value)| {
            let rendered_value = value
                .to_str()
                .map(std::borrow::ToOwned::to_owned)
                .unwrap_or_else(|_| format!("{value:?}"));
            format!("  {name}: {rendered_value}\n")
        })
        .collect()
}

fn format_body_bytes(body: &[u8]) -> String {
    String::from_utf8_lossy(body).to_string()
}

async fn append_log(path: &Path, entry: String) -> std::io::Result<()> {
    let path = path.to_path_buf();

    task::spawn_blocking(move || -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        file.write_all(entry.as_bytes())?;
        Ok(())
    })
    .await
    .map_err(|error| IoError::other(format!("Failed to join log task: {error}")))?
}
#[derive(Debug, Clone)]
pub struct Originator {
    pub value: String,
    pub header_value: HeaderValue,
}
static ORIGINATOR: OnceLock<Originator> = OnceLock::new();

#[derive(Debug)]
pub enum SetOriginatorError {
    InvalidHeaderValue,
    AlreadyInitialized,
}

fn get_originator_value(provided: Option<String>) -> Originator {
    let value = std::env::var(CODEX_INTERNAL_ORIGINATOR_OVERRIDE_ENV_VAR)
        .ok()
        .or(provided)
        .unwrap_or(DEFAULT_ORIGINATOR.to_string());

    match HeaderValue::from_str(&value) {
        Ok(header_value) => Originator {
            value,
            header_value,
        },
        Err(e) => {
            tracing::error!("Unable to turn originator override {value} into header value: {e}");
            Originator {
                value: DEFAULT_ORIGINATOR.to_string(),
                header_value: HeaderValue::from_static(DEFAULT_ORIGINATOR),
            }
        }
    }
}

pub fn set_default_originator(value: String) -> Result<(), SetOriginatorError> {
    let originator = get_originator_value(Some(value));
    ORIGINATOR
        .set(originator)
        .map_err(|_| SetOriginatorError::AlreadyInitialized)
}

pub fn originator() -> &'static Originator {
    ORIGINATOR.get_or_init(|| get_originator_value(None))
}

pub fn get_codex_user_agent() -> String {
    let build_version = env!("CARGO_PKG_VERSION");
    let os_info = os_info::get();
    let prefix = format!(
        "{}/{build_version} ({} {}; {}) {}",
        originator().value.as_str(),
        os_info.os_type(),
        os_info.version(),
        os_info.architecture().unwrap_or("unknown"),
        crate::terminal::user_agent()
    );
    let suffix = USER_AGENT_SUFFIX
        .lock()
        .ok()
        .and_then(|guard| guard.clone());
    let suffix = suffix
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(String::new, |value| format!(" ({value})"));

    let candidate = format!("{prefix}{suffix}");
    sanitize_user_agent(candidate, &prefix)
}

/// Sanitize the user agent string.
///
/// Invalid characters are replaced with an underscore.
///
/// If the user agent fails to parse, it falls back to fallback and then to ORIGINATOR.
fn sanitize_user_agent(candidate: String, fallback: &str) -> String {
    if HeaderValue::from_str(candidate.as_str()).is_ok() {
        return candidate;
    }

    let sanitized: String = candidate
        .chars()
        .map(|ch| if matches!(ch, ' '..='~') { ch } else { '_' })
        .collect();
    if !sanitized.is_empty() && HeaderValue::from_str(sanitized.as_str()).is_ok() {
        tracing::warn!(
            "Sanitized Codex user agent because provided suffix contained invalid header characters"
        );
        sanitized
    } else if HeaderValue::from_str(fallback).is_ok() {
        tracing::warn!(
            "Falling back to base Codex user agent because provided suffix could not be sanitized"
        );
        fallback.to_string()
    } else {
        tracing::warn!(
            "Falling back to default Codex originator because base user agent string is invalid"
        );
        originator().value.clone()
    }
}

/// Create an HTTP client with default `originator` and `User-Agent` headers set.
pub fn create_client() -> CodexHttpClient {
    let inner = build_reqwest_client();
    CodexHttpClient::new(inner)
}

pub fn build_reqwest_client() -> reqwest::Client {
    use reqwest::header::HeaderMap;

    let mut headers = HeaderMap::new();
    headers.insert("originator", originator().header_value.clone());
    let ua = get_codex_user_agent();

    let mut builder = reqwest::Client::builder()
        // Set UA via dedicated helper to avoid header validation pitfalls
        .user_agent(ua)
        .default_headers(headers);
    if is_sandboxed() {
        builder = builder.no_proxy();
    }

    builder.build().unwrap_or_else(|_| reqwest::Client::new())
}

fn is_sandboxed() -> bool {
    std::env::var(CODEX_SANDBOX_ENV_VAR).as_deref() == Ok("seatbelt")
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_test_support::skip_if_no_network;

    #[test]
    fn test_get_codex_user_agent() {
        let user_agent = get_codex_user_agent();
        assert!(user_agent.starts_with("codex_cli_rs/"));
    }

    #[tokio::test]
    async fn test_create_client_sets_default_headers() {
        skip_if_no_network!();

        use wiremock::Mock;
        use wiremock::MockServer;
        use wiremock::ResponseTemplate;
        use wiremock::matchers::method;
        use wiremock::matchers::path;

        let client = create_client();

        // Spin up a local mock server and capture a request.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let resp = client
            .get(server.uri())
            .send()
            .await
            .expect("failed to send request");
        assert!(resp.status().is_success());

        let requests = server
            .received_requests()
            .await
            .expect("failed to fetch received requests");
        assert!(!requests.is_empty());
        let headers = &requests[0].headers;

        // originator header is set to the provided value
        let originator_header = headers
            .get("originator")
            .expect("originator header missing");
        assert_eq!(originator_header.to_str().unwrap(), "codex_cli_rs");

        // User-Agent matches the computed Codex UA for that originator
        let expected_ua = get_codex_user_agent();
        let ua_header = headers
            .get("user-agent")
            .expect("user-agent header missing");
        assert_eq!(ua_header.to_str().unwrap(), expected_ua);
    }

    #[test]
    fn test_invalid_suffix_is_sanitized() {
        let prefix = "codex_cli_rs/0.0.0";
        let suffix = "bad\rsuffix";

        assert_eq!(
            sanitize_user_agent(format!("{prefix} ({suffix})"), prefix),
            "codex_cli_rs/0.0.0 (bad_suffix)"
        );
    }

    #[test]
    fn test_invalid_suffix_is_sanitized2() {
        let prefix = "codex_cli_rs/0.0.0";
        let suffix = "bad\0suffix";

        assert_eq!(
            sanitize_user_agent(format!("{prefix} ({suffix})"), prefix),
            "codex_cli_rs/0.0.0 (bad_suffix)"
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_macos() {
        use regex_lite::Regex;
        let user_agent = get_codex_user_agent();
        let re = Regex::new(
            r"^codex_cli_rs/\d+\.\d+\.\d+ \(Mac OS \d+\.\d+\.\d+; (x86_64|arm64)\) (\S+)$",
        )
        .unwrap();
        assert!(re.is_match(&user_agent));
    }
}
