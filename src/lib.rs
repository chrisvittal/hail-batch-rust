use reqwest::header;
use std::{convert::TryFrom, sync::Arc, time::Duration};

pub mod batch;
pub mod billing;
mod util;

const DEFAULT_BATCH_URI: &str = "https://batch.hail.is";

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("{0}")]
    Header(#[from] header::InvalidHeaderValue),
    #[error("{0}")]
    Request(#[from] reqwest::Error),
    #[error("{0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("{0}")]
    Msg(std::borrow::Cow<'static, str>),
    #[error("{request_error} [{extra}]")]
    Service {
        extra: String,
        request_error: reqwest::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Client {
    client: reqwest::Client,
    data: Arc<ClientData>,
}

#[derive(Debug, Clone)]
struct ClientData {
    base_url: reqwest::Url,
    billing_project: Option<String>,
}

pub struct ClientBuilder {
    billing_project: Option<String>,
    base_url: reqwest::Url,
    token: String,
    timeout: Duration,
    headers: header::HeaderMap,
}

impl Client {
    pub fn builder(token: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            token: token.into(),
            ..ClientBuilder::new()
        }
    }

    pub async fn list_billing_projects(&self) -> Result<Vec<billing::Project>> {
        self.get("/api/v1alpha/billing_projects").await
    }

    pub async fn get_billing_project<S: AsRef<str>>(&self, name: S) -> Result<billing::Project> {
        let path = format!("/api/v1alpha/billing_projects/{}", name.as_ref());
        self.get(&path).await
    }

    pub fn url(&self) -> &reqwest::Url {
        &self.data.base_url
    }

    fn join_url(&self, path: &str) -> reqwest::Url {
        let mut url = self.url().clone();
        url.set_path(path);
        url
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.join_url(path);
        let resp = self.client.get(url).send().await?;
        util::handle(resp)
            .await?
            .json()
            .await
            .map_err(Error::Request)
    }

    async fn patch(&self, path: &str) -> Result<reqwest::Response> {
        let url = self.join_url(path);
        util::handle(self.client.patch(url).send().await?).await
    }

    async fn post_json<T>(&self, path: &str, body: &T) -> Result<reqwest::Response>
    where
        T: serde::Serialize + ?Sized,
    {
        let url = self.join_url(path);
        let resp = self.client.post(url).json(body).send().await?;
        util::handle(resp).await
    }

    async fn post<T>(&self, path: &str, content_type: &str, body: T) -> Result<reqwest::Response>
    where
        T: Into<reqwest::Body>,
    {
        let url = self.join_url(path);
        let resp = self
            .client
            .post(url)
            .header(header::CONTENT_TYPE, content_type)
            .body(body)
            .send()
            .await?;
        util::handle(resp).await
    }
}

impl ClientBuilder {
    fn new() -> Self {
        Self {
            billing_project: None,
            base_url: reqwest::Url::parse(DEFAULT_BATCH_URI).unwrap(),
            timeout: Duration::from_secs(60),
            token: String::new(),
            headers: header::HeaderMap::new(),
        }
    }

    pub fn billing_project(mut self, project: impl Into<String>) -> Self {
        self.billing_project = Some(project.into());
        self
    }

    /// Sets the base url used for all api requests.
    ///
    /// # Default Value
    /// `https://batch.hail.is`
    ///
    /// # Notes
    /// The client will set the path when making requests, as such, any path component will be
    /// overridden. This must also be a valid http URI, like all URLs used with reqwest.
    pub fn service_url(mut self, url: impl AsRef<str>) -> Result<Self> {
        self.base_url = reqwest::Url::parse(url.as_ref())?;
        Ok(self)
    }

    /// Sets the request timeout for requests issued by the client
    ///
    /// # Default Value
    /// 60 Seconds
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the Authorization header token
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = token.into();
        self
    }

    /// Sets an arbitrary header to be sent with all requests
    pub fn header<K>(mut self, name: K, value: header::HeaderValue) -> Self
    where
        K: header::IntoHeaderName,
    {
        self.headers.append(name, value);
        self
    }

    pub fn build(mut self) -> Result<Client> {
        let mut token = header::HeaderValue::try_from(format!("Bearer {}", self.token))?;
        token.set_sensitive(true);
        self.headers.insert(header::AUTHORIZATION, token);
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .default_headers(self.headers)
            .build()?;
        Ok(Client {
            client,
            data: Arc::new(ClientData {
                base_url: self.base_url,
                billing_project: self.billing_project,
            }),
        })
    }
}
