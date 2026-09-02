use crate::line_protocol::{Point, to_line_protocol};
use crate::settings::InfluxSettings;
use anyhow::{Context, Result, bail};
use reqwest::Client as HttpClient;
use serde::Serialize;
use url::Url;

#[derive(Copy, Clone, Debug, Serialize)]
pub enum Precision {
    #[serde(rename = "s")]
    Second,
    #[serde(rename = "ms")]
    Millisecond,
    #[serde(rename = "us")]
    Microsecond,
    #[serde(rename = "ns")]
    Nanosecond,
}

#[derive(Clone, Debug, Serialize)]
struct WriteOptions<'a> {
    pub org: &'a str,
    pub bucket: &'a str,
    pub precision: Precision,
}

#[derive(Clone, Debug)]
pub struct Client {
    write_url: Url,
    org: String,
    token: String,
    client: HttpClient,
}

impl Client {
    pub async fn write(
        &self,
        bucket: &str,
        points: impl IntoIterator<Item = Point>,
        precision: Precision,
    ) -> Result<()> {
        let options = WriteOptions {
            org: &self.org,
            bucket,
            precision,
        };

        let req = self
            .client
            .post(self.write_url.clone())
            .query(&options)
            .header("Authorization", format!("Token {}", self.token))
            .body(to_line_protocol(points));

        let res = req.send().await.context("Write request failed.")?;

        if !res.status().is_success() {
            bail!("Write API returned status {}.", res.status())
        }

        Ok(())
    }
}

impl TryFrom<InfluxSettings> for Client {
    type Error = url::ParseError;

    fn try_from(options: InfluxSettings) -> Result<Self, Self::Error> {
        Ok(Self {
            write_url: options.url.join("api/v2/write")?,
            org: options.org,
            token: options.token,
            client: HttpClient::new(),
        })
    }
}
