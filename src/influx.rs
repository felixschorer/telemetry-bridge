use crate::line_protocol::Point;
use anyhow::{Context, Result, bail};
use clap::Args;
use reqwest::{Client as HttpClient, Url};
use serde::Serialize;

#[derive(Args, Debug)]
pub struct InfluxOptions {
    #[arg(long, env = "TB_INFLUX_URL")]
    pub influx_url: Url,

    #[arg(long, env = "TB_INFLUX_ORG")]
    pub influx_org: String,

    #[arg(long, env = "TB_INFLUX_TOKEN")]
    pub influx_token: String,
}

#[derive(Clone, Debug)]
pub struct Client {
    base_url: Url,
    org: String,
    token: String,
    client: HttpClient,
}

impl Client {
    pub fn new(options: InfluxOptions) -> Self {
        Self {
            base_url: options.influx_url,
            org: options.influx_org,
            token: options.influx_token,
            client: HttpClient::new(),
        }
    }

    pub async fn write(&self, bucket: &str, point: Point, precision: Precision) -> Result<()> {
        let options = WriteOptions::new(&self.org, bucket, precision);

        let url = self
            .base_url
            .join("api/v2/write")
            .context("Constructing Influx endpoint url failed.")?;

        let line = point
            .to_line()
            .context("Serializing point to line protocol failed.")?;

        let req = self
            .client
            .post(url)
            .query(&options)
            .header("Authorization", format!("Token {}", self.token))
            .body(line);

        let res = req.send().await.context("Write request failed.")?;

        if !res.status().is_success() {
            bail!("Write API returned status {}.", res.status())
        }

        Ok(())
    }
}

#[expect(unused)]
#[derive(Serialize, Debug)]
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
#[derive(Serialize, Debug)]
struct WriteOptions<'a> {
    pub org: &'a str,
    pub bucket: &'a str,
    pub precision: Precision,
}

impl<'a> WriteOptions<'a> {
    pub fn new(org: &'a str, bucket: &'a str, precision: Precision) -> Self {
        Self {
            org,
            bucket,
            precision,
        }
    }
}
