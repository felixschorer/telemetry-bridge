use anyhow::{Context, Result, bail};
use clap::Args;
use reqwest::{Client as HttpClient, Url};
use serde::Serialize;
use std::collections::BTreeMap;

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
            .into_line()
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

#[derive(Serialize, Debug)]
pub enum Precision {
    #[expect(unused)]
    #[serde(rename = "s")]
    Second,

    #[serde(rename = "ms")]
    Millisecond,

    #[expect(unused)]
    #[serde(rename = "us")]
    Microsecond,

    #[expect(unused)]
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

#[derive(Clone, Debug)]
pub struct Point {
    measurement: String,
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, String>,
    timestamp: Option<i64>,
}

impl Point {
    pub fn new(measurement: &str) -> Self {
        Self {
            measurement: escape_measurement(measurement),
            tags: BTreeMap::new(),
            fields: BTreeMap::new(),
            timestamp: None,
        }
    }

    pub fn add_tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.tags.insert(escape_key(key), escape_key(value));
        self
    }

    #[expect(unused)]
    pub fn add_str_field(&mut self, key: &str, value: &str) -> &mut Self {
        self.fields.insert(escape_key(key), encode_str_value(value));
        self
    }

    #[expect(unused)]
    pub fn add_int_field(&mut self, key: &str, value: i64) -> &mut Self {
        self.fields.insert(escape_key(key), format!("{}i", value));
        self
    }

    pub fn add_float_field(&mut self, key: &str, value: f64) -> &mut Self {
        self.fields.insert(escape_key(key), format!("{}", value));
        self
    }

    pub fn add_bool_field(&mut self, key: &str, value: bool) -> &mut Self {
        self.fields.insert(escape_key(key), format!("{}", value));
        self
    }

    pub fn set_timestamp(&mut self, timestamp: i64) -> &mut Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn into_line(mut self) -> Result<String> {
        let mut line = String::new();

        line.push_str(&self.measurement);

        for (key, value) in self.tags {
            line.push(',');
            line.push_str(&key);
            line.push('=');
            line.push_str(&value);
        }

        line.push(' ');

        let Some((key, value)) = self.fields.pop_first() else {
            bail!("Expected point to have at least one field.");
        };

        line.push_str(&key);
        line.push('=');
        line.push_str(&value);

        for (key, value) in self.fields {
            line.push(',');
            line.push_str(&key);
            line.push('=');
            line.push_str(&value);
        }

        if let Some(timestamp) = self.timestamp {
            line.push(' ');
            line.push_str(&timestamp.to_string());
        }

        Ok(line)
    }
}

fn escape_measurement(key: &str) -> String {
    let mut escaped = String::with_capacity(key.len());
    for char in key.chars() {
        match char {
            ',' => escaped.push_str("\\,"),
            ' ' => escaped.push_str("\\ "),
            // New line signals the start of the next measurement.
            // InfluxDB v2 parser does not handle the escape sequence and
            // will just include the literal escape sequence in the measurement.
            '\n' => escaped.push_str("\\n"),
            // For consistency: include escape sequence of common non-printable characters in the measurement.
            '\t' => escaped.push_str("\\t"),
            '\r' => escaped.push_str("\\r"),
            // InfluxDB v2 parser handles backslashes improperly. They cannot be escaped!
            // Unescaped backslashes work unless they are followed by a character which needs escaping,
            // or are the last character in the measurement.
            // Trying to escape a backslash will just include two backslashes in the measurement
            // and all the same gotchas of a single backslash apply.
            // Therefore, we drop them.
            '\\' => (),
            _ => escaped.push(char),
        }
    }
    escaped
}

fn escape_key(key: &str) -> String {
    let mut escaped = String::with_capacity(key.len());
    for char in key.chars() {
        match char {
            ',' => escaped.push_str("\\,"),
            ' ' => escaped.push_str("\\ "),
            '=' => escaped.push_str("\\="),
            // New line signals the start of the next measurement.
            // InfluxDB v2 parser does not handle the escape sequence and
            // will just include the literal escape sequence in the measurement.
            '\n' => escaped.push_str("\\n"),
            // For consistency: include escape sequence of common non-printable characters in the measurement.
            '\t' => escaped.push_str("\\t"),
            '\r' => escaped.push_str("\\r"),
            // InfluxDB v2 parser handles backslashes improperly. They cannot be escaped!
            // Unescaped backslashes work unless they are followed by a character which needs escaping,
            // or are the last character in the measurement.
            // Trying to escape a backslash will just include two backslashes in the measurement
            // and all the same gotchas of a single backslash apply.
            // Therefore, we drop them.
            '\\' => (),
            _ => escaped.push(char),
        }
    }
    escaped
}

fn encode_str_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    encoded.push('"');

    for char in value.chars() {
        match char {
            '\\' => encoded.push_str("\\\\"),
            '"' => encoded.push_str("\\\""),
            _ => encoded.push(char),
        }
    }

    encoded.push('"');
    encoded
}
