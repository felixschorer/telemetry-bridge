use crate::influx;
use crate::influx::Precision;
use crate::line_protocol::Point;
use crate::message::TimestampedMessage;
use crate::topic_pattern::TopicPattern;
use anyhow::{Context, Result, anyhow};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_stream::{Stream, StreamExt};
use tracing::info;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Settings {
    pub mqtt_topic: TopicPattern,

    pub influx_bucket: String,
    pub influx_measurement: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            mqtt_topic: "rtl433/events/+model/+id".parse().unwrap(),
            influx_bucket: "rtl433".to_owned(),
            influx_measurement: "events".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Event {
    time: String,

    protocol: u64,

    freq1: f64,
    freq2: f64,

    rssi: f64,
    snr: f64,
    noise: f64,
}

pub async fn run(
    settings: Settings,
    messages: impl Stream<Item = TimestampedMessage>,
    influx_client: influx::Client,
) -> Result<()> {
    let chunked = messages.chunks_timeout(10, Duration::from_secs(5));

    tokio::pin!(chunked);

    info!("Starting ingestion on topic '{}'.", settings.mqtt_topic);

    while let Some(chunk) = chunked.next().await {
        let points: Vec<Point> = chunk
            .into_iter()
            .map(|message| create_point(&settings, message))
            .try_collect()?;

        influx_client
            .write(&settings.influx_bucket, points, Precision::Millisecond)
            .await
            .context("Failed to write to Influx.")?;
    }

    Ok(())
}

fn create_point(settings: &Settings, message: TimestampedMessage) -> Result<Point> {
    let topic = str::from_utf8(&message.publish.topic).context("Failed to decode topic.")?;

    let mut point = Point::new(&settings.influx_measurement);

    let model = settings
        .mqtt_topic
        .extract_value(topic, "model")
        .ok_or_else(|| anyhow!("Failed to extract 'model' wildcard from topic."))?;

    point.add_tag("model", model);

    let id = settings
        .mqtt_topic
        .extract_value(topic, "id")
        .ok_or_else(|| anyhow!("Failed to extract 'id' wildcard from topic."))?;

    point.add_tag("id", id);

    let payload: Event = serde_json::from_slice(&message.publish.payload)
        .context("Failed to parse message payload.")?;

    point.add_tag("protocol", &payload.protocol.to_string());

    point
        .add_float_field("freq1_MHz", payload.freq1)?
        .add_float_field("freq2_MHz", payload.freq2)?
        .add_float_field("rssi_dB", payload.rssi)?
        .add_float_field("snr_dB", payload.snr)?
        .add_float_field("noise_dB", payload.noise)?;

    let epoch_seconds: f64 = payload.time.parse().context("Failed to parse timestamp.")?;
    point.set_timestamp((epoch_seconds * 1000.0) as i64);

    Ok(point)
}
