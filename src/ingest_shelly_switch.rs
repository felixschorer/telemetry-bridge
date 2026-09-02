use crate::influx;
use crate::influx::Precision;
use crate::line_protocol::Point;
use crate::message::TimestampedMessage;
use crate::topic_pattern::TopicPattern;
use anyhow::{Context, Result, anyhow};
use itertools::Itertools;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio_stream::{Stream, StreamExt};
use tracing::info;

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    pub mqtt_topic: TopicPattern,

    pub influx_bucket: String,
    pub influx_measurement: String,

    #[serde(default = "HashMap::new")]
    pub device_names: HashMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SwitchState {
    output: bool,

    voltage: f64,
    current: f64,

    #[serde(rename = "apower")]
    a_power: f64,

    #[serde(rename = "aenergy")]
    a_energy: AEnergy,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AEnergy {
    total: f64,
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

    let device_type = settings
        .mqtt_topic
        .extract_value(topic, "deviceType")
        .ok_or_else(|| anyhow!("Failed to extract 'deviceType' wildcard from topic."))?;

    point.add_tag("deviceType", device_type);

    let device_id = settings
        .mqtt_topic
        .extract_value(topic, "deviceId")
        .ok_or_else(|| anyhow!("Failed to extract 'deviceId' wildcard from topic."))?;

    if let Some(device_name) = settings.device_names.get(device_id) {
        point.add_tag("deviceName", device_name);
    } else {
        point.add_tag("deviceName", device_id);
    }

    let payload: SwitchState = serde_json::from_slice(&message.publish.payload)
        .context("Failed to parse message payload.")?;

    point
        .add_bool_field("switchedOn", payload.output)
        .add_float_field("power_W", payload.a_power)?
        .add_float_field("voltage_V", payload.voltage)?
        .add_float_field("current_A", payload.current)?
        .add_float_field("totalEnergy_Wh", payload.a_energy.total)?;

    point.set_timestamp(message.received_at.try_into()?);

    Ok(point)
}
