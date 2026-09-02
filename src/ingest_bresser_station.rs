use crate::influx;
use crate::influx::Precision;
use crate::line_protocol::Point;
use crate::message::TimestampedMessage;
use crate::topic_pattern::TopicPattern;
use anyhow::{Context, Result};
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
    pub device_names: HashMap<u64, String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WeatherData {
    time: String,

    id: u64,
    model: String,

    #[serde(rename = "temperature_C")]
    temperature_c: Option<f64>,

    humidity: Option<f64>,

    wind_avg_m_s: Option<f64>,
    wind_max_m_s: Option<f64>,
    wind_dir_deg: Option<f64>,

    rain_mm: Option<f64>,

    uvi: Option<f64>,
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
    let payload: WeatherData = serde_json::from_slice(&message.publish.payload)
        .context("Failed to parse message payload.")?;

    let mut point = Point::new(&settings.influx_measurement);

    point.add_tag("deviceType", &payload.model);

    if let Some(device_name) = settings.device_names.get(&payload.id) {
        point.add_tag("deviceName", device_name);
    } else {
        point.add_tag("deviceName", &payload.id.to_string());
    }

    if let Some(temperature) = payload.temperature_c {
        point.add_float_field("temperature_C", temperature)?;
    }

    if let Some(humidity) = payload.humidity {
        point.add_float_field("humidity_%rel", humidity)?;
    }

    if let Some(wind_avg) = payload.wind_avg_m_s {
        point.add_float_field("windAvg_km/h", wind_avg * 3.6)?;
    }

    if let Some(wind_max) = payload.wind_max_m_s {
        point.add_float_field("windMax_km/h", wind_max * 3.6)?;
    }

    if let Some(wind_dir) = payload.wind_dir_deg {
        point.add_float_field("windDirection_deg", wind_dir)?;
    }

    if let Some(rain) = payload.rain_mm {
        point.add_float_field("totalRain_mm", rain)?;
    }

    if let Some(uv_index) = payload.uvi {
        point.add_float_field("uvIndex", uv_index)?;
    }

    let epoch_seconds: f64 = payload.time.parse().context("Failed to parse timestamp.")?;
    point.set_timestamp((epoch_seconds * 1000.0) as i64);

    Ok(point)
}
