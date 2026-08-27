use crate::command::CommandContext;
use crate::influx;
use crate::influx::{InfluxOptions, Precision};
use crate::line_protocol::Point;
use anyhow::{Context, Result};
use clap::Args;
use rumqttc::v5::mqttbytes::QoS;
use serde::Deserialize;
use tracing::info;

#[derive(Args, Debug)]
pub struct IngestBresserStation {
    #[clap(flatten)]
    pub influx_options: InfluxOptions,

    #[arg(long, env = "TB_INFLUX_BUCKET", default_value = "devices")]
    pub influx_bucket: String,
}

#[derive(Deserialize, Debug)]
struct MessagePayload {
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

pub async fn run(command: IngestBresserStation, mut context: CommandContext) -> Result<()> {
    let influx_client = influx::Client::new(command.influx_options);

    info!("Starting ingestion...");

    context
        .mqtt_client
        .subscribe("rtl433/events/Bresser-6in1/+", QoS::AtMostOnce)
        .await
        .context("Failed to subscribe to topic.")?;

    loop {
        let Some(message) = context.message_rx.recv().await else {
            return Ok(());
        };

        let payload: MessagePayload = serde_json::from_slice(&message.packet.payload)
            .context("Failed to parse message payload.")?;

        let mut point = Point::new("weather_station");

        point
            .add_tag("deviceType", &payload.model)
            .add_tag("deviceName", &payload.id.to_string());

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

        influx_client
            .write(&command.influx_bucket, point, Precision::Millisecond)
            .await
            .context("Failed to write weather station state to Influx.")?;
    }
}
