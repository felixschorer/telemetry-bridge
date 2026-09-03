use crate::topic::Topic;
use anyhow::{Context, Result};
use rumqttc::v5::AsyncClient;
use rumqttc::v5::mqttbytes::QoS;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Settings {
    pub topic: Topic,
    pub message: String,
    pub interval: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            topic: "shellies/command".parse().unwrap(),
            message: "status_update".to_owned(),
            interval: 60,
        }
    }
}

pub async fn run(
    settings: Settings,
    mqtt_client: AsyncClient,
    token: CancellationToken,
) -> Result<()> {
    let duration = Duration::from_secs(settings.interval);

    let mut interval = tokio::time::interval(duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!(
        "Publishing message on topic '{}' every {}s.",
        settings.topic, settings.interval
    );

    loop {
        select! {
            _ = interval.tick() => {},
            _ = token.cancelled() => return Ok(())
        }

        let message = settings.message.clone();
        let topic = settings.topic.clone();

        mqtt_client
            .publish(topic, QoS::AtMostOnce, false, message)
            .await
            .with_context(|| format!("Failed to publish message on topic '{}'.", settings.topic))?;
    }
}
