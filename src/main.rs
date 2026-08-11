mod command;
mod influx;
mod message;

use crate::command::{Command, CommandContext};
use crate::message::Message;
use anyhow::{Context, Result, bail};
use clap::Parser;
use rumqttc::Outgoing;
use rumqttc::v5::{AsyncClient, Event, EventLoop, Incoming, MqttOptions};
use std::alloc::System;
use tokio::signal;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: System = System;

#[derive(Parser, Debug)]
#[command(name = "telemetry-bridge", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    #[arg(long, env = "TB_MQTT_CLIENT_ID")]
    pub mqtt_client_id: Option<String>,

    #[arg(long, env = "TB_MQTT_HOST")]
    pub mqtt_host: String,

    #[arg(long, env = "TB_MQTT_PORT", default_value = "1883")]
    pub mqtt_port: u16,

    #[arg(long, env = "TB_MQTT_USER")]
    pub mqtt_user: Option<String>,

    #[arg(long, env = "TB_MQTT_PASSWORD")]
    pub mqtt_password: Option<String>,

    #[arg(long, env = "TB_MESSAGE_QUEUE_SIZE", default_value = "100")]
    pub message_queue_size: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let mqtt_client_id = cli
        .mqtt_client_id
        .unwrap_or_else(|| format!("telemetry-bridge-{:4x}", rand::random::<u16>()));

    let mut mqtt_options = MqttOptions::new(&mqtt_client_id, &cli.mqtt_host, cli.mqtt_port);
    if let (Some(user), Some(password)) = (&cli.mqtt_user, &cli.mqtt_password) {
        mqtt_options.set_credentials(user, password);
    }

    info!("Connecting as {}.", mqtt_client_id);

    let (mqtt_client, event_loop) = AsyncClient::new(mqtt_options, 10);

    let (tx, rx) = tokio::sync::mpsc::channel(cli.message_queue_size);
    let mut driver = tokio::spawn(drive_event_loop(event_loop, tx));

    tokio::select! {
        driver_res = &mut driver => {
            driver_res?.context("MQTT connection lost.")?;
            warn!("MQTT driver exited unexpectedly.");
            Ok(())
        }
        cmd_res = command::run(cli.command, CommandContext::new(mqtt_client.clone(), rx)) => {
            warn!("Command exited unexpectedly.");
            mqtt_client.disconnect().await?;
            let _ = driver.await?;
            cmd_res
        }
        _ = shutdown_signal() => {
            mqtt_client.disconnect().await?;
            let _ = driver.await?;
            Ok(())
        }
    }
}

async fn drive_event_loop(mut event_loop: EventLoop, message_tx: Sender<Message>) -> Result<()> {
    loop {
        match event_loop.poll().await? {
            Event::Incoming(Incoming::ConnAck(_)) => {
                info!("Connected to MQTT broker.");
            }
            Event::Incoming(Incoming::Publish(packet)) => {
                let message = Message::with_current_timestamp(packet);

                match message_tx.try_send(message) {
                    Ok(_) => continue,
                    Err(TrySendError::Full(_)) => warn!("Dropping message. Channel is full."),
                    Err(TrySendError::Closed(_)) => bail!("Channel is closed."),
                }
            }
            Event::Outgoing(Outgoing::Disconnect) => {
                info!("Disconnecting from MQTT broker...");
            }
            _ => continue,
        }
    }
}

// Source: https://oneuptime.com/blog/post/2026-01-07-rust-graceful-shutdown/view
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler.");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, initiating shutdown.");
        }
        _ = terminate => {
            info!("Received SIGTERM, initiating shutdown.");
        }
    }
}
