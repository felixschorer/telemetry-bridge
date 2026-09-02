extern crate core;

mod influx;
mod ingest_bresser_station;
mod ingest_rtl433_events;
mod ingest_shelly_switch;
mod line_protocol;
mod message;
mod message_router;
mod send_periodic_message;
mod settings;
mod topic_pattern;

use crate::message::TimestampedMessage;
use crate::message_router::{MessageRouter, MessageRouterBuilder};
use crate::settings::{MqttSettings, Settings};
use anyhow::{Context, Result};
use clap::Parser;
use config::{Config, Environment, File};
use rumqttc::Outgoing;
use rumqttc::v5::{AsyncClient, Event, EventLoop, Incoming, MqttOptions};
use std::alloc::System;
use tokio::task::JoinSet;
use tokio::{select, signal};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: System = System;

#[derive(Clone, Debug, Parser)]
#[command(name = "telemetry-bridge", version)]
struct Cli {
    #[arg(short)]
    config_file: Option<String>,

    #[clap(subcommand)]
    command: Option<Command>,
}

#[derive(Clone, Debug, Parser)]
enum Command {
    Init,
    Show,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::try_parse()?;

    match cli.command {
        None => {
            let settings = load_settings(cli.config_file)?;
            run(settings).await?;
        }
        Some(Command::Init) => {
            let settings = Settings::default();
            print!("{}", toml::to_string_pretty(&settings)?);
        }
        Some(Command::Show) => {
            let settings = load_settings(cli.config_file)?;
            print!("{}", toml::to_string_pretty(&settings)?);
        }
    }

    Ok(())
}

async fn run(settings: Settings) -> Result<()> {
    let (mqtt_client, event_loop) = create_mqtt_client(&settings.mqtt);
    let influx_client = influx::Client::try_from(settings.influx)?;

    let mut tasks: JoinSet<Result<()>> = JoinSet::new();
    let token = CancellationToken::new();

    for settings in settings.send_periodic_message {
        let fut = send_periodic_message::run(settings.clone(), mqtt_client.clone(), token.clone());
        tasks.spawn(fut);
    }

    let mut router_builder = MessageRouterBuilder::new(10);

    if let Some(settings) = settings.ingest_shelly_switch {
        let messages = router_builder.add_subscriber(&settings.mqtt_topic);
        let fut = ingest_shelly_switch::run(settings.clone(), messages, influx_client.clone());
        tasks.spawn(fut);
    }

    if let Some(settings) = settings.ingest_bresser_station {
        let messages = router_builder.add_subscriber(&settings.mqtt_topic);
        let fut = ingest_bresser_station::run(settings.clone(), messages, influx_client.clone());
        tasks.spawn(fut);
    }

    if let Some(settings) = settings.ingest_rtl433_events {
        let messages = router_builder.add_subscriber(&settings.mqtt_topic);
        let fut = ingest_rtl433_events::run(settings.clone(), messages, influx_client.clone());
        tasks.spawn(fut);
    }

    let router = router_builder.build();

    info!("Connecting to MQTT as '{}'.", settings.mqtt.client_id);
    tasks.spawn(drive_event_loop(event_loop, router.clone()));

    router
        .subscribe(&mqtt_client)
        .await
        .context("Failed to subscribe to MQTT topics.")?;

    // `handle_shutdown` relies on the `drive_event_loop` task to hold the only
    // reference to the channels contained within the `router`.
    // The ingestion tasks only stop once all references are dropped.
    drop(router);

    handle_shutdown(tasks, mqtt_client, token).await
}

fn load_settings(config_file: Option<String>) -> Result<Settings> {
    let mut config_builder = Config::builder();

    if let Some(file_name) = config_file {
        let file_source = File::with_name(&file_name).required(false);
        config_builder = config_builder.add_source(file_source);
    }

    let config = config_builder
        .add_source(Environment::with_prefix("TB").separator("_"))
        .build()
        .context("Failed to load configuration.")?;

    let settings: Settings = config
        .try_deserialize()
        .context("Failed to parse settings from configuration.")?;

    Ok(settings)
}

fn create_mqtt_client(settings: &MqttSettings) -> (AsyncClient, EventLoop) {
    let mut mqtt_options = MqttOptions::new(&settings.client_id, &settings.host, settings.port);

    mqtt_options
        .set_credentials(&settings.user, &settings.password)
        .set_clean_start(true);

    AsyncClient::new(mqtt_options, 10)
}

async fn drive_event_loop(mut event_loop: EventLoop, router: MessageRouter) -> Result<()> {
    loop {
        let event = event_loop
            .poll()
            .await
            .context("Error polling MQTT connection.")?;

        match event {
            Event::Incoming(Incoming::ConnAck(_)) => {
                info!("Connected to MQTT broker.");
            }
            Event::Incoming(Incoming::Publish(publish)) => {
                let message = TimestampedMessage::at_current_time(publish);
                router.handle_message(message);
            }
            Event::Outgoing(Outgoing::Disconnect) => {
                info!("Disconnecting from MQTT broker...");
            }
            _ => continue,
        }
    }
}

async fn handle_shutdown(
    mut tasks: JoinSet<Result<()>>,
    mqtt_client: AsyncClient,
    token: CancellationToken,
) -> Result<()> {
    let res = select! {
        task_result = tasks.join_next() => {
            match task_result {
                Some(r) => r.context("Failed to join task.")?,
                None => Ok(()),
            }
        },
        _ = shutdown_signal() => Ok(()),
    };

    token.cancel();
    let _ = mqtt_client.disconnect().await;

    while let Some(task_result) = tasks.join_next().await {
        let _ = task_result.context("Failed to join task.")?;
    }

    res
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
            .expect("Failed to install SIGTERM handler.")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, initiating shutdown.");
        }
        _ = terminate => {
            info!("Received SIGTERM, initiating shutdown.");
        }
    }
}
