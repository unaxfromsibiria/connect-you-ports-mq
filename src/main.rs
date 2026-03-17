mod common;
mod stat;
mod client;
mod server;
mod data;

use std::sync::Arc;
use std::time::Duration;

use chrono::{Utc, DateTime};
use rumqttc::{AsyncClient, Event, Incoming};
use tokio::{runtime::Builder, task::JoinSet};
use tokio::sync::RwLock;
use common::{Settings, create_settings, LoadingParams};
use data::{DataHandlerSettings, DataHandler};
use tokio::time::sleep;
use client::{tcp_client_processing, udp_client_processing};
use server::{server_tcp_processing, server_udp_processing};
use log::{info, warn, error, debug};

use crate::stat::{Stat, StatManage};

/// Handles statistics collection and display at regular intervals
async fn handle_stat(settings: &Settings, stat: Arc<RwLock<Stat>>, tasks: &mut JoinSet<()>) {
    let interval = Duration::from_secs(settings.stat_delay as u64);
    let stat = stat.clone();
    tasks.spawn(async move {
        loop {
            sleep(interval).await;
            let r_stat = stat.read().await;
            r_stat.show();
        }
    });
}

/// Monitors service health by publishing and checking messages on MQTT topic
/// Restarts application if connection issues are detected
async fn handle_checking(settings: &Settings, tasks: &mut JoinSet<()>) {
    let (cap, _) = settings.channel_size();
    let connection_name = format!("{}-v", settings.client_name);
    let mqtt_options = settings.make_mqtt_options(&connection_name);
    let settings = settings.clone();
    tasks.spawn(async move {
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, cap * 2);
        let is_s = settings.is_server;
        let topic = format!("val-{}-{}", if is_s {"s"} else {"c"}, settings.client_name);
        let delay = settings.check_time();
        let qos = settings.qos_level();
        let exit_code = 1;
        let mut in_iter_now = Utc::now();
        match client.subscribe(&topic, qos).await {
            Ok(_) => {
                info!("Waiting for messages from '{}' connection {}", topic, connection_name);
            },
            Err(err) => {
                error!("Error subscribing in checker: {}", err);
                sleep(delay).await;
                std::process::exit(exit_code);
            }
        }
        let wait_limit = (delay * settings.delay_rate as u32).as_secs() as i64;
        loop {
            tokio::select! {
                _ = sleep(delay) => {
                    let now = Utc::now();
                    if (now - in_iter_now).num_seconds() > wait_limit {
                        // There is a 'sleeping/waiting state' issue on devices, as everything runs in Docker,
                        // it was decided to restart the application because it's unclear at which stage the connection to
                        // the broker is lost. Client connections may also get stuck and could have gone to sleep as well.
                        error!("Too long lag detected in service {} => {}", in_iter_now, now);
                        break;
                    }
                    let now_st = now.to_rfc3339();
                    let payload = now_st.as_bytes();
                    match client.publish(&topic, qos, false, payload).await {
                        Ok(_) => {
                            in_iter_now = Utc::now();
                            sleep(delay).await;
                        },
                        Err(err) => {
                            error!("Problem sending data in checker service: {}", err);
                            break;
                        }
                    }
                },
                notification = eventloop.poll() => {
                    match notification {
                        Ok(event) => {
                            if let Event::Incoming(Incoming::Publish(publish)) = event {
                                let payload = publish.payload.to_vec();
                                match String::from_utf8(payload) {
                                    Ok(dt_str) => {
                                        match DateTime::parse_from_rfc3339(&dt_str) {
                                            Ok(dt) => {
                                                debug!("Checking topic transfer from {}", dt);
                                                in_iter_now = Utc::now();
                                                sleep(delay).await;
                                            },
                                            Err(err) => {
                                                warn!("Wrong datetime from topic {}: {}", topic, err);
                                            },
                                        }
                                    },
                                    Err(err) => {
                                        warn!("Wrong value from topic {}: {}", topic, err);
                                    }
                                }
                            }
                        },
                        Err(err) => {
                            error!("MQTT event critical error: {}", err);
                            break;
                        },
                    }
                }
            }
        }
        std::process::exit(exit_code);
    });
}

async fn run(settings: &Settings) -> Result<(), Box<dyn std::error::Error>> {
    let is_s = settings.is_server;
    info!("Mode: {} loading: {} buffer size: {}", if is_s {"server"} else {"client"}, settings.loading_level, settings.buffer_size);
    info!("Client: {}", settings.client_name);
    let mut data_handler = DataHandlerSettings::new();
    if !data_handler.setup(&settings) {
        error!("Wrong settings for cipher");
        return Ok(());
    }
    let mut set = JoinSet::new();
    let stat = Stat::new();
    let arc_stat = Arc::new(RwLock::new(stat));
    let copy_arc_stat = arc_stat.clone();

    if is_s {
        server_tcp_processing(&settings, arc_stat.clone(), &mut set).await;
        server_udp_processing(&settings, arc_stat, &mut set).await;
    } else {
        tcp_client_processing(&settings, arc_stat.clone(), &mut set).await;
        udp_client_processing(&settings, arc_stat, &mut set).await;
    }
    handle_checking(&settings, &mut set).await;
    handle_stat(&settings, copy_arc_stat, &mut set).await;

    while let Some(res) = set.join_next().await {
        match res {
            Ok(val) => {
                info!("Task done with result: {:?}", val);
            }
            Err(err) => {
                warn!("A task panicked or was cancelled: {}", err);
            }
        }
    }
    Ok(())
}

fn main() {
    env_logger::init();
    let settings = create_settings();
    let rt = Builder::new_multi_thread().worker_threads(
        settings.workers
    ).enable_all().build().unwrap();
    info!("Tokio thread count: {}", settings.workers);
    let _ = rt.block_on(run(&settings));
}
