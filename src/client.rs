use crate::common;
use crate::data::DataMessageFormater;
use crate::stat;
use crate::data;

use common::{
    code_name,
    fast_name,
    Settings,
    LoadingParams,
    RoutingManager,
    RoutingState,
    clear_routing,
};
use data::{client_data_topic, server_data_topic, DataHandlerSettings, DataHandler, DataChunk};
use stat::{Stat, StatManage};
use std::time::Duration;
use std::sync::Arc;
use std::collections::HashMap;
use futures::StreamExt;
use paho_mqtt as mqtt;
use log::{info, warn, error, debug};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio::net::{TcpListener, TcpStream, UdpSocket};

/// Processes service transport for TCP connections or UDP transfering
async fn processing_service_transport(
    is_tcp: bool,
    settings: Settings,
    stat: Arc<RwLock<Stat>>,
    service_code: String,
    service_name: String,
    mut in_data_channel: mpsc::Receiver<(String, Vec<u8>)>,
    routing_arc: Arc<RwLock<RoutingState>>,
) {
    let serv = format!("{} ({})", service_name, service_code);
    info!("Starting {} service {}", if is_tcp {"TCP"} else {"UDP"}, serv);
    let cap = settings.stream_capacity();
    let send_interval = settings.collect_message_timeout();
    let warn_size = settings.chunk_size_warning();
    let mut data_handler = DataHandlerSettings::new();
    if !data_handler.setup(&settings) {
        error!("Wrong settings for cipher");
        return;
    }
    let connection_name = format!(
        "{}-{}-{}", settings.client_name, if is_tcp {"t"} else {"u"}, service_code
    );
    let (create_opts, conn_opts) = settings.make_mqtt_options(&connection_name);
    let mut chunk: DataChunk = DataChunk::new();
    let service_delay = settings.service_delay();
    let topic = server_data_topic(is_tcp, &service_code);
    let server_topic = client_data_topic(is_tcp, &service_code);
    let qos = settings.qos_level();
    let mut client = mqtt::AsyncClient::new(create_opts).expect("Error creating the async MQTT client");

    match client.connect(conn_opts).await {
        Ok(_) => {
            info!("MQTT client connected");
        },
        Err(err) => {
            error!("MQTT connection error: {}", err);
            return;
        }
    }
    match client.subscribe(&server_topic, qos).await {
        Ok(_) => {
            info!("Waiting for messages from '{}' in {} connection {}", server_topic, serv, connection_name);
        },
        Err(err) => {
            error!("Error subscribing to {}: {}", serv, err);
            let mut stat_update = stat.write().await;
            stat_update.add_error("connection", &service_name, 1);
            return;
        }
    }
    let (mut raw_bytes, mut msg_bytes, mut error_count) = (0, 0, 0);
    let mut stream = client.get_stream(cap);

    loop {
        tokio::select! {
            Some((client_id, data)) = in_data_channel.recv() => {
                let msg = if data.is_empty() {
                    data_handler.make_quit_message(&service_code, &client_id)
                } else {
                    data_handler.make_data_message(&data, &service_code, &client_id)
                };
                chunk.set.push(msg);
            },
            _ = sleep(send_interval) => {
                if chunk.set.is_empty() {
                    sleep(send_interval).await;
                    continue;
                }
                raw_bytes = chunk.data_size();
                let chunk_size = chunk.len();
                chunk.e = "".to_string();
                if chunk_size > warn_size {
                    warn!("Too many messages in chunk with {} messages in service {}", chunk_size, serv);
                    loop {
                        if chunk.len() <= warn_size {
                            break;
                        }
                        let part_chunk = chunk.extract_slice(warn_size);
                        let payload = part_chunk.dump();
                        let n = payload.len();
                        let out_msg = mqtt::Message::new(&topic, payload, qos);
                        match client.publish(out_msg).await {
                            Ok(_) => {
                                msg_bytes += n;
                                debug!("sending to '{}'", topic);
                            },
                            Err(err) => {
                                error!("Problem sending data in {}: {}", serv, err);
                                error_count += 1;
                            }
                        }
                        sleep(service_delay).await;
                    }
                }
                let payload = chunk.dump();
                let n = payload.len();
                chunk.set.clear();
                let out_msg = mqtt::Message::new(&topic, payload, qos);
                match client.publish(out_msg).await {
                    Ok(_) => {
                        msg_bytes += n;
                        debug!("sending to '{}'", topic);
                    },
                    Err(err) => {
                        error!("Problem sending data in {}: {}", serv, err);
                        error_count += 1;
                    }
                }
            },
            Some(msg_opt) = stream.next() => {
                match msg_opt {
                    Some(new_msg) => {
                        let mut count_messages = 0;
                        let payload = new_msg.payload();
                        let new_chunk: DataChunk = data_handler.load_data_message(&payload);
                        if !new_chunk.e.is_empty() {
                            error!("Server reported error: {}", new_chunk.e);
                            continue;
                        }
                        let mut routing = routing_arc.write().await;
                        for msg in new_chunk.set.iter() {
                            let client = msg.c_id.clone();
                            count_messages += 1;
                            if msg.x {
                                info!("Server requested {} to close connection", client);
                                routing.send_quit(&client).await;
                            } else {
                                routing.send_data(&client, &msg.d).await;
                            }
                        }
                        if count_messages > 0 {
                            debug!("Processed chunk with {} messages", count_messages);
                        }
                    },
                    None => {
                        error!("MQTT event critical error: non payload message");
                        break;
                    },
                }
            }

        }
        if error_count + raw_bytes + msg_bytes > 0 {
            let mut stat_update = stat.write().await;
            if error_count > 0 {
                stat_update.add_error("processing", &service_name, error_count);
            }
            stat_update.update_data_size_rate(msg_bytes, raw_bytes);
            error_count = 0;
            raw_bytes = 0;
            msg_bytes = 0;
        }
    }
}

/// Handles TCP connection for a client, managing data transfer and connection state.
async fn handle_tcp_connection(
    client_id: String,
    settings: Settings,
    stat: Arc<RwLock<Stat>>,
    mut reader: ReadHalf<TcpStream>,
    mut writer: WriteHalf<TcpStream>,
    ip: String,
    service_code: String,
    service_name: String,
    in_data_channel: mpsc::Sender<(String, Vec<u8>)>,
    mut out_data_channel: mpsc::Receiver<(String, Vec<u8>)>,
) {
    let mut buf = vec![0; settings.buffer_size];
    let idle_limit = Duration::from_secs(settings.idle_tcp_limit as u64);
    let serv = {
        let mut update_stat = stat.write().await;
        update_stat.connection_new(&ip, &service_name);
        format!("{} ({})", service_name, service_code)
    };
    info!("New connection {} in {} from {}", client_id, serv, ip);
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);

    loop {
        if in_bytes + out_bytes + error_count > 0 {
            let mut stat_update = stat.write().await;
            if error_count > 0 {
                stat_update.add_error(&ip, &service_name, error_count);
            }
            stat_update.add_input_traffic(&ip, &service_name, in_bytes);
            stat_update.add_output_traffic(&ip, &service_name, out_bytes);
            (in_bytes, out_bytes, error_count) = (0, 0, 0);
        }
        tokio::select! {
            // read client connection
            n = reader.read(&mut buf) => {
                let n = match n {
                    Ok(n) => n,
                    Err(err) => {
                        error_count += 1;
                        error!("Failed to read from {} in {}: {}", ip, serv, err);
                        break;
                    }
                };
                if n == 0 {
                    debug!("Closing read TCP connection for {} client {} in {}", ip, client_id, serv);
                    break;
                }
                out_bytes = n;
                if in_data_channel.send((client_id.clone(), buf[..n].to_vec())).await.is_err() {
                    warn!("Failed to send data to channel for {} in {}", client_id, serv);
                    error_count += 1;
                    break;
                } else {
                    debug!("Sent {} bytes to channel for {} in {}", n, client_id, serv);
                }
            },
            Some((_, data)) = out_data_channel.recv() => {
                if data.is_empty() {
                    info!("Closing connection {} client {} by request", ip, client_id);
                    break;
                }
                if let Err(err) = writer.write_all(&data).await {
                    error!("Failed to write to TCP stream for {} in {}: {}", client_id, serv, err);
                    if in_data_channel.send((client_id.clone(), [].to_vec())).await.is_err() {
                        error_count += 1;
                        warn!("Failed to send QUIT to channel for {} in {}", client_id, serv);
                    } else {
                        info!("Sent QUIT to channel for {} in {}", client_id, serv);
                    }
                    break;
                } else {
                    in_bytes = data.len();
                    debug!("Sent {} bytes to connection {} in {}", data.len(), client_id, serv);
                }
            },
            _ = sleep(idle_limit) => {
                warn!("Idle timeout for TCP connection {} client {}", ip, client_id);
                if in_data_channel.send((client_id.clone(), [].to_vec())).await.is_err() {
                    error_count += 1;
                    warn!("Failed to send QUIT to channel for {} in {}", client_id, serv);
                } else {
                    debug!("Sent QUIT to channel for {} in {}", client_id, serv);
                }
                break;
            },
        }
    }

    info!("Stopping connection handler for {} in {}", client_id, serv);
    let mut stat_update = stat.write().await;
    stat_update.connection_lost(&ip, &service_name);
    if error_count > 0 {
        stat_update.add_error(&ip, &service_name, error_count);
    }
    stat_update.add_input_traffic(&ip, &service_name, in_bytes);
    stat_update.add_output_traffic(&ip, &service_name, out_bytes);
}

/// Processes TCP client connections based on configuration settings
pub async fn tcp_client_processing(settings: &Settings, stat: Arc<RwLock<Stat>>, tasks: &mut JoinSet<()>) {
    let tcp_keys: Vec<_> = settings.tcp_sockets.keys().cloned().collect();
    for serv_name in settings.udp_sockets.keys() {
        if tcp_keys.contains(serv_name) {
            error!(
                "Configuration has duplicate service names: '{}' in UDP and TCP sockets",
                serv_name
            );
            return;
        }
    }

    for (service_name, ip_port_map) in settings.tcp_sockets.iter() {
        let Some((with_ip, with_port)) = ip_port_map.iter().next() else {
            error!("No IP-Port mapping found for server {}", service_name);
            continue;
        };
        let local_settings = settings.clone();
        let service_settings = settings.clone();
        let (service_in_channel_size, service_out_channel_size) = local_settings.channel_size();
        let (tx, rx) = mpsc::channel(service_in_channel_size);
        let service_code = code_name(&format!("{}{}", service_name, settings.client_name));
        let client_arc_stat = stat.clone();
        let service_arc_stat = stat.clone();
        let ip = with_ip.clone();
        let port = with_port.clone();
        let service_name = service_name.clone();
        let client_routing = RoutingState::create(service_code.clone());
        let routing_arc = Arc::new(RwLock::new(client_routing));
        let s_code = service_code.clone();
        let s_name = service_name.clone();
        let serv_info = format!("{} ({})", s_name, s_code);
        let out_routing_arc = routing_arc.clone();
        let watch_out_routing_arc = routing_arc.clone();
        let idle_tcp_limit = (settings.idle_tcp_limit as u64).clone();

        tasks.spawn(async move {
            processing_service_transport(
                true,
                service_settings,
                service_arc_stat,
                s_code,
                s_name,
                rx,
                out_routing_arc
            ).await;
        });
        tasks.spawn(async move {
            clear_routing(serv_info, watch_out_routing_arc, idle_tcp_limit).await;
        });
        tasks.spawn(async move {
            let listener = match TcpListener::bind((ip, port)).await {
                Ok(listener) => listener,
                Err(err) => {
                    error!("Failed to bind to {}:{} : {}", ip, port, err);
                    return;
                }
            };
            info!("Listening on tcp://{}:{} service {} ({})", ip, port, service_name, service_code);
            let serv_name = service_name.clone();
            loop {
                let (stream, c_addr) = match listener.accept().await {
                    Ok(result) => result,
                    Err(err) => {
                        error!("Failed to accept connection: {}", err);
                        let mut stat = client_arc_stat.write().await;
                        stat.add_error(&ip.to_string(), &serv_name, 1);
                        continue;
                    }
                };
                let c_ip = c_addr.ip().to_string();
                let client_id = fast_name();
                let serv_code = service_code.clone();
                let serv_name = serv_name.clone();
                let tx = tx.clone();
                let local_settings = local_settings.clone();
                let (client_tx, client_rx) = mpsc::channel(service_out_channel_size);
                let mut routing = routing_arc.write().await;
                routing.add_client(&client_id, client_tx);
                let conn_arc_stat = client_arc_stat.clone();

                tokio::spawn(async move {
                    let (reader, writer) = tokio::io::split(stream);
                    handle_tcp_connection(
                        client_id,
                        local_settings,
                        conn_arc_stat,
                        reader,
                        writer,
                        c_ip,
                        serv_code,
                        serv_name,
                        tx,
                        client_rx,
                    ).await;
                });
            }
        });
    }
}

/// Processes UDP client connections based on the provided settings.
/// Manages UDP sockets, client connections, and data routing for each service.
pub async fn udp_client_processing(settings: &Settings, stat: Arc<RwLock<Stat>>, tasks: &mut JoinSet<()>) {
    let tcp_keys: Vec<_> = settings.udp_sockets.keys().cloned().collect();
    for serv_name in settings.tcp_sockets.keys() {
        if tcp_keys.contains(serv_name) {
            error!(
                "Configuration error: duplicate service name '{}' found in both UDP and TCP sockets",
                serv_name
            );
            return;
        }
    }

    for (service_name, ip_port_map) in settings.udp_sockets.iter() {
        let Some((with_ip, with_port)) = ip_port_map.iter().next() else {
            error!("No UDP port mapping found for service {}", service_name);
            continue;
        };
        let service_settings = settings.clone();
        let (service_in_channel_size, service_out_channel_size) = settings.channel_size();
        let (tx, rx) = mpsc::channel(service_in_channel_size);
        let service_code = code_name(&format!("{}{}", service_name, settings.client_name));
        let service_arc_stat = stat.clone();
        let client_arc_stat = service_arc_stat.clone();
        let ip = with_ip.clone();
        let port = with_port.clone();
        let service_name = service_name.clone();
        let client_routing = RoutingState::create(service_code.clone());
        let routing_arc = Arc::new(RwLock::new(client_routing));
        let s_code = service_code.clone();
        let s_name = service_name.clone();
        let serv_info = format!("{} ({})", s_name, s_code);
        let out_routing_arc = routing_arc.clone();
        let watch_out_routing_arc = routing_arc.clone();
        let idle_udp_limit = (settings.idle_udp_limit as u64).clone();
        let client_name = settings.client_name.clone();
        let in_data_channel = tx.clone();
        let buffer_size = settings.buffer_size;

        tasks.spawn(async move {
            processing_service_transport(
                false,
                service_settings,
                service_arc_stat,
                s_code,
                s_name,
                rx,
                out_routing_arc
            ).await;
        });
        tasks.spawn(async move {
            clear_routing(serv_info, watch_out_routing_arc, idle_udp_limit).await;
        });
        tasks.spawn(async move {
            let server_socket = match UdpSocket::bind((ip, port)).await {
                Ok(result) => {
                    info!("UDP service listening on {}:{} for service {} ({})", ip, port, service_name, service_code);
                    result
                },
                Err(err) => {
                    error!("Failed to bind UDP socket for service {}: {}", service_name, err);
                    let mut update_stat = client_arc_stat.write().await;
                    update_stat.add_error(&ip.to_string(), &service_name, 1);
                    return;
                }
            };

            let serv_name = service_name.clone();
            let mut current_ip = ip.to_string();
            let sync_interval = Duration::from_secs(1);
            let mut buf = vec![0; buffer_size];
            let mut current_clients = HashMap::new();
            let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
            let mut lost_conn_iter = 0;
            let (single_tx, mut single_rx) = mpsc::channel(service_out_channel_size);
            loop {
                if in_bytes + out_bytes + error_count > 0 {
                    let mut stat_update = client_arc_stat.write().await;
                    if error_count > 0 {
                        stat_update.add_error(&current_ip, &service_name, error_count);
                    }
                    stat_update.add_input_traffic(&current_ip, &service_name, in_bytes);
                    stat_update.add_output_traffic(&current_ip, &service_name, out_bytes);
                    (in_bytes, out_bytes, error_count) = (0, 0, 0);
                }
                if lost_conn_iter > 0{
                    lost_conn_iter = 0;
                    let mut stat_update = client_arc_stat.write().await;
                    stat_update.connection_lost(&current_ip, &service_name);
                }
                tokio::select! {
                    res = server_socket.recv_from(&mut buf) => {
                        let (n, peer) = match res {
                            Ok((n, peer)) => {
                                current_ip = peer.ip().to_string();
                                (n, peer)
                            },
                            Err(err) => {
                                error!("UDP receive error for service {}: {}", service_name, err);
                                error_count = 1;
                                lost_conn_iter = 1;
                                continue;
                            }
                        };
                        if n == 0 {
                            debug!("Closing UDP connection for peer {}", peer);
                            lost_conn_iter = 1;
                            continue;
                        }
                        let client_id = code_name(&format!("{}-{}", client_name, peer.to_string()));

                        if !current_clients.contains_key(&client_id) {
                            info!("New UDP client connected: {} with ID {}", peer, client_id);
                            let mut update_stat = client_arc_stat.write().await;
                            let mut routing = routing_arc.write().await;
                            routing.add_client(&client_id, single_tx.clone());
                            update_stat.connection_new(&peer.ip().to_string(), &service_name);
                        }
                        _ = current_clients.entry(client_id.clone()).or_insert(peer);

                        out_bytes = n;
                        if in_data_channel.send((client_id.clone(), buf[..n].to_vec())).await.is_err() {
                            warn!("Failed to send data to channel for UDP client {} in service {}", client_id, serv_name);
                            error_count += 1;
                            continue;
                        } else {
                            debug!("Sent {} bytes to channel for client {} in service {}", n, client_id, serv_name);
                        }
                    },
                    Some((current_client_id, data)) = single_rx.recv() => {
                        if data.is_empty() {
                            info!("Closing UDP transfer for client {} by request", current_client_id);
                            if !current_clients.contains_key(&current_client_id) {
                                current_clients.remove(&current_client_id);
                            }
                            lost_conn_iter = 1;
                            continue;
                        }
                        let client_peer = current_clients.iter().find(|&(code, _)| code.eq(&current_client_id)).map(|(_, p)| p);
                        if client_peer.is_none() {
                            error!("Client peer not found for ID: {}", current_client_id);
                            lost_conn_iter = 1;
                            continue;
                        }
                        let ip = client_peer.unwrap();
                        current_ip = ip.ip().to_string();
                        in_bytes = match server_socket.send_to(&data, ip).await {
                            Ok(sent_n) => sent_n,
                            Err(err) => {
                                error!("UDP send error for client {} to {}: {}", current_client_id, current_ip, err);
                                lost_conn_iter = 1;
                                error_count = 1;
                                0
                            }
                        };
                    },
                    _ = sleep(sync_interval) => {
                        let routing = routing_arc.read().await;
                        current_clients.retain(|key, _| routing.exist(key));
                    },
                }
            }
        });
    }
}
