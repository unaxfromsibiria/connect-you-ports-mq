use crate::common;
use crate::stat;
use crate::data;

use common::{Settings, code_name, LoadingParams, RoutingManager, RoutingState, clear_routing};
use data::{
    server_data_topic,
    client_data_topic,
    DataHandlerSettings,
    DataHandler,
    DataChunk,
    DataMessageFormater,
    TargetChunks,
    ChunkTopic,
};
use bytes::Bytes;
use stat::{Stat, StatManage};
use log::{info, warn, error, debug};
use futures::StreamExt;
use paho_mqtt as mqtt;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

const OUT_TTL: u32 = 128;


/// Handles a TCP connection to a target service, forwarding data between client and target
async fn handle_target_tcp_connection(
    client_id: String,
    settings: Arc<Settings>,
    stat: Arc<RwLock<Stat>>,
    service_code: String,
    service_name: String,
    service_index: u16,
    mut in_data_channel: mpsc::Receiver<(String, Bytes)>,
    out_channel: mpsc::Sender<(String, Bytes, u16)>,
    target_host: IpAddr,
    target_port: u16,
) {
    let tcp_stream = match tokio::net::TcpStream::connect((target_host, target_port)).await {
        Ok(stream) => {stream},
        Err(err) => {
            error!("Failed to connect to TCP target '{}' {}:{} : {}", service_name, target_host, target_port, err);
            return ;
        }
    };
    let buffer_size= settings.buffer_size;
    if tcp_stream.set_ttl(OUT_TTL).is_err() {
        warn!("TTL is {}", tcp_stream.ttl().unwrap());
    }
    let (mut reader, mut writer) = tokio::io::split(tcp_stream);
    let mut read_buffer = vec![0u8; buffer_size];
    let ip = target_host.to_string();
    let serv = {
        let mut stat_update = stat.write().await;
        stat_update.connection_new(&ip, &service_name);
        format!("{} ({}) - {}:{}", service_name, service_code, target_host, target_port)
    };
    let idle_limit = Duration::from_secs(settings.idle_tcp_limit as u64);
    let mut with_quit = String::new();
    let wait_before_close_time = settings.collect_message_timeout(true);
    let min_delay= settings.service_delay();
    let keep_conn = settings.keep_connection_mode;
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);

    loop {
        tokio::select! {
            read_result = reader.read(&mut read_buffer) => {
                match read_result {
                    Ok(0) => {
                        with_quit = format!("Connection {} closed by peer for {}", serv, client_id);
                        break;
                    },
                    Ok(n) => {
                        let data = &read_buffer[..n];
                        match out_channel.send((client_id.clone(), Bytes::from(data.to_vec()), service_index)).await {
                            Ok(_) => {
                                in_bytes = n;
                            },
                            Err(err) => {
                                error_count += 1;
                                error!("Failed to send data to client {} in {} {}", client_id, serv, err);
                                with_quit = format!("Connection {} closed after closed out channel {}", serv, client_id);
                                if keep_conn {
                                    sleep(min_delay).await;
                                    continue;
                                } else {
                                    break;
                                }
                            }
                        }
                    },
                    Err(err) => {
                        error!("Failed to read from TCP stream for {} {}: {}", client_id, serv, err);
                        with_quit = format!("Connection {} closed by peer for {}", serv, client_id);
                        error_count += 1;
                        break;
                    }
                }
            },
            Some((_, out_data)) = in_data_channel.recv() => {
                if out_data.is_empty() {
                    info!("Connection {} closed by request for {}", serv, client_id);
                    break;
                }
                if let Err(err) = writer.write_all(&out_data).await {
                    error_count += 1;
                    error!("Failed to write to TCP stream for {} {}: {}", client_id, serv, err);
                    with_quit = format!("Connection {} closed by error for {}", serv, client_id);
                    if keep_conn {
                        sleep(min_delay).await;
                        continue;
                    } else {
                        break;
                    }
                } else {
                    out_bytes = out_data.len();
                }
            },
            _ = sleep(idle_limit) => {
                with_quit = format!("Connection {} closed by idle timeout for {}", serv, client_id);
                break;
            }
        }
        if in_bytes + out_bytes + error_count > 0 {
            let mut stat_update = stat.write().await;
            if error_count > 0 {
                stat_update.add_error(&ip, &service_name, error_count);
            }
            stat_update.add_input_traffic(&ip, &service_name, in_bytes);
            stat_update.add_output_traffic(&ip, &service_name, out_bytes);
            (in_bytes, out_bytes, error_count) = (0, 0, 0);
        }
    }
    // final data transfering operations
    loop {
        tokio::select! {
            Some((_, out_data)) = in_data_channel.recv() => {
                if out_data.is_empty() {
                    continue;
                }
                if let Err(err) = writer.write_all(&out_data).await {
                    error!("Failed to write to TCP stream for {} {}: {}", client_id, serv, err);
                    error_count += 1;
                }
            },
            _ = sleep(wait_before_close_time) => {
                if !with_quit.is_empty() {
                    info!("{} (quit request sending)", with_quit);
                    match out_channel.send((client_id.clone(), Bytes::new(), service_index)).await {
                        Ok(_) => {},
                        Err(err) => {
                            error_count += 1;
                            error!("Out channel stopped for {} with error: {}", serv, err);
                        }
                    }
                }
                break;
            }
        }
    }
    let mut stat_update = stat.write().await;
    if in_bytes + out_bytes + error_count > 0 {
        if error_count > 0 {
            stat_update.add_error(&ip, &service_name, error_count);
        }
        stat_update.add_input_traffic(&ip, &service_name, in_bytes);
        stat_update.add_output_traffic(&ip, &service_name, out_bytes);
    }
    stat_update.connection_lost(&ip, &service_name);
}

/// Handles UDP transfer between client and target host
async fn handle_target_udp_transfering(
    client_id: String,
    settings: Arc<Settings>,
    stat: Arc<RwLock<Stat>>,
    service_code: String,
    service_name: String,
    service_index: u16,
    mut in_data_channel: mpsc::Receiver<(String, Bytes)>,
    out_channel: mpsc::Sender<(String, Bytes, u16)>,
    target_host: IpAddr,
    target_port: u16,
) {
    let udp_bind_from = match SocketAddr::from_str(&settings.udp_bind_from) {
        Ok(addr_new) => addr_new,
        Err(err) => {
            error!("Incorrect address in settings {}: {}", settings.udp_bind_from, err);
            return;
        }
    };
    let socket = match UdpSocket::bind(&udp_bind_from).await {
        Ok(socket) => {
            debug!("New output socket {} -> {}:{} for {}", udp_bind_from, target_host, target_port, client_id);
            socket
        },
        Err(err) => {
            error!("Error binding UDP socket {} for {} service {}: {}", udp_bind_from, client_id, service_code, err);
            return;
        }
    };
    let buffer_size = settings.buffer_size;
    let mut read_buffer = vec![0u8; buffer_size];
    let ip = target_host.to_string();
    let serv = {
        let mut stat_update = stat.write().await;
        stat_update.connection_new(&ip, &service_name);
        format!("{} ({}) - {}:{}", service_name, service_code, target_host, target_port)
    };
    let idle_limit = Duration::from_secs(settings.idle_tcp_limit as u64);
    let wait_before_close_time = settings.collect_message_timeout(true);
    let mut with_quit = String::new();
    let (mut in_bytes, mut out_bytes, mut error_count) = (0, 0, 0);
    let target_addr = (target_host, target_port);

    loop {
        tokio::select! {
            read_result = socket.recv_from(&mut read_buffer) => {
                match read_result {
                    Ok((n, addr)) => {
                        if n == 0 {
                            with_quit = format!("Connection {} closed by peer for {} client: {}", serv, addr, client_id);
                            break;
                        }
                        let data = &read_buffer[..n];
                        match out_channel.send((client_id.clone(), Bytes::from(data.to_vec()), service_index)).await {
                            Ok(_) => {
                                in_bytes = n;
                            },
                            Err(err) => {
                                error_count += 1;
                                error!("Failed to send data to UDP client {} in {}: {}", client_id, serv, err);
                                continue;
                            }
                        }
                    },
                    Err(err) => {
                        error!("Failed to read from UDP socket for {} {}: {}", client_id, serv, err);
                        with_quit = format!("Connection {} closed by peer for {}", serv, client_id);
                        error_count += 1;
                        break;
                    }
                }
            },
            Some((_, out_data)) = in_data_channel.recv() => {
                if out_data.is_empty() {
                    info!("Connection {} closed by request for {}", serv, client_id);
                    break;
                }

                match socket.send_to(&out_data, target_addr).await {
                    Ok(n) => {
                        out_bytes = n;
                    },
                    Err(err) => {
                        error_count += 1;
                        error!("Failed to send UDP data to {} ({}) from {}: {}", target_host, service_name, client_id, err);
                        break;
                    }
                }
            },
            _ = sleep(idle_limit) => {
                with_quit = format!("Connection {} closed by idle timeout for {}", serv, client_id);
                break;
            }
        }
        if in_bytes + out_bytes + error_count > 0 {
            let mut stat_update = stat.write().await;
            if error_count > 0 {
                stat_update.add_error(&ip, &service_name, error_count);
            }
            stat_update.add_input_traffic(&ip, &service_name, in_bytes);
            stat_update.add_output_traffic(&ip, &service_name, out_bytes);
            (in_bytes, out_bytes, error_count) = (0, 0, 0);
        }
    }
    // final reading client
    loop {
        tokio::select! {
            Some((_, out_data)) = in_data_channel.recv() => {
                if out_data.is_empty() {
                    continue;
                }

                match socket.send_to(&out_data, target_addr).await {
                    Ok(n) => {
                        out_bytes += n;
                    },
                    Err(err) => {
                        error_count += 1;
                        error!("Failed to send UDP data to {} ({}) from {}: {}", target_host, service_name, client_id, err);
                        break;
                    }
                }
            },
            _ = sleep(wait_before_close_time) => {
                if !with_quit.is_empty() {
                    match out_channel.send((client_id.clone(), Bytes::new(), service_index)).await {
                        Ok(_) => {
                            warn!("{} (quit request sending)", with_quit);
                        },
                        Err(err) => {
                            error!("Out channel stopped for {} with error: {}", serv, err);
                        }
                    }
                }
                break;
            }
        }
    }

    let mut stat_update = stat.write().await;
    if in_bytes + out_bytes + error_count > 0 {
        if error_count > 0 {
            stat_update.add_error(&ip, &service_name, error_count);
        }
        stat_update.add_input_traffic(&ip, &service_name, in_bytes);
        stat_update.add_output_traffic(&ip, &service_name, out_bytes);
    }
    stat_update.connection_lost(&ip, &service_name);
}

/// Processes TCP server connections and handles MQTT message routing
pub async fn server_tcp_processing(settings: &Settings, stat: Arc<RwLock<Stat>>, tasks: &mut JoinSet<()>) {
    let tcp_keys: Vec<_> = settings.tcp_targets.keys().cloned().collect();
    for serv_name in settings.udp_targets.keys() {
        if tcp_keys.contains(serv_name) {
            error!("Configuration has duplicate service names: '{}'", serv_name);
            return;
        }
    }
    let mut targets = Vec::new();
    let mut service_index = 0;
    for (serv_name, addr_map) in settings.tcp_targets.iter() {
        let Some((ip, port)) = addr_map.iter().next() else {
            error!("No target socket found for server {}", serv_name);
            continue;
        };
        for client in settings.clients.iter() {
            let service_code = code_name(&format!("{}{}", serv_name, client));
            let input_topic = server_data_topic(true, &service_code);
            let client_topic = client_data_topic(true, &service_code);
            info!("TCP service {} (code: {}) for client {} -> tcp://{}:{}", serv_name, service_code, client, ip, port);
            targets.push((
                serv_name.clone(), service_code.clone(), input_topic, client_topic, ip.clone(), *port, service_index as u16
            ));
            service_index += 1;
        }
    }
    let mut data_handler = DataHandlerSettings::new();
    if !data_handler.setup(settings) {
        error!("Failed to initialize cipher settings");
        return;
    }
    let mut service_routing = HashMap::new();
    let mut service_index_map = HashMap::new();
    let connection_name = format!("main-t-{}", settings.client_name);
    for (s_name, s_code, topic, out_topic, ip_s, port_s, index) in targets.iter() {
        service_routing.entry(topic.clone()).or_insert((index.clone(), s_name.clone(), s_code.clone(), *ip_s, *port_s));
        service_index_map.entry(index.clone()).or_insert((s_code.clone(), out_topic.clone()));
    }
    let settings = settings.clone();
    let out_service_routing = Arc::new(RwLock::new(service_routing));
    let arc_stat = stat.clone();

    tasks.spawn(async move {
        let cap = settings.stream_capacity();
        let qos = settings.qos_level();
        let (create_opts, conn_opts) = settings.make_mqtt_options(&connection_name);
        let mut client = mqtt::AsyncClient::new(create_opts).expect("Error creating the async MQTT client");

        match client.connect(conn_opts).await {
            Ok(_) => {
                info!("MQTT client connected");
                let out_services = out_service_routing.read().await;
                for topic in out_services.keys() {
                    match client.subscribe(topic, qos).await {
                        Ok(_) => {
                            info!("Subscribed to topic '{}' for connection {}", topic, connection_name);
                        },
                        Err(err) => {
                            error!("Failed to subscribe to topic {}: {}", topic, err);
                        }
                    }
                }
            },
            Err(err) => {
                error!("MQTT connection error: {}", err);
                return;
            }
        }

        let (service_in_channel_size, service_out_channel_size) = settings.channel_size();
        let (data_in_channel, mut data_out_channel) = mpsc::channel(service_in_channel_size);
        // input chunks
        let route = RoutingState::create("main".to_string());
        let idle_tcp_limit = (settings.idle_tcp_limit as u64).clone();
        let watch_connection_route_arc = Arc::new(RwLock::new(route));
        let connection_route_arc = watch_connection_route_arc.clone();

        tokio::spawn(async move {
            clear_routing("tcp-server".to_string(), watch_connection_route_arc, idle_tcp_limit).await;
        });

        let mut stream = client.get_stream(cap);
        let (mut raw_bytes, mut msg_bytes, mut error_count) = (0, 0, 0);
        let mut chunks = TargetChunks::new();
        let send_delay = settings.collect_message_timeout(false);
        let service_delay = settings.service_delay();
        let chunk_size = settings.chunk_output_size();
        let arc_settings = Arc::new(settings);

        loop {
            if error_count + raw_bytes + msg_bytes > 0 {
                let mut stat_update = arc_stat.write().await;
                if error_count > 0 {
                    stat_update.add_error("processing", "tcp", error_count);
                }
                stat_update.update_data_size_rate(msg_bytes, raw_bytes);
                (error_count, raw_bytes, msg_bytes) = (0, 0, 0);
            }
            tokio::select! {
                Some(msg_opt) = stream.next() => {
                    match msg_opt {
                        Some(new_msg) => {
                            let payload = new_msg.payload();
                            let topic = new_msg.topic();
                            msg_bytes += payload.len();
                            let new_chunk: DataChunk = data_handler.load_data_message(&payload);
                            if !new_chunk.e.is_empty() {
                                error!("Client reported error in topic {}: {}", topic, new_chunk.e);
                                error_count += 1;
                                continue;
                            }
                            for msg in new_chunk.set.iter() {
                                let client_id = &msg.c_id;
                                let is_new = {
                                    let connection_route = connection_route_arc.read().await;
                                    !connection_route.exist(&client_id)
                                };
                                if is_new {
                                    info!("New TCP client {} connected to {}", client_id, topic);
                                    let (tx, rx) = mpsc::channel(service_out_channel_size);
                                    let out_services = out_service_routing.read().await;
                                    if let Some(item) = out_services.get(topic) {
                                        let (index_service, s_name, s_code, ip_s, port_s) = item.clone();
                                        let main_tx = data_in_channel.clone();
                                        let stat = arc_stat.clone();
                                        let client = client_id.clone();
                                        let conn_settings = arc_settings.clone();
                                        tokio::spawn(async move {
                                            handle_target_tcp_connection(
                                                client,
                                                conn_settings,
                                                stat,
                                                s_code,
                                                s_name,
                                                index_service,
                                                rx,
                                                main_tx,
                                                ip_s,
                                                port_s,
                                            ).await;
                                        });
                                        let mut connection_route = connection_route_arc.write().await;
                                        connection_route.add_client(&client_id, tx);
                                    } else {
                                        warn!("No routing configuration for topic {}", topic);
                                    }
                                }
                                let mut connection_route = connection_route_arc.write().await;
                                if msg.x {
                                    connection_route.send_quit(&client_id).await;
                                } else {
                                    raw_bytes += msg.d.len();
                                    connection_route.send_data(&client_id, &msg.d).await;
                                }
                            }
                        },
                        None => {
                            error!("MQTT event critical error: non payload message");
                            sleep(send_delay).await;
                            continue;
                        },
                    }
                },
                _ = sleep(send_delay) => {
                    loop {
                        let (cl_topic, chunk) = chunks.extract_slice(chunk_size);
                        if cl_topic.is_empty() || chunk.len() < 1 {
                            sleep(service_delay).await;
                            break;
                        }
                        let payload = chunk.dump();
                        msg_bytes += payload.len();
                        let out_msg = mqtt::Message::new(&cl_topic, payload, qos);
                        match client.publish(out_msg).await {
                            Ok(_) => {
                                raw_bytes += chunk.data_size();
                            },
                            Err(err) => {
                                error!("Problem sending TCP data in {}: {}", cl_topic, err);
                                sleep(service_delay).await;
                                error_count += 1;
                                break;
                            }
                        }
                    }
                },
                Some((client_id, data, service_index)) = data_out_channel.recv() => {
                    if let Some((service_code, cl_topic)) = service_index_map.get(&service_index) {
                        let msg = if data.is_empty() {
                            data_handler.make_quit_message(&service_code, &client_id)
                        } else {
                            data_handler.make_data_message(&data, &service_code, &client_id)
                        };
                        chunks.add_data(&cl_topic, msg);
                    } else {
                        error!("Unknown service index {}", service_index);
                    }
                },
            }
        }
    });
}

pub async fn server_udp_processing(settings: &Settings, stat: Arc<RwLock<Stat>>, tasks: &mut JoinSet<()>) {
    let tcp_keys: Vec<_> = settings.tcp_targets.keys().cloned().collect();
    for serv_name in settings.udp_targets.keys() {
        if tcp_keys.contains(serv_name) {
            error!("Configuration has duplicate service names: '{}'", serv_name);
            return;
        }
    }
    let mut targets = Vec::new();
    let mut service_index = 0;
    for (serv_name, addr_map) in settings.udp_targets.iter() {
        let Some((ip, port)) = addr_map.iter().next() else {
            error!("No target socket found for server {}", serv_name);
            continue;
        };
        for client in settings.clients.iter() {
            let service_code = code_name(&format!("{}{}", serv_name, client));
            let input_topic = server_data_topic(false, &service_code);
            let client_topic = client_data_topic(false, &service_code);
            info!("{} ({} {} {}) -> udp://{}:{}", input_topic, serv_name, service_code, client, ip, port);
            targets.push((
                serv_name.clone(), service_code.clone(), input_topic, client_topic, ip.clone(), *port, service_index as u16
            ));
            service_index += 1;
        }
    }
    let mut data_handler = DataHandlerSettings::new();
    if !data_handler.setup(settings) {
        error!("Wrong settings for cipher");
        return;
    }
    let mut service_routing = HashMap::new();
    let mut service_index_map = HashMap::new();
    let connection_name = format!("main-u-{}", settings.client_name);
    for (s_name, s_code, topic, out_topic, ip_s, port_s, index) in targets.iter() {
        service_routing.entry(topic.clone()).or_insert((index.clone(), s_name.clone(), s_code.clone(), *ip_s, *port_s));
        service_index_map.entry(index.clone()).or_insert((s_code.clone(), out_topic.clone()));
    }
    if service_routing.len() < 1 {
        info!("No targets for UDP services.");
        return;
    }
    let settings = settings.clone();
    let out_service_routing = Arc::new(RwLock::new(service_routing));
    let arc_stat = stat.clone();

    tasks.spawn(async move {
        let cap = settings.stream_capacity();
        let qos = settings.qos_level();
        let delay = settings.collect_message_timeout(false);
        let (create_opts, conn_opts) = settings.make_mqtt_options(&connection_name);
        let mut client = mqtt::AsyncClient::new(create_opts).expect("Error creating the async MQTT client");

        match client.connect(conn_opts).await {
            Ok(_) => {
                info!("MQTT client connected");
                let out_services = out_service_routing.read().await;
                for topic in out_services.keys() {
                    match client.subscribe(topic, qos).await {
                        Ok(_) => {
                            info!("Waiting for messages from '{}' connection {}", topic, connection_name);
                        },
                        Err(err) => {
                            error!("Error subscribing to {} in main handler: {}", topic, err);
                        }
                    }
                }
            },
            Err(err) => {
                error!("MQTT connection error: {}", err);
                return;
            }
        }
        let (service_in_channel_size, service_out_channel_size) = settings.channel_size();
        let (data_in_channel, mut data_out_channel) = mpsc::channel(service_in_channel_size);
        // input chunks
        let route = RoutingState::create("main".to_string());
        let idle_udp_limit = (settings.idle_udp_limit as u64).clone();
        let watch_connection_route_arc = Arc::new(RwLock::new(route));
        let connection_route_arc = watch_connection_route_arc.clone();

        tokio::spawn(async move {
            clear_routing("udp-server".to_string(), watch_connection_route_arc, idle_udp_limit).await;
        });

        let mut stream = client.get_stream(cap);
        let (mut raw_bytes, mut msg_bytes, mut error_count) = (0, 0, 0);
        let mut chunks = TargetChunks::new();
        let send_delay = settings.collect_message_timeout(false);
        let service_delay = settings.service_delay();
        let chunk_size = settings.chunk_output_size();
        let arc_settings = Arc::new(settings);

        loop {
            if error_count + raw_bytes + msg_bytes > 0 {
                let mut stat_update = arc_stat.write().await;
                if error_count > 0 {
                    stat_update.add_error("processing", "tcp", error_count);
                }
                stat_update.update_data_size_rate(msg_bytes, raw_bytes);
                (error_count, raw_bytes, msg_bytes) = (0, 0, 0);
            }
            tokio::select! {
                Some(msg_opt) = stream.next() => {
                    match msg_opt {
                        Some(new_msg) => {
                            let payload = new_msg.payload();
                            let topic = new_msg.topic();
                            msg_bytes = payload.len();
                            let new_chunk: DataChunk = data_handler.load_data_message(&payload);
                            if !new_chunk.e.is_empty() {
                                error!("Client reported error: {} in {}", new_chunk.e, topic);
                                error_count += 1;
                                continue;
                            }
                            for msg in new_chunk.set.iter() {
                                let client_id = &msg.c_id;
                                let is_new = {
                                    let connection_route = connection_route_arc.read().await;
                                    !connection_route.exist(&client_id)
                                };
                                if is_new {
                                    info!("New UDP client {} connected to {}", client_id, topic);
                                    let (tx, rx) = mpsc::channel(service_out_channel_size);
                                    let out_services = out_service_routing.read().await;
                                    if let Some(item) = out_services.get(topic) {
                                        let (index_service, s_name, s_code, ip_s, port_s) = item.clone();
                                        let main_tx = data_in_channel.clone();
                                        let client = client_id.to_string();
                                        let stat = arc_stat.clone();
                                        let conn_settings = arc_settings.clone();
                                        tokio::spawn(async move {
                                            handle_target_udp_transfering(
                                                client,
                                                conn_settings,
                                                stat,
                                                s_code,
                                                s_name,
                                                index_service,
                                                rx,
                                                main_tx,
                                                ip_s,
                                                port_s,
                                            ).await;
                                        });
                                        let mut connection_route = connection_route_arc.write().await;
                                        connection_route.add_client(&client_id, tx);
                                    } else {
                                        warn!("No routing settings for topic {}", topic);
                                    }
                                }
                                let mut connection_route = connection_route_arc.write().await;
                                if msg.x {
                                    connection_route.send_quit(&client_id).await;
                                } else {
                                    raw_bytes += msg.d.len();
                                    connection_route.send_data(&client_id, &msg.d).await;
                                }
                            }
                        },
                        None => {
                            error!("MQTT event critical error: non payload message");
                            sleep(delay).await;
                            continue;
                        },
                    }
                },
                _ = sleep(send_delay) => {
                    loop {
                        let (cl_topic, chunk) = chunks.extract_slice(chunk_size);
                        if cl_topic.is_empty() || chunk.len() < 1 {
                            sleep(service_delay).await;
                            break;
                        }
                        let payload = chunk.dump();
                        msg_bytes += payload.len();
                        let out_msg = mqtt::Message::new(&cl_topic, payload, qos);
                        match client.publish(out_msg).await {
                            Ok(_) => {
                                raw_bytes += chunk.data_size();
                            },
                            Err(err) => {
                                error!("Problem sending UDP data in {}: {}", cl_topic, err);
                                sleep(service_delay).await;
                                error_count += 1;
                                break;
                            }
                        }
                    }
                },
                Some((client_id, data, service_index)) = data_out_channel.recv() => {
                    if let Some((service_code, cl_topic)) = service_index_map.get(&service_index) {
                        let msg = if data.is_empty() {
                            data_handler.make_quit_message(&service_code, &client_id)
                        } else {
                            data_handler.make_data_message(&data, &service_code, &client_id)
                        };
                        chunks.add_data(&cl_topic, msg);
                    } else {
                        error!("Unknown service index {}", service_index);
                    }
                },
            }
        }
    });
}
