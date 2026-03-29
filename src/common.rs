use std::env;
use std::fmt;
use std::str::FromStr;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use uuid::Uuid;
use chrono::{Utc, DateTime};
use sha1::{Sha1, Digest};
use log::{debug, warn, info};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use paho_mqtt as mqtt;
use paho_mqtt::types::{QOS_0, QOS_1, QOS_2};
use rand;

// env variables
const ENV_IS_SERVER: &str = "SERVER";
const ENV_WORKERS: &str = "WORKERS";
const ENV_CLIENT_NAME: &str = "CLIENT_NAME";
const ENV_CLIENTS: &str = "CLIENTS";
const ENV_BUFFER_SIZE: &str = "READ_BUFFER_SIZE";
const ENV_TCP_SOCKETS: &str = "TCP_SOCKETS";
const ENV_UDP_SOCKETS: &str = "UDP_SOCKETS";
const ENV_STAT_SHOW_INTERVAL: &str = "STAT_SHOW_INTERVAL";
const ENV_MAX_DELAY_RATE: &str = "MAX_DELAY_RATE";
const ENV_TCP_TARGET: &str = "SERVER_TCP_TARGET";
const ENV_UDP_TARGET: &str = "SERVER_UDP_TARGET";
const ENV_KEY_CIPHER: &str = "CRYPTO_KEY";
const ENV_CONNECTION_IDLE: &str = "CONNECTION_IDLE_LIMIT";
const ENV_UDP_CONNECTION_IDLE: &str = "UDP_CONNECTION_IDLE_LIMIT";
const ENV_UDP_BIND_FROM: &str = "UDP_BIND_FROM";
const ENV_BROKER_HOST: &str = "BROKER_HOST";
const ENV_BROKER_USER: &str = "BROKER_USER";
const ENV_BROKER_QOS: &str = "QOS";
const ENV_BROKER_PASSWORD: &str = "BROKER_PASSWORD";
const ENV_BROKER_PORT: &str = "BROKER_PORT";
const ENV_LOADING_LEVEL: &str = "LOADING_LEVEL";
const ENV_KEEP_CONNECTION: &str = "KEEP_CONNECTION";

pub const TOPIC_NAME_DATA_CLIENT: &str = "data-c";
pub const TOPIC_NAME_DATA_SERVER: &str = "data-s";
type IpPortMap = HashMap<String, HashMap<IpAddr, u16>>;

/// Represents different levels of loading intensity
#[derive(Clone, PartialEq, Debug)]
pub enum LoadingLevelEnum {
    Extremely,
    Default,
    Low,
    High,
}

impl fmt::Display for LoadingLevelEnum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let txt = match self {
            LoadingLevelEnum::Default => "Default",
            LoadingLevelEnum::Low => "Low",
            LoadingLevelEnum::High => "High",
            LoadingLevelEnum::Extremely => "Extremely",
        };
        write!(f, "{} level", txt)
    }
}

impl FromStr for LoadingLevelEnum {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().trim() {
            "" => Ok(LoadingLevelEnum::Default),
            "default" => Ok(LoadingLevelEnum::Default),
            "high" => Ok(LoadingLevelEnum::High),
            "extremely" => Ok(LoadingLevelEnum::Extremely),
            "low" => Ok(LoadingLevelEnum::Low),
            _ => Err(format!("Invalid loading level: '{}'", s)),
        }
    }
}

/// Reads socket mapping configuration from environment variable
fn _read_env_socket_maps(name: &str, silent: bool) -> IpPortMap {
    let mut result = HashMap::new();
    let map_str = match env::var(name) {
        Ok(val) => val,
        Err(e) => {
            if !silent {
                warn!("Failed to read socket map from {}: {}", name, e);
            }
            return result
        },
    };

    for entry in map_str.split(';') {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 3 {
            if !silent {
                warn!("Invalid socket entry format '{}' - expected 'service:ip:port'", entry);
            }
            continue;
        }

        let service_name = parts[0].to_string();
        let ip_str = parts[1];
        let port_str = parts[2];
        let ip = match ip_str.parse::<IpAddr>() {
            Ok(ip) => ip,
            Err(e) => {
                if !silent {
                    warn!("Invalid IP address '{}' in socket entry: {}", ip_str, e);
                }
                continue;
            }
        };

        let port = match port_str.parse::<u16>() {
            Ok(port) => port,
            Err(e) => {
                if !silent {
                    warn!("Invalid port '{}' in socket entry: {}", port_str, e);
                }
                continue;
            }
        };
        result.entry(service_name).or_insert_with(HashMap::new).insert(ip, port);
    }
    result
}

/// Reads boolean configuration from environment variable
fn _read_env_bool(name: &str, silent: bool, default: bool) -> bool {
    let bool_str = match env::var(name) {
        Ok(val) => val,
        Err(e) => {
            if !silent {
                warn!("Failed to read boolean value from {}: {}", name, e);
            }
            "".to_string()
        },
    };
    let normalized = bool_str.to_lowercase();
    let key = normalized.trim();
    if normalized.is_empty() {
        return default;
    }
    let true_values = ["on", "yes", "1", "true", "ok"];
    true_values.contains(&key)
}

/// Reads string configuration from environment variable
fn _read_env_str(name: &str, silent: bool) -> String {
    match env::var(name) {
        Ok(val) => val,
        Err(e) => {
            if !silent {
                warn!("Failed to read string value from {}: {}", name, e);
            }
            "".to_string()
        },
    }
}

/// Reads unsigned integer configuration from environment variable
fn _read_env_uint(name: &str, silent: bool, default: usize) -> usize {
    match env::var(name) {
        Ok(val) => match val.parse::<usize>() {
            Ok(num) => num,
            Err(e) => {
                if !silent {
                    warn!("Failed to parse {} as unsigned integer: {}", name, e);
                }
                default
            },
        },
        Err(e) => {
            if !silent {
                warn!("Failed to read {} as unsigned integer: {}", name, e);
            }
            default
        },
    }
}

/// Reads list of strings configuration from environment variable
fn _read_env_strings(name: &str, silent: bool) -> Vec<String> {
    match env::var(name) {
        Ok(val) => {
            val.split(';').map(|s| s.trim().to_string()).collect()
        },
        Err(e) => {
            if !silent {
                warn!("Failed to read string list from {}: {}", name, e);
            }
            Vec::new()
        },
    }
}

#[derive(Clone)]
pub struct Settings {
    pub is_server: bool,
    pub broker_host: String,
    pub broker_user: String,
    pub broker_password: String,
    pub broker_port: u16,
    broker_qos: usize,
    pub workers: usize,
    pub buffer_size: usize,
    pub stat_delay: usize,
    pub client_name: String,
    pub clients: Vec<String>,
    pub tcp_sockets: IpPortMap,
    pub udp_sockets: IpPortMap,
    pub tcp_targets: IpPortMap,
    pub udp_targets: IpPortMap,
    pub cipher_key: String,
    pub idle_tcp_limit: usize,
    pub idle_udp_limit: usize,
    pub udp_bind_from: String,
    pub loading_level: LoadingLevelEnum,
    pub delay_rate: usize,
    pub keep_connection_mode: bool,
}

pub trait EncryptionData {
    fn main_cipher_key(&self) -> String;
}

impl EncryptionData for Settings {
    fn main_cipher_key(&self) -> String {
        self.cipher_key.clone()
    }
}

pub fn fast_name() -> String {
    let val = Uuid::new_v4().to_string();
    let parts: Vec<&str> = val.split('-').collect();
    format!("{}{}", parts[0], &parts[4][..4])
}

pub fn code_name(value: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(value);
    let result = hasher.finalize();
    let res = result[..10].iter().map(|b| format!("{:02x}", b)).collect();
    res
}

pub trait LoadingParams {
    fn channel_size(&self) -> (usize, usize);
    fn collect_message_timeout(&self, final_mode: bool) -> Duration;
    fn chunk_output_size(&self) -> usize;
    fn chunk_size_warning(&self) -> usize;
    fn qos_level(&self) -> i32;
    fn check_time(&self) -> Duration;
    fn make_mqtt_options(&self, connection_name: &str) -> (mqtt::CreateOptions, mqtt::ConnectOptions);
    fn default_buffer_size(&self) -> usize;
    fn stream_capacity(&self) -> usize;
    fn service_delay(&self) -> Duration;
}

/// Provides configuration parameters based on the loading level
impl LoadingParams for Settings {
    /// Returns the default buffer size based on the loading level
    fn default_buffer_size(&self) -> usize {
        match self.loading_level {
            LoadingLevelEnum::Default => 4 * 1024,
            LoadingLevelEnum::High => 8 * 1024,
            LoadingLevelEnum::Extremely => 8 * 1024,
            LoadingLevelEnum::Low => 4 * 1024
        }
    }

    /// Returns the stream capacity based on the loading level
    /// Higher loading levels result in larger stream capacities to handle more concurrent data.
    fn stream_capacity(&self) -> usize {
        match self.loading_level {
            LoadingLevelEnum::Default => 400,
            LoadingLevelEnum::High => 600,
            LoadingLevelEnum::Extremely => 1000,
            LoadingLevelEnum::Low => 100
        }
    }

    /// Returns the channel size configuration based on the loading level
    fn channel_size(&self) -> (usize, usize) {
        match self.loading_level {
            LoadingLevelEnum::Default => (1024 * 10, 1024 * 6),
            LoadingLevelEnum::High => (1024 * 12, 1024 * 8),
            LoadingLevelEnum::Extremely => (1024 * 20, 1024 * 12),
            LoadingLevelEnum::Low => (1024 * 4, 1024 * 2),
        }
    }

    /// Minimal pause value for network operations
    fn service_delay(&self) -> Duration {
        let ms = match self.loading_level {
            LoadingLevelEnum::Default => 4,
            LoadingLevelEnum::High => 3,
            LoadingLevelEnum::Extremely => 2,
            LoadingLevelEnum::Low => 6,
        };
        Duration::from_millis(ms)
    }

    /// Returns the timeout duration for collecting messages based on the loading level
    fn collect_message_timeout(&self, final_mode: bool) -> Duration {
        let (ms, long_ms) = match self.loading_level {
            LoadingLevelEnum::Default => (12, 250),
            LoadingLevelEnum::High => (10, 200),
            LoadingLevelEnum::Extremely => (6, 190),
            LoadingLevelEnum::Low => (15, 300),
        };
        Duration::from_millis(if final_mode {long_ms} else {ms})
    }

    /// Returns the chunk size warning threshold based on the loading level
    fn chunk_size_warning(&self) -> usize {
        match self.loading_level {
            LoadingLevelEnum::Default => 110,
            LoadingLevelEnum::High => 150,
            LoadingLevelEnum::Extremely => 180,
            LoadingLevelEnum::Low => 55,
        }
    }

    /// Returns the output buffer size for chunks based on the loading level
    fn chunk_output_size(&self) -> usize {
        match self.loading_level {
            LoadingLevelEnum::Default => 120,
            LoadingLevelEnum::High => 160,
            LoadingLevelEnum::Extremely => 200,
            LoadingLevelEnum::Low => 70,
        }
    }

    /// Creates MQTT options configured based on the loading level
    fn make_mqtt_options(&self, connection_name: &str) -> (mqtt::CreateOptions, mqtt::ConnectOptions) {
        let k_alive = {
            let val = match self.loading_level {
                LoadingLevelEnum::Default => 20,
                LoadingLevelEnum::High => 30,
                LoadingLevelEnum::Extremely => 30,
                LoadingLevelEnum::Low => 25,
            };
            Duration::from_secs(val)
        };
        let create_opts = mqtt::CreateOptionsBuilder::new().server_uri(
            format!("tcp://{}:{}", self.broker_host, self.broker_port)
        ).client_id(
            connection_name
        ).finalize();

         let conn_opts = mqtt::ConnectOptionsBuilder::new().keep_alive_interval(
            k_alive
        ).clean_session(true).user_name(
            self.broker_user.clone()
        ).password(
            self.broker_password.clone()
        ).finalize();
        (create_opts, conn_opts)
    }

    /// Returns the check interval duration (2 sec with random ~500ms)
    fn check_time(&self) -> Duration {
        Duration::from_secs(2) + Duration::from_millis(rand::random_range(100..1000))
    }

    /// Returns the QoS level based on broker configuration
    fn qos_level(&self) -> i32 {
        if self.broker_qos == 1 {
            QOS_1
        } else if self.broker_qos == 2 {
            QOS_2
        } else {
            QOS_0
        }
    }
}

/// Creates and configures application settings from environment variables.
/// Returns a fully initialized Settings struct with defaults for missing values.
pub fn create_settings() -> Settings {
    let is_server = _read_env_bool(ENV_IS_SERVER, true, false);
    let cipher_key = _read_env_str(ENV_KEY_CIPHER, true);
    let buffer_size = _read_env_uint(ENV_BUFFER_SIZE, true, 0);
    let workers = _read_env_uint(ENV_WORKERS, true, 4);
    let mut client_name = _read_env_str(ENV_CLIENT_NAME, true);
    if client_name.is_empty() {
        client_name = format!("{}-{}", if is_server {"s"} else {"c"}, fast_name());
    }
    let stat_delay = _read_env_uint(ENV_STAT_SHOW_INTERVAL, true, 120);
    let tcp_sockets = if !is_server {
        _read_env_socket_maps(ENV_TCP_SOCKETS, false)
    } else {
        IpPortMap::new()
    };
    let udp_sockets = if !is_server {
        _read_env_socket_maps(ENV_UDP_SOCKETS, false)
    } else {
        IpPortMap::new()
    };
    let tcp_targets = if is_server {
        _read_env_socket_maps(ENV_TCP_TARGET, false)
    } else {
        IpPortMap::new()
    };
    let udp_targets = if is_server {
        _read_env_socket_maps(ENV_UDP_TARGET, false)
    } else {
        IpPortMap::new()
    };
    let idle_tcp_limit = _read_env_uint(ENV_CONNECTION_IDLE, true, 60 * 5);
    let idle_udp_limit = _read_env_uint(ENV_UDP_CONNECTION_IDLE, true, 60 * 2);
    let mut udp_bind_from = _read_env_str(ENV_UDP_BIND_FROM, true);
    if udp_bind_from.is_empty() {udp_bind_from = "0.0.0.0:0".to_string();}
    let broker_qos = _read_env_uint(ENV_BROKER_QOS, true, 0);
    let broker_host = _read_env_str(ENV_BROKER_HOST, false);
    let broker_user = _read_env_str(ENV_BROKER_USER, false);
    let broker_password = _read_env_str(ENV_BROKER_PASSWORD, false);
    let broker_port = _read_env_uint(ENV_BROKER_PORT, true, 1883) as u16;
    let clients = _read_env_strings(ENV_CLIENTS, !is_server);
    let delay_rate = _read_env_uint(ENV_MAX_DELAY_RATE, true, 5);
    let keep_connection_mode = _read_env_bool(ENV_KEEP_CONNECTION, true, false);
    let loading_level = match LoadingLevelEnum::from_str(
        &_read_env_str(ENV_LOADING_LEVEL, true)
    ) {
        Ok(val) => val,
        Err(_) => LoadingLevelEnum::Default,
    };

    let mut settings = Settings {
        is_server,
        broker_host,
        broker_user,
        broker_password,
        broker_port,
        broker_qos,
        workers,
        buffer_size,
        stat_delay,
        client_name,
        clients,
        tcp_sockets,
        udp_sockets,
        tcp_targets,
        udp_targets,
        cipher_key,
        idle_tcp_limit,
        idle_udp_limit,
        udp_bind_from,
        loading_level,
        delay_rate,
        keep_connection_mode,
    };
    if settings.buffer_size < 1024 {
        settings.buffer_size = settings.default_buffer_size();
    }
    settings
}

/// Manages client routing state for message distribution.
/// Tracks active clients and their message timestamps for cleanup.
pub struct RoutingState {
    service: String,
    client_routing: HashMap<String, mpsc::Sender<(String, Vec<u8>)>>,
    message_update: HashMap<String, DateTime<Utc>>,
}

/// Provides methods for managing client routing and message distribution.
pub trait RoutingManager {
    /// Checks if a client exists in the routing table.
    fn exist(&self, client_id: &String) -> bool;

    /// Adds a new client to the routing table with its message channel.
    fn add_client(&mut self, client_id: &String, tx: mpsc::Sender<(String, Vec<u8>)>);

    /// Sends data to a specific client asynchronously.
    async fn send_data(&mut self, client_id: &String, data: &[u8]);

    /// Sends a quit signal to a client asynchronously.
    async fn send_quit(&mut self, client_id: &String);

    /// Removes inactive clients based on age threshold.
    fn clear_old(&mut self, max_age: Duration);

    /// Creates a new RoutingState instance for the specified service.
    fn create(service: String) -> Self;
}

impl RoutingManager for RoutingState {

    /// Creates a new RoutingState instance for the specified service.
    fn create(service: String) -> Self {
        RoutingState {
            service: service.clone(),
            client_routing: HashMap::new(),
            message_update: HashMap::new(),
        }
    }

    /// Removes inactive clients based on age threshold.
    fn clear_old(&mut self, max_age: Duration) {
        let now = Utc::now();
        let mut to_remove = Vec::new();

        for (client_id, timestamp) in self.message_update.iter() {
            if *timestamp + max_age < now {
                to_remove.push(client_id.clone());
            }
        }

        let mut deleted_count = 0;
        for client_id in to_remove {
            if let Some(tx) = self.client_routing.remove(&client_id) {
                let data_vec = [].to_vec();
                let send_result = tx.try_send((client_id.clone(), data_vec));
                if send_result.is_err() {
                    debug!("Failed to send quit to client {} during cleanup", client_id);
                }
                deleted_count += 1;
            }
            self.message_update.remove(&client_id);
        }
        if deleted_count > 0 {
            info!("{} routes deleted for {}", deleted_count, self.service);
        }
    }

    /// Checks if a client exists in the routing table.
    fn exist(&self, client_id: &String) -> bool {
        self.client_routing.contains_key(client_id)
    }

    /// Adds a new client to the routing table with its message channel.
    fn add_client(&mut self, client_id: &String, tx: mpsc::Sender<(String, Vec<u8>)>) {
        self.client_routing.insert(client_id.clone(), tx);
        self.message_update.insert(client_id.clone(), Utc::now());
    }

    /// Sends a quit signal to a client asynchronously.
    async fn send_quit(&mut self, client_id: &String) {
        if let Some(tx) = self.client_routing.get(client_id) {
            let data_vec = [].to_vec();
            let send_result = tx.send((client_id.clone(), data_vec)).await;
            match send_result {
                Ok(_) => {
                    self.message_update.insert(client_id.clone(), Utc::now());
                },
                Err(err) => {
                    warn!("Failed to send quit to client {}: {}", client_id, err);
                }
            }
        } else {
            warn!("Client {} not found in routing table for {}", client_id, self.service);
        }
    }

    /// Sends data to a specific client asynchronously.
    async fn send_data(&mut self, client_id: &String, data: &[u8]) {
        if let Some(tx) = self.client_routing.get(client_id) {
            let data_vec = data.to_vec();
            let send_result = tx.send((client_id.clone(), data_vec)).await;
            match send_result {
                Ok(_) => {
                    self.message_update.insert(client_id.clone(), Utc::now());
                },
                Err(err) => {
                    warn!("Failed to send data to client {}: {}", client_id, err);
                }
            }
        } else {
            warn!("Client {} not found in routing table for {}", client_id, self.service);
        }
    }
}

/// Periodically cleans up inactive routes for a service.
/// Runs in a loop, checking for and removing stale client connections.
pub async fn clear_routing(service: String, routing_arc: Arc<RwLock<RoutingState>>, idle_time: u64) {
    let interval = Duration::from_secs(idle_time / 5);
    let max_age = Duration::from_secs(idle_time * 2);
    let mut count = 0;
    info!("Running autocleaning routes for {}. Max age: {}, interval: {}", service, max_age.as_secs_f32(), interval.as_secs_f32());
    loop {
        sleep(interval).await;
        if count > 0 {
            let mut routing = routing_arc.write().await;
            routing.clear_old(max_age);
        }
        count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::time::Duration;
    use std::sync::Arc;
    use tokio::sync::{RwLock, mpsc};

    #[test]
    fn test_loading_level_enum_display() {
        assert_eq!(format!("{}", LoadingLevelEnum::Default), "Default level");
        assert_eq!(format!("{}", LoadingLevelEnum::Low), "Low level");
        assert_eq!(format!("{}", LoadingLevelEnum::High), "High level");
        assert_eq!(format!("{}", LoadingLevelEnum::Extremely), "Extremely level");
    }

    #[test]
    fn test_loading_level_enum_from_str() {
        assert_eq!(LoadingLevelEnum::from_str("").unwrap(), LoadingLevelEnum::Default);
        assert_eq!(LoadingLevelEnum::from_str("default").unwrap(), LoadingLevelEnum::Default);
        assert_eq!(LoadingLevelEnum::from_str("DEFAULT").unwrap(), LoadingLevelEnum::Default);
        assert_eq!(LoadingLevelEnum::from_str("low").unwrap(), LoadingLevelEnum::Low);
        assert_eq!(LoadingLevelEnum::from_str("high").unwrap(), LoadingLevelEnum::High);
        assert_eq!(LoadingLevelEnum::from_str("extremely").unwrap(), LoadingLevelEnum::Extremely);
        let invalid = LoadingLevelEnum::from_str("unknown");
        assert!(invalid.is_err());
    }

    #[test]
    fn test_read_env_socket_maps() {
        unsafe {
            env::set_var("TEST_SOCKET_MAP", "svc1:127.0.0.1:8080;svc2:192.168.1.1:3000");
        }
        let result = _read_env_socket_maps("TEST_SOCKET_MAP", true);
        let svc1_ips = result.get("svc1").unwrap();
        let ip = "127.0.0.1".parse::<IpAddr>().unwrap();
        assert_eq!(svc1_ips.get(&ip), Some(&8080));
        let svc2_ips = result.get("svc2").unwrap();
        let ip2 = "192.168.1.1".parse::<IpAddr>().unwrap();
        assert_eq!(svc2_ips.get(&ip2), Some(&3000));
        unsafe {
            env::remove_var("TEST_SOCKET_MAP");
        }
    }

    #[test]
    fn test_read_env_bool() {
        let test_cases = vec![
            ("true", true),
            ("True", true),
            ("YES", true),
            ("on", true),
            ("1", true),
            ("ok", true),
            ("false", false),
            ("no", false),
            ("0", false),
            ("off", false),
            ("", false),
        ];

        for (val, expected) in test_cases {
            unsafe {
                env::set_var("TEST_BOOL_VAR", val);
            }
            let result = _read_env_bool("TEST_BOOL_VAR", true, false);
            assert_eq!(result, expected, "Failed for input '{}'", val);
            unsafe {
                env::remove_var("TEST_BOOL_VAR");
            }
        }
        unsafe {
            env::remove_var("TEST_BOOL_VAR");
        }
        let result = _read_env_bool("MISSING_VAR", true, true);
        assert_eq!(result, true);
    }

    #[test]
    fn test_read_env_str() {
        unsafe {
            env::set_var("TEST_STRING_VAR", "Hello World");
        }
        let result = _read_env_str("TEST_STRING_VAR", true);
        assert_eq!(result, "Hello World");
        unsafe {
            env::remove_var("TEST_STRING_VAR");
        }
        let result_missing = _read_env_str("MISSING_VAR", true);
        assert_eq!(result_missing, "");
    }

    #[test]
    fn test_read_env_uint() {
        unsafe {
            env::set_var("TEST_UINT_VAR", "42");
        }
        let result = _read_env_uint("TEST_UINT_VAR", true, 0);
        assert_eq!(result, 42);
        unsafe {
            env::remove_var("TEST_UINT_VAR");
        }
        unsafe {
            env::set_var("TEST_UINT_VAR", "abc");
        }
        let result = _read_env_uint("TEST_UINT_VAR", true, 10);
        assert_eq!(result, 10);
        unsafe {
            env::remove_var("TEST_UINT_VAR");
        }
    }

    #[test]
    fn test_read_env_strings() {
        unsafe {
            env::set_var("TEST_STRINGS_VAR", "item1; item2; item3");
        }
        let result = _read_env_strings("TEST_STRINGS_VAR", true);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], "item1");
        assert_eq!(result[1], "item2");
        assert_eq!(result[2], "item3");
        unsafe {
            env::remove_var("TEST_STRINGS_VAR");
        }
        let result_empty = _read_env_strings("MISSING_VAR", true);
        assert!(result_empty.is_empty());
    }

    #[tokio::test]
    async fn test_routing_create_and_exist() {
        let routing = RoutingState::create("test_service".to_string());
        let test_id = "client_1".to_string();
        assert!(!routing.exist(&test_id));
    }

    #[tokio::test]
    async fn test_add_client_and_exist() {
        let mut routing = RoutingState::create("test_service".to_string());
        let client_id = "client_1".to_string();
        let (tx, rx) = mpsc::channel::<(String, Vec<u8>)>(1);
        routing.add_client(&client_id, tx);
        assert!(routing.exist(&client_id));
        drop(rx);
    }

    #[tokio::test]
    async fn test_send_data() {
        let mut routing = RoutingState::create("test_service".to_string());
        let client_id = "client_1".to_string();
        let (tx, mut rx) = mpsc::channel::<(String, Vec<u8>)>(1);
        routing.add_client(&client_id, tx);
        let data = vec![1, 2, 3];
        routing.send_data(&client_id, &data).await;
        let (received_id, received_data) = rx.recv().await.unwrap();
        assert_eq!(received_id, client_id);
        assert_eq!(received_data, data);
    }

    #[tokio::test]
    async fn test_send_quit() {
        let mut routing = RoutingState::create("test_service".to_string());
        let client_id = "client_1".to_string();
        let (tx, mut rx) = mpsc::channel::<(String, Vec<u8>)>(1);
        routing.add_client(&client_id, tx);
        routing.send_quit(&client_id).await;
        let (received_id, received_data) = rx.recv().await.unwrap();
        assert_eq!(received_id, client_id);
        assert!(received_data.is_empty());
    }

    #[tokio::test]
    async fn test_clear_old() {
        let mut routing = RoutingState::create("test_service".to_string());
        let client_id = "client_1".to_string();
        let (tx, _rx) = mpsc::channel::<(String, Vec<u8>)>(1);
        routing.add_client(&client_id, tx);
        let max_age = Duration::from_secs(0);
        routing.clear_old(max_age);
        assert!(!routing.exist(&client_id));
    }

    #[tokio::test]
    async fn test_clear_routing_lifecycle() {
        let service = "test_service".to_string();
        let routing_arc: Arc<RwLock<RoutingState>> = Arc::new(RwLock::new(RoutingState::create(service.clone())));
        let client_id = "client_1".to_string();
        let (tx, _rx) = mpsc::channel::<(String, Vec<u8>)>(1);
        {
            let mut routing = routing_arc.write().await;
            routing.add_client(&client_id, tx);
        }
        let idle_time = 2; 
        let handle = tokio::spawn(clear_routing(service.clone(), Arc::clone(&routing_arc), idle_time));
        tokio::time::sleep(Duration::from_secs(idle_time * 3)).await;
        handle.abort();
    }
}
