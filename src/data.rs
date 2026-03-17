use crate::common;

use log::{info, error};
use serde::{Serialize, Deserialize};
use hex::decode;
use common::{Settings, TOPIC_NAME_DATA_CLIENT, TOPIC_NAME_DATA_SERVER};
use aes_gcm::{Aes256Gcm, Nonce, Key, aead::{Aead, AeadCore, KeyInit, OsRng}};

pub fn client_data_topic(is_tcp: bool, service_code: &str) -> String {
    format!("i{}-{}-{}", if is_tcp {"t"} else {"u"}, TOPIC_NAME_DATA_SERVER, service_code)
}

pub fn server_data_topic(is_tcp: bool, service_code: &str) -> String {
    format!("s{}-{}-{}", if is_tcp {"t"} else {"u"}, TOPIC_NAME_DATA_CLIENT, service_code)
}


#[derive(Serialize, Deserialize, Clone)]
pub struct DataMsg {
    pub c_id: String,
    pub se: String,
    pub d: Vec<u8>,
    pub e: String,
    pub x: bool,
    pub n: Vec<u8>,
}


#[derive(Serialize, Deserialize, Clone)]
pub struct DataChunk {
    pub set: Vec<DataMsg>,
    pub e: String,
}


#[derive(Clone)]
pub struct DataHandlerSettings {
    cipher: Option<Aes256Gcm>,
    encryption: bool,
}

pub trait DataMessageFormater {
    fn new() -> Self;
    fn dump(&self) -> Vec<u8>;
    fn data_size(&self) -> usize;
}

impl DataMessageFormater for DataChunk {
    fn new() -> Self {
        DataChunk {e: "".to_string(), set: [].to_vec()}
    }
    fn dump(&self) -> Vec<u8> {
        let serialized = bincode::serialize(&self).unwrap();
        serialized
    }
    fn data_size(&self) -> usize {
        let mut res = 0;
        for msg in self.set.iter() {
            res += msg.d.len();
        }
        res
    }
}

pub trait DataHandler {
    fn new() -> Self;
    fn setup(&mut self, settings: &Settings) -> bool;
    fn make_data_message(&self, data: &[u8], code: &str, client: &str) -> DataMsg;
    fn make_quit_message(&self, code: &str, client: &str) -> DataMsg;
    fn load_data_message(&self, data: &[u8]) -> DataChunk;
}

impl DataHandler for DataHandlerSettings {
    fn new() -> Self {
        DataHandlerSettings {cipher: None, encryption: false}
    }

    fn setup(&mut self, settings: &Settings) -> bool {
        if !settings.cipher_key.is_empty() {
            let key_bytes = decode(&settings.cipher_key).expect("Incorrect Aes256Gcm key value");
            if key_bytes.is_empty() {
                return false;
            } else {
                // from cryptography.hazmat.primitives.ciphers.aead import AESGCM
                // print("new:", AESGCM.generate_key(bit_length=256))
                let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
                let cipher = Aes256Gcm::new(key);
                self.cipher = Some(cipher);
                self.encryption = true;
                info!("Using Aes-256-Gcm encryption");
            }
        }
        true
    }

    fn make_data_message(&self, data: &[u8], code: &str, client: &str) -> DataMsg {
        let mut oper_error = "".to_string();
        let msg_data;
        let n: Vec<u8>;
        if self.encryption && let Some(cipher) = &self.cipher {       
            let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
            n = nonce.to_vec();
            match cipher.encrypt(&nonce, data) {
                Ok(res) => msg_data = res,
                Err(err) => {
                    msg_data = [].to_vec();
                    oper_error = format!("Unexpected cipher using error: {}", err);
                    error!("Message creation: {}", oper_error);
                }
            }
        } else {
            n = [].to_vec();
            msg_data = data.to_vec();
        }
        DataMsg {
            c_id: client.to_string(),
            se: code.to_string(),
            d: msg_data,
            x: false,
            n: n,
            e: oper_error,
        }
    }

    fn make_quit_message(&self, code: &str, client: &str) -> DataMsg {
        DataMsg {
            c_id: client.to_string(),
            se: code.to_string(),
            d: [].to_vec(),
            x: true,
            n: [].to_vec(),
            e: "".to_string(),
        }
    }

    fn load_data_message(&self, data: &[u8]) -> DataChunk {
        match bincode::deserialize::<DataChunk>(data) {
            Ok(mut chunk) => {
                if self.encryption {
                    for msg in chunk.set.iter_mut() {
                        if msg.n.is_empty() {
                            continue;
                        }
                        let nonce = Nonce::from_slice(&msg.n);
                        if let Some(cipher) = &self.cipher {
                            match cipher.decrypt(&nonce, msg.d.as_slice()) {
                                Ok(res) => {
                                    msg.d = res;
                                },
                                Err(err) => {
                                    msg.e = format!("Unexpected cipher using error: {}", err);
                                }
                            }
                        }
                    }
                }
                chunk
            },
            Err(err) => DataChunk {set: [].to_vec(), e: err.to_string()},
        }
    }
}
