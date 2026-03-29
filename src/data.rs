use std::collections::HashMap;

use crate::common;

use log::{info, error};
use serde::{Serialize, Deserialize};
use hex::decode;
use common::{EncryptionData, TOPIC_NAME_DATA_CLIENT, TOPIC_NAME_DATA_SERVER};
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

#[derive(Clone)]
pub struct DataHandlerSettings {
    cipher: Option<Aes256Gcm>,
    encryption: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DataChunk {
    pub set: Vec<DataMsg>,
    pub e: String,
}

pub trait DataMessageFormater {
    fn new() -> Self;
    fn dump(&self) -> Vec<u8>;
    fn data_size(&self) -> usize;
    fn len(&self) -> usize;
    fn extract_slice(&mut self, size: usize) -> Self;
}

impl DataMessageFormater for DataChunk {
    fn new() -> Self {
        DataChunk {e: "".to_string(), set: [].to_vec()}
    }
    fn dump(&self) -> Vec<u8> {
        let serialized = bincode::serialize(&self).unwrap();
        serialized
    }
    fn len(&self) -> usize {
        self.set.len()
    }
    fn data_size(&self) -> usize {
        let mut res = 0;
        for msg in self.set.iter() {
            res += msg.d.len();
        }
        res
    }
    fn extract_slice(&mut self, size: usize) -> DataChunk {
        let take_count = std::cmp::min(size, self.set.len());
        DataChunk {
            e: self.e.clone(),
            set: if take_count == 0 {
                Vec::new()
            } else {
                self.set.splice(0..take_count, []).collect()
            },
        }
    }
}

pub trait DataHandler {
    fn new() -> Self;
    fn setup<T: EncryptionData>(&mut self, settings: &T) -> bool;
    fn make_data_message(&self, data: &[u8], code: &str, client: &str) -> DataMsg;
    fn make_quit_message(&self, code: &str, client: &str) -> DataMsg;
    fn load_data_message(&self, data: &[u8]) -> DataChunk;
}

impl DataHandler for DataHandlerSettings {
    fn new() -> Self {
        DataHandlerSettings {cipher: None, encryption: false}
    }

    fn setup<T: EncryptionData>(&mut self, settings: &T) -> bool {
        let cipher_key = settings.main_cipher_key();
        if !settings.main_cipher_key().is_empty() {
            let key_bytes = decode(cipher_key).expect("Incorrect Aes256Gcm key value");
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

#[derive(Clone)]
pub struct TargetChunks {
    pub targets: HashMap<String, DataChunk>
}

pub trait ChunkTopic {
    fn new() -> Self;
    fn add_data(&mut self, topic: &str, msg: DataMsg);
    fn extract_slice(&mut self, size: usize) -> (String, DataChunk);
}

impl ChunkTopic for TargetChunks {
    fn new() -> Self {
        TargetChunks {
            targets: HashMap::new(),
        }
    }

    fn add_data(&mut self, topic: &str, msg: DataMsg) {
        let chunk = self.targets
            .entry(topic.to_string())
            .or_insert_with(|| DataChunk {
                e: String::new(),
                set: Vec::new(),
            });
        
        chunk.set.push(msg);
    }

    fn extract_slice(&mut self, size: usize) -> (String, DataChunk) {
        for (topic, chunk) in self.targets.iter_mut() {
            if !chunk.set.is_empty() {
                let extracted_chunk = chunk.extract_slice(size);
                if extracted_chunk.set.is_empty() {
                    continue;
                }
                return (topic.clone(), extracted_chunk);
            }
        }
        let mut empty_chunk = DataChunk::new();
        empty_chunk.e = String::new();
        ("".to_string(), empty_chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::EncryptionData;

    struct MockEncryptionData {
        key: String,
    }

    impl MockEncryptionData {
        fn new(key: &str) -> Self {
            MockEncryptionData { key: key.to_string() }
        }
    }

    impl EncryptionData for MockEncryptionData {
        fn main_cipher_key(&self) -> String {
            self.key.clone()
        }
    }

    #[test]
    fn test_topic_generation() {
        let topic_tcp = client_data_topic(true, "svc123");
        assert!(topic_tcp.contains("t"));
        assert!(topic_tcp.contains("svc123"));
        let topic_udp = client_data_topic(false, "svc123");
        assert!(topic_udp.contains("u"));
        let server_topic = server_data_topic(true, "svc123");
        assert!(server_topic.contains("s"));
    }

    #[test]
    fn test_data_handler_plain() {
        let mut handler = DataHandlerSettings::new();
        let _ = handler.setup(&MockEncryptionData::new(""));
        assert!(!handler.encryption);
        let msg = handler.make_data_message(b"Hello World", "OP1", "client123");
        assert_eq!(msg.c_id, "client123");
        assert_eq!(msg.se, "OP1");
        assert_eq!(msg.d, b"Hello World");
        assert!(!msg.n.is_empty() || msg.n.is_empty());
        let msg_quit = handler.make_quit_message("QUIT", "client123");
        assert!(msg_quit.x);
        assert_eq!(msg_quit.d, vec![]);
    }

    #[test]
    fn test_data_chunk_formatting() {
        let mut chunk = DataChunk::new();
        chunk.set.push(DataMsg {
            c_id: "c1".to_string(),
            se: "op".to_string(),
            d: vec![1, 2, 3],
            e: "".to_string(),
            x: false,
            n: vec![],
        });
        chunk.set.push(DataMsg {
            c_id: "c2".to_string(),
            se: "op".to_string(),
            d: vec![4, 5, 6],
            e: "".to_string(),
            x: false,
            n: vec![],
        });
        assert_eq!(chunk.len(), 2);
        assert_eq!(chunk.data_size(), 6);
        let serialized = chunk.dump();
        let deserialized: DataChunk = bincode::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.set.len(), 2);
        assert_eq!(deserialized.set[0].d, vec![1, 2, 3]);
        let mut chunk = chunk;
        let extracted = chunk.extract_slice(10);
        assert!(!extracted.set.is_empty());
        assert_eq!(extracted.set.len(), 2);
    }

    #[test]
    fn test_target_chunks() {
        let mut targets = TargetChunks::new();
        let msg1 = DataMsg {
            c_id: "c1".to_string(), se: "op1".to_string(), d: vec![1], e: "".to_string(), x: false, n: vec![]
        };
        let msg2 = DataMsg {
            c_id: "c2".to_string(), se: "op2".to_string(), d: vec![2], e: "".to_string(), x: false, n: vec![]
        };
        targets.add_data("topic_A", msg1.clone());
        targets.add_data("topic_A", msg2);
        targets.add_data("topic_A", msg1.clone());
        let (topic, chunk) = targets.extract_slice(1);
        assert_eq!(topic, "topic_A");
        assert_eq!(chunk.set.len(), 1);
        let (topic, chunk) = targets.extract_slice(1);
        assert_eq!(topic, "topic_A");
        assert_eq!(chunk.set.len(), 1);
        let (topic, chunk) = targets.extract_slice(1);
        assert_eq!(topic, "topic_A");
        assert_eq!(chunk.set.len(), 1);
        let (topic, chunk) = targets.extract_slice(1);
        assert_eq!(topic, "");
        assert!(chunk.set.is_empty());

        targets.add_data("topic_C", DataMsg { c_id: "c".to_string(), se: "op".to_string(), d: vec![0], e: "".to_string(), x: false, n: vec![] });
        let (_, empty) = targets.extract_slice(0);
        assert!(empty.set.is_empty());
        let (_, empty) = targets.extract_slice(1);
        assert_eq!(empty.set.len(), 1);
        let (topic_final, empty) = targets.extract_slice(1);
        assert_eq!(topic_final, "");
        assert!(empty.set.is_empty());
    }
}
