use std::collections::HashMap;
use log::info;

pub struct Stat {
    values: HashMap<String, (usize, usize)>,
    connections: HashMap<String, (usize, usize)>,
    errors: HashMap<String, usize>,
    raw_data_size: u64,
    transfer_data_size: u64,
}

pub trait StatManage {
    fn add_input_traffic(&mut self, ip: &str, target: &str, value: usize);
    fn add_output_traffic(&mut self, ip: &str, target: &str, value: usize);
    fn connection_new(&mut self, ip: &str, target: &str);
    fn connection_lost(&mut self, ip: &str, target: &str);
    fn add_error(&mut self, ip: &str, target: &str, count: usize);
    fn update_data_size_rate(&mut self, payload_size: usize, raw_size: usize);
    fn show(&self);
    fn new() -> Self;
}

impl StatManage for Stat {
    fn new() -> Self {
        Stat {
            values: HashMap::new(),
            connections: HashMap::new(),
            errors: HashMap::new(),
            raw_data_size: 0,
            transfer_data_size: 0,
        }
    }

    fn add_error(&mut self, ip: &str, target: &str, count: usize) {
        let key = format!("{} {}", target, ip);
        let entry = self.errors.entry(key).or_insert(0);
        *entry += count;
    }

    fn add_input_traffic(&mut self, ip: &str, target: &str, value: usize) {
        let key = format!("{} {}", target, ip);
        let entry = self.values.entry(key).or_insert((0, 0));
        entry.0 += value;
    }

    fn add_output_traffic(&mut self, ip: &str, target: &str, value: usize) {
        let key = format!("{} {}", target, ip);
        let entry = self.values.entry(key).or_insert((0, 0));
        entry.1 += value;
    }

    fn connection_new(&mut self, ip: &str, target: &str) {
        let key = format!("{} {}", target, ip);
        let entry = self.connections.entry(key).or_insert((0, 0));
        entry.0 += 1;
    }

    fn connection_lost(&mut self, ip: &str, target: &str) {
        let key = format!("{} {}", target, ip);
        let entry = self.connections.entry(key).or_insert((0, 0));
        entry.1 += 1;
    }

    fn update_data_size_rate(&mut self, payload_size: usize, raw_size: usize) {
        self.transfer_data_size += payload_size as u64;
        self.raw_data_size += raw_size as u64;
    }

    fn show(&self) {
        info!("traffic:");
        for (key, (in_value, out_value)) in &self.values {
            let print_value = |value: usize| -> (f64, &'static str) {
                if value >= 1024 * 1024 {
                    let mb = (value as f64 / (1024.0 * 1024.0)) * 10.0;
                    (mb.round() / 10.0, "mb")
                } else {
                    let kb = (value as f64 / 1024.0) * 10.0;
                    (kb.round() / 10.0, "kb")
                }
            };

            let (in_value_formatted, in_unit) = print_value(*in_value);
            let (out_value_formatted, out_unit) = print_value(*out_value);

            info!(
                "  target {} in: {:.1} {} out: {:.1} {}",
                key, in_value_formatted, in_unit, out_value_formatted, out_unit
            );
        }

        info!("connections:");
        for (key, (new_connections, lost_connections)) in &self.connections {
            info!(
                "  target {} new: {} lost: {}",
                key, new_connections, lost_connections
            );
        }

        info!("errors:");
        for (key, error_count) in &self.errors {
            info!(
                "  target {} errors: {}",
                key, error_count
            );
        }

        if self.raw_data_size > 0 {
            let rate = (self.transfer_data_size as f64 / self.raw_data_size as f64) * 100.0 - 100.0;
            info!("payload rate: {}{:.1}%", if rate > 0.0 {"+"} else {""}, rate);
        } else {
            info!("payload rate: +0.0%");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let stats = Stat::new();
        assert!(stats.values.is_empty());
        assert!(stats.connections.is_empty());
        assert!(stats.errors.is_empty());
        assert_eq!(stats.raw_data_size, 0);
        assert_eq!(stats.transfer_data_size, 0);
    }

    #[test]
    fn test_add_input_output_traffic() {
        let mut stats = Stat::new();
        stats.add_input_traffic("192.168.1.1", "target_a", 1000);
        stats.add_input_traffic("192.168.1.1", "target_a", 500);
        stats.add_output_traffic("192.168.1.1", "target_a", 200);
        let key = "target_a 192.168.1.1";
        let (in_val, out_val) = stats.values.get(key).unwrap();
        assert_eq!(*in_val, 1500);
        assert_eq!(*out_val, 200);
    }

    #[test]
    fn test_connection_management() {
        let mut stats = Stat::new();
        stats.connection_new("10.0.0.1", "target_b");
        stats.connection_new("10.0.0.1", "target_b");
        stats.connection_lost("10.0.0.1", "target_b");
        let key = "target_b 10.0.0.1";
        let (new_cnt, lost_cnt) = stats.connections.get(key).unwrap();
        assert_eq!(*new_cnt, 2);
        assert_eq!(*lost_cnt, 1);
    }

    #[test]
    fn test_error_tracking() {
        let mut stats = Stat::new();
        stats.add_error("172.16.0.5", "target_c", 5);
        stats.add_error("172.16.0.5", "target_c", 3);
        stats.add_error("172.16.0.6", "target_c", 10);
        let key1 = "target_c 172.16.0.5";
        let key2 = "target_c 172.16.0.6";
        assert_eq!(*stats.errors.get(key1).unwrap(), 8);
        assert_eq!(*stats.errors.get(key2).unwrap(), 10);
    }

    #[test]
    fn test_data_size_rate() {
        let mut stats = Stat::new();
        stats.update_data_size_rate(1000, 2000);
        stats.update_data_size_rate(500, 1000);
        assert_eq!(stats.transfer_data_size, 1500);
        assert_eq!(stats.raw_data_size, 3000);
        assert_eq!(stats.transfer_data_size as f64 / stats.raw_data_size as f64, 0.5);
    }

    #[test]
    fn test_traffic_formatting_logic() {
        let mut stats = Stat::new();
        stats.add_input_traffic("192.168.1.1", "target_big", 1048576);
        let key = "target_big 192.168.1.1";
        assert!(stats.values.contains_key(key));
        assert_eq!(stats.values.get(key).unwrap().0, 1048576);
    }
}
