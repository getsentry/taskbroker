use cityhasher;
use rand::Rng;

pub enum KeyPrefix {
    Payload,
    IDLookup,
    Pending,
    Processing,
    Delay,
    Retry,
    Deadletter,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HashKey {
    pub namespace: String,
    pub topic: String,
    pub partition: i32,
}
impl HashKey {
    pub fn new(namespace: String, topic: String, partition: i32) -> Self {
        Self {
            namespace,
            topic,
            partition,
        }
    }

    pub fn hash(&self) -> String {
        format!("{}:{}:{}", self.namespace, self.topic, self.partition)
    }
}

#[derive(Debug)]
pub struct KeyBuilder {
    num_buckets: usize,
}
impl KeyBuilder {
    pub fn new(num_buckets: usize) -> Self {
        Self { num_buckets }
    }

    pub fn compute_bucket(&self, activation_id: &str) -> String {
        let hashint: u64 = cityhasher::hash(activation_id);
        format!("{:04x}", hashint % self.num_buckets as u64)
    }

    pub fn get_id_lookup_key(&self, activation_id: &str) -> Key {
        Key::new(
            KeyPrefix::IDLookup,
            HashKey::new(String::new(), String::new(), 0),
            String::new(),
            Some(activation_id.to_string()),
        )
    }

    pub fn get_payload_key(&self, hash_key: HashKey, activation_id: &str) -> Key {
        Key::new(
            KeyPrefix::Payload,
            hash_key,
            self.compute_bucket(activation_id),
            Some(activation_id.to_string()),
        )
    }

    pub fn get_pending_key(&self, hash_key: HashKey, activation_id: &str) -> Key {
        Key::new(
            KeyPrefix::Pending,
            hash_key,
            self.compute_bucket(activation_id),
            None,
        )
    }

    pub fn get_pending_key_for_iter(&self, hash_key: HashKey, bucket_hash: &str) -> Key {
        Key::new(KeyPrefix::Pending, hash_key, bucket_hash.to_string(), None)
    }

    pub fn get_processing_key(&self, hash_key: HashKey, activation_id: &str) -> Key {
        Key::new(
            KeyPrefix::Processing,
            hash_key,
            self.compute_bucket(activation_id),
            None,
        )
    }

    pub fn get_processing_key_for_iter(&self, hash_key: HashKey, bucket_hash: &str) -> Key {
        Key::new(
            KeyPrefix::Processing,
            hash_key,
            bucket_hash.to_string(),
            None,
        )
    }

    pub fn get_delay_key(&self, hash_key: HashKey, activation_id: &str) -> Key {
        Key::new(
            KeyPrefix::Delay,
            hash_key,
            self.compute_bucket(activation_id),
            None,
        )
    }

    pub fn get_delay_key_for_iter(&self, hash_key: HashKey, bucket_hash: &str) -> Key {
        Key::new(KeyPrefix::Delay, hash_key, bucket_hash.to_string(), None)
    }

    pub fn get_retry_key(&self, hash_key: HashKey, activation_id: &str) -> Key {
        Key::new(
            KeyPrefix::Retry,
            hash_key,
            self.compute_bucket(activation_id),
            None,
        )
    }

    pub fn get_retry_key_for_iter(&self, hash_key: HashKey, bucket_hash: &str) -> Key {
        Key::new(KeyPrefix::Retry, hash_key, bucket_hash.to_string(), None)
    }

    pub fn get_deadletter_key(&self, hash_key: HashKey, activation_id: &str) -> Key {
        Key::new(
            KeyPrefix::Deadletter,
            hash_key,
            self.compute_bucket(activation_id),
            None,
        )
    }

    pub fn get_deadletter_key_for_iter(&self, hash_key: HashKey, bucket_hash: &str) -> Key {
        Key::new(
            KeyPrefix::Deadletter,
            hash_key,
            bucket_hash.to_string(),
            None,
        )
    }

    pub fn get_expired_key(&self, hash_key: HashKey, activation_id: &str) -> Key {
        Key::new(
            KeyPrefix::Expired,
            hash_key,
            self.compute_bucket(activation_id),
            None,
        )
    }

    pub fn get_expired_key_for_iter(&self, hash_key: HashKey, bucket_hash: &str) -> Key {
        Key::new(KeyPrefix::Expired, hash_key, bucket_hash.to_string(), None)
    }
}

pub struct Key {
    pub prefix: KeyPrefix,
    pub hashkey: HashKey,
    pub bucket_hash: String,
    pub activation_id: Option<String>,
}
impl Key {
    pub fn new(
        prefix: KeyPrefix,
        hash_key: HashKey,
        bucket_hash: String,
        activation_id: Option<String>,
    ) -> Self {
        Self {
            prefix,
            hashkey: hash_key,
            bucket_hash,
            activation_id,
        }
    }

    pub fn build_redis_key(&self) -> String {
        let key = match self.prefix {
            KeyPrefix::Payload => {
                format!("payload:{{{}:{}}}", self.hashkey.hash(), self.bucket_hash)
            }
            KeyPrefix::IDLookup => "idlookup:".to_string(),
            KeyPrefix::Pending => {
                format!("pending:{{{}:{}}}", self.hashkey.hash(), self.bucket_hash)
            }
            KeyPrefix::Processing => format!(
                "processing:{{{}:{}}}",
                self.hashkey.hash(),
                self.bucket_hash
            ),
            KeyPrefix::Delay => format!("delay:{{{}:{}}}", self.hashkey.hash(), self.bucket_hash),
            KeyPrefix::Retry => format!("retry:{{{}:{}}}", self.hashkey.hash(), self.bucket_hash),
            KeyPrefix::Deadletter => format!(
                "deadletter:{{{}:{}}}",
                self.hashkey.hash(),
                self.bucket_hash
            ),
            KeyPrefix::Expired => {
                format!("expired:{{{}:{}}}", self.hashkey.hash(), self.bucket_hash)
            }
        };
        if self.activation_id.is_some() {
            format!("{}:{}", key, self.activation_id.clone().unwrap())
        } else {
            key
        }
    }
}

pub struct RandomStartIterator {
    total_values: usize,
    pub random_start: usize,
    current_index: usize,
}

impl RandomStartIterator {
    pub fn new(total_values: usize) -> Self {
        Self {
            total_values,
            random_start: rand::thread_rng().gen_range(0..total_values),
            current_index: 0,
        }
    }
}

impl Iterator for RandomStartIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.total_values {
            return None;
        }
        self.current_index += 1;
        let idx = (self.random_start + self.current_index) % self.total_values;
        Some(idx)
    }
}
