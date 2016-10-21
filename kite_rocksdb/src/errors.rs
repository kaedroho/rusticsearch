pub struct RocksDBReadError {
    key: Vec<u8>,
    message: String,
}


impl RocksDBReadError {
    pub fn new(key: Vec<u8>, message: String) -> RocksDBReadError {
        RocksDBReadError {
            key: key,
            message: message,
        }
    }
}
