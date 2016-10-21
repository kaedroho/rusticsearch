#[derive(Debug)]
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


#[derive(Debug)]
enum RocksDBWriteOperation {
    Put,
    Merge,
    Delete,
}


#[derive(Debug)]
pub struct RocksDBWriteError {
    key: Vec<u8>,
    operation: RocksDBWriteOperation,
    message: String,
}


impl RocksDBWriteError {
    pub fn new_put(key: Vec<u8>, message: String) -> RocksDBWriteError {
        RocksDBWriteError {
            key: key,
            operation: RocksDBWriteOperation::Put,
            message: message,
        }
    }

    pub fn new_merge(key: Vec<u8>, message: String) -> RocksDBWriteError {
        RocksDBWriteError {
            key: key,
            operation: RocksDBWriteOperation::Merge,
            message: message,
        }
    }

    pub fn new_delete(key: Vec<u8>, message: String) -> RocksDBWriteError {
        RocksDBWriteError {
            key: key,
            operation: RocksDBWriteOperation::Delete,
            message: message,
        }
    }
}