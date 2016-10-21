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
    Put(Vec<u8>),
    Merge(Vec<u8>),
    Delete(Vec<u8>),
    CommitWriteBatch,
}


#[derive(Debug)]
pub struct RocksDBWriteError {
    operation: RocksDBWriteOperation,
    message: String,
}


impl RocksDBWriteError {
    pub fn new_put(key: Vec<u8>, message: String) -> RocksDBWriteError {
        RocksDBWriteError {
            operation: RocksDBWriteOperation::Put(key),
            message: message,
        }
    }

    pub fn new_merge(key: Vec<u8>, message: String) -> RocksDBWriteError {
        RocksDBWriteError {
            operation: RocksDBWriteOperation::Merge(key),
            message: message,
        }
    }

    pub fn new_delete(key: Vec<u8>, message: String) -> RocksDBWriteError {
        RocksDBWriteError {
            operation: RocksDBWriteOperation::Delete(key),
            message: message,
        }
    }

    pub fn new_commit_write_batch(message: String) -> RocksDBWriteError {
        RocksDBWriteError {
            operation: RocksDBWriteOperation::CommitWriteBatch,
            message: message,
        }
    }
}