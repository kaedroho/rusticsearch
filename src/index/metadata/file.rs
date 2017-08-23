use std::path::Path;
use std::io::{self, Read, Write};
use std::fs::File;

use serde_json;
use atomicwrites::{self, AtomicFile, AllowOverwrite};

use index::metadata::IndexMetadata;
use index::metadata::parse::{parse, IndexMetadataParseError};


#[derive(Debug)]
pub enum SaveIndexMetadataError {
    JsonEncoderError(serde_json::Error),
    IoError(atomicwrites::Error<io::Error>),
}


impl From<SaveIndexMetadataError> for String {
    fn from(e: SaveIndexMetadataError) -> String {
        match e {
            SaveIndexMetadataError::JsonEncoderError(e) => format!("failed to save index metadata: {}", e).to_string(),
            SaveIndexMetadataError::IoError(e) => format!("failed to save index metadata: {}", e).to_string(),
        }
    }
}

impl From<serde_json::Error> for SaveIndexMetadataError {
    fn from(e: serde_json::Error) -> SaveIndexMetadataError {
        SaveIndexMetadataError::JsonEncoderError(e)
    }
}


impl From<atomicwrites::Error<io::Error>> for SaveIndexMetadataError {
    fn from(e: atomicwrites::Error<io::Error>) -> SaveIndexMetadataError {
        SaveIndexMetadataError::IoError(e)
    }
}


#[derive(Debug)]
pub enum LoadIndexMetadataError {
    IndexMetadataParseError(IndexMetadataParseError),
    JsonParserError(serde_json::Error),
    IoError(io::Error),
}


impl From<LoadIndexMetadataError> for String {
    fn from(e: LoadIndexMetadataError) -> String {
        match e {
            LoadIndexMetadataError::IndexMetadataParseError(e) => format!("failed to load index metadata: {:?}", e).to_string(),
            LoadIndexMetadataError::JsonParserError(e) => format!("failed to load index metadata: {}", e).to_string(),
            LoadIndexMetadataError::IoError(e) => format!("failed to load index metadata: {}", e).to_string(),
        }
    }
}


impl From<IndexMetadataParseError> for LoadIndexMetadataError {
    fn from(e: IndexMetadataParseError) -> LoadIndexMetadataError {
        LoadIndexMetadataError::IndexMetadataParseError(e)
    }
}


impl From<serde_json::Error> for LoadIndexMetadataError {
    fn from(e: serde_json::Error) -> LoadIndexMetadataError {
        LoadIndexMetadataError::JsonParserError(e)
    }
}


impl From<io::Error> for LoadIndexMetadataError {
    fn from(e: io::Error) -> LoadIndexMetadataError {
        LoadIndexMetadataError::IoError(e)
    }
}


impl IndexMetadata {
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), SaveIndexMetadataError> {
        // Encode to JSON
        let s = format!("{}", serde_json::to_value(self)?);

        // Write to file
        let file = AtomicFile::new(path, AllowOverwrite);
        file.write(|f| {
            f.write_all(s.as_bytes())
        })?;

        Ok(())
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<IndexMetadata, LoadIndexMetadataError> {
        let mut file = File::open(path)?;
        let mut s = String::new();
        file.read_to_string(&mut s)?;

        let mut metadata = IndexMetadata::default();
        parse(&mut metadata, serde_json::from_str(&s)?)?;

        Ok(metadata)
    }
}
