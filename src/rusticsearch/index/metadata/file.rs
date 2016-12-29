use std::path::Path;
use std::io::{self, Read, Write};
use std::fs::File;

use serde_json;
use serde_json::value::ToJson;
use atomicwrites::{self, AtomicFile, AllowOverwrite};

use index::metadata::IndexMetaData;
use index::metadata::parse::{parse, IndexMetaDataParseError};


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
pub enum LoadIndexMetaDataError {
    IndexMetaDataParseError(IndexMetaDataParseError),
    JsonParserError(serde_json::Error),
    IoError(io::Error),
}


impl From<LoadIndexMetaDataError> for String {
    fn from(e: LoadIndexMetaDataError) -> String {
        match e {
            LoadIndexMetaDataError::IndexMetaDataParseError(e) => format!("failed to load index metadata: {:?}", e).to_string(),
            LoadIndexMetaDataError::JsonParserError(e) => format!("failed to load index metadata: {}", e).to_string(),
            LoadIndexMetaDataError::IoError(e) => format!("failed to load index metadata: {}", e).to_string(),
        }
    }
}


impl From<IndexMetaDataParseError> for LoadIndexMetaDataError {
    fn from(e: IndexMetaDataParseError) -> LoadIndexMetaDataError {
        LoadIndexMetaDataError::IndexMetaDataParseError(e)
    }
}


impl From<serde_json::Error> for LoadIndexMetaDataError {
    fn from(e: serde_json::Error) -> LoadIndexMetaDataError {
        LoadIndexMetaDataError::JsonParserError(e)
    }
}


impl From<io::Error> for LoadIndexMetaDataError {
    fn from(e: io::Error) -> LoadIndexMetaDataError {
        LoadIndexMetaDataError::IoError(e)
    }
}


impl IndexMetaData {
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), SaveIndexMetadataError> {
        // Encode to JSON
        let s = format!("{}", try!(self.to_json()));

        // Write to file
        let file = AtomicFile::new(path, AllowOverwrite);
        try!(file.write(|f| {
            f.write_all(s.as_bytes())
        }));

        Ok(())
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<IndexMetaData, LoadIndexMetaDataError> {
        let mut file = try!(File::open(path));
        let mut s = String::new();
        try!(file.read_to_string(&mut s));

        let mut metadata = IndexMetaData::default();
        try!(parse(&mut metadata, try!(serde_json::from_str(&s))));

        Ok(metadata)
    }
}
