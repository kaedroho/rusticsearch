use std::path::Path;
use std::io::{self, Read, Write};
use std::fs::File;

use serde_json;
use serde_json::value::ToJson;
use atomicwrites::{self, AtomicFile, AllowOverwrite};

use index::registry::NameRegistry;
//use index::metadata::parse::{parse, NameRegistryParseError};


#[derive(Debug)]
pub enum SaveNameRegistryError {
    JsonEncoderError(serde_json::Error),
    IoError(atomicwrites::Error<io::Error>),
}


impl From<SaveNameRegistryError> for String {
    fn from(e: SaveNameRegistryError) -> String {
        match e {
            SaveNameRegistryError::JsonEncoderError(e) => format!("failed to save index metadata: {}", e).to_string(),
            SaveNameRegistryError::IoError(e) => format!("failed to save index metadata: {}", e).to_string(),
        }
    }
}

impl From<serde_json::Error> for SaveNameRegistryError {
    fn from(e: serde_json::Error) -> SaveNameRegistryError {
        SaveNameRegistryError::JsonEncoderError(e)
    }
}


impl From<atomicwrites::Error<io::Error>> for SaveNameRegistryError {
    fn from(e: atomicwrites::Error<io::Error>) -> SaveNameRegistryError {
        SaveNameRegistryError::IoError(e)
    }
}

/*
#[derive(Debug)]
pub enum LoadNameRegistryError {
    NameRegistryParseError(NameRegistryParseError),
    JsonParserError(serde_json::Error),
    IoError(io::Error),
}


impl From<LoadNameRegistryError> for String {
    fn from(e: LoadNameRegistryError) -> String {
        match e {
            LoadNameRegistryError::NameRegistryParseError(e) => format!("failed to load index metadata: {:?}", e).to_string(),
            LoadNameRegistryError::JsonParserError(e) => format!("failed to load index metadata: {}", e).to_string(),
            LoadNameRegistryError::IoError(e) => format!("failed to load index metadata: {}", e).to_string(),
        }
    }
}


impl From<NameRegistryParseError> for LoadNameRegistryError {
    fn from(e: NameRegistryParseError) -> LoadNameRegistryError {
        LoadNameRegistryError::NameRegistryParseError(e)
    }
}


impl From<serde_json::Error> for LoadNameRegistryError {
    fn from(e: serde_json::Error) -> LoadNameRegistryError {
        LoadNameRegistryError::JsonParserError(e)
    }
}


impl From<io::Error> for LoadNameRegistryError {
    fn from(e: io::Error) -> LoadNameRegistryError {
        LoadNameRegistryError::IoError(e)
    }
}
*/

impl NameRegistry {
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), SaveNameRegistryError> {
        // Encode to JSON
        let s = format!("{}", try!(self.to_json()));

        // Write to file
        let file = AtomicFile::new(path, AllowOverwrite);
        try!(file.write(|f| {
            f.write_all(s.as_bytes())
        }));

        Ok(())
    }
/*
    pub fn load<P: AsRef<Path>>(path: P) -> Result<NameRegistry, LoadNameRegistryError> {
        let mut file = try!(File::open(path));
        let mut s = String::new();
        try!(file.read_to_string(&mut s));

        let mut metadata = NameRegistry::default();
        try!(parse(&mut metadata, try!(serde_json::from_str(&s))));

        Ok(metadata)
    }
*/
}
