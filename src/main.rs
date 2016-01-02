extern crate iron;
extern crate router;
extern crate persistent;
extern crate rustc_serialize;

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use iron::prelude::*;
use iron::typemap::Key;
use iron::status;
use router::Router;
use rustc_serialize::json::{self, Json};


#[derive(Debug)]
struct Document {
    data: Json,
}

impl Document {
    fn from_json(data: Json) -> Document {
        Document{
            data: data,
        }
    }
}


#[derive(Debug)]
enum Filter {
    Not(Box<Filter>),
    Or(Vec<Filter>),
    And(Vec<Filter>),
    Term(String, String),
}


impl Filter {
    fn matches(&self, doc: &Document) -> bool {
        match *self {
            Filter::Not(ref filter) => !filter.matches(doc),
            Filter::Or(ref filters) => {
                for filter in filters.iter() {
                    if (filter.matches(doc)) {
                        return true;
                    }
                }

                false
            },
            Filter::And(ref filters) => {
                for filter in filters.iter() {
                    if (!filter.matches(doc)) {
                        return false;
                    }
                }

                true
            },
            Filter::Term(ref field, ref value) => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    if let Json::String(ref field_value) = *field_value {
                        return field_value == value
                    }
                }

                false
            }
        }
    }
}


#[derive(Debug)]
struct Mapping {
    pub docs: HashMap<String, Document>,
}

impl Mapping {
    fn new() -> Mapping {
        Mapping{
            docs: HashMap::new(),
        }
    }
}


#[derive(Debug)]
struct Index {
    pub mappings: HashMap<String, Mapping>,
}


impl Index {
    fn new() -> Index {
        Index{
            mappings: HashMap::new(),
        }
    }
}

struct Globals {
    pub indices: Mutex<HashMap<String, Index>>,
}

impl Globals {
    fn new() -> Globals {
        Globals {
            indices: Mutex::new(HashMap::new())
        }
    }
}

impl Key for Globals { type Value = Globals; }

fn index_not_found_response() -> Response {
    let mut response = Response::with((status::NotFound, "{\"message\": \"Index not found\"}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    return response;
}


fn main() {
    let f = Filter::Or(vec![
        Filter::Term("title".to_owned(), "test".to_owned()),
        Filter::Term("title".to_owned(), "foo".to_owned()),
    ]);

    println!("{:?}", f);

    let mut router = Router::new();

    router.get("/", |_: &mut Request| -> IronResult<Response> {
        Ok(Response::with((status::Ok, "Hello World!")))
    });

    router.get("/:index/_count", move |req: &mut Request| -> IronResult<Response> {
        let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

        // URL parameters
        let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

        // Lock index array
        let indices = glob.indices.lock().unwrap();

        // Find index
        let index = match indices.get(index_name) {
            Some(index) => index,
            None => {
                return Ok(index_not_found_response());
            }
        };

        // Load query from body
        let mut payload = String::new();
        req.body.read_to_string(&mut payload).unwrap();

        let data = if !payload.is_empty() {
            Some(match Json::from_str(&payload) {
                Ok(data) => data,
                Err(error) => {
                    // TODO: What specifically is bad about the JSON?
                    let mut response = Response::with((status::BadRequest, "{\"message\": \"Couldn't parse JSON\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    return Ok(response);
                }
            })
        } else {
            None
        };

        // TODO: Run query

        // Temporary count and return numbers
        let mut count = 0;
        for mapping in index.mappings.values() {
            count += mapping.docs.len();
        }

        let mut response = Response::with((status::Ok, format!("{{\"count\": {}}}", count)));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    });

    router.get("/:index/_search", move |req: &mut Request| -> IronResult<Response> {
        let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

        // URL parameters
        let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

        // Lock index array
        let indices = glob.indices.lock().unwrap();

        // Find index
        let index = match indices.get(index_name) {
            Some(index) => index,
            None => {
                return Ok(index_not_found_response());
            }
        };

        // Load query from body
        let mut payload = String::new();
        req.body.read_to_string(&mut payload).unwrap();

        let data = if !payload.is_empty() {
            Some(match Json::from_str(&payload) {
                Ok(data) => data,
                Err(error) => {
                    // TODO: What specifically is bad about the JSON?
                    let mut response = Response::with((status::BadRequest, "{\"message\": \"Couldn't parse JSON\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    return Ok(response);
                }
            })
        } else {
            None
        };

        // TODO: Run query

        let mut response = Response::with((status::Ok, "{\"hits\": {\"total\": 0, \"hits\": []}}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    });

    router.get("/:index/:mapping/:doc", move |req: &mut Request| -> IronResult<Response> {
        let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

        // URL parameters
        let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
        let mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");
        let doc_id = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

        // Lock index array
        let indices = glob.indices.lock().unwrap();

        // Find index
        let index = match indices.get(index_name) {
            Some(index) => index,
            None => {
                return Ok(index_not_found_response());
            }
        };

        // Find mapping
        let mapping = match index.mappings.get(mapping_name) {
            Some(mapping) => mapping,
            None => {
                let mut response = Response::with((status::NotFound, "{\"message\": \"Mapping not found\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        };

        // Find document
        let doc = match mapping.docs.get(doc_id) {
            Some(doc) => doc,
            None => {
                let mut response = Response::with((status::NotFound, "{\"message\": \"Document not found\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        };

        let mut response = Response::with((status::Ok, json::encode(&doc.data).unwrap()));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    });

    router.put("/:index/:mapping/:doc", move |req: &mut Request| -> IronResult<Response> {
        let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

        // URL parameters
        let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
        let mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");
        let ref doc_id = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

        // Lock index array
        let mut indices = glob.indices.lock().unwrap();

        // Find index
        let mut index = match indices.get_mut(index_name) {
            Some(index) => index,
            None => {
                return Ok(index_not_found_response());
            }
        };

        // Find mapping
        let mut mapping = match index.mappings.get_mut(mapping_name) {
            Some(mapping) => mapping,
            None => {
                let mut response = Response::with((status::NotFound, "{\"message\": \"Mapping not found\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        };

        // Load data from body
        let mut payload = String::new();
        req.body.read_to_string(&mut payload).unwrap();

        let data = if !payload.is_empty() {
            Some(match Json::from_str(&payload) {
                Ok(data) => data,
                Err(error) => {
                    // TODO: What specifically is bad about the JSON?
                    let mut response = Response::with((status::BadRequest, "{\"message\": \"Couldn't parse JSON\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    return Ok(response);
                }
            })
        } else {
            None
        };

        // Create and insert document
        if let Some(data) = data {
            let doc = Document::from_json(data);
            println!("{:?}", f.matches(&doc));
            mapping.docs.insert(doc_id.clone().to_owned(), doc);
        }

        let mut response = Response::with((status::Ok, "{}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    });

    router.put("/:index", move |req: &mut Request| -> IronResult<Response> {
        let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

        // URL parameters
        let ref index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

        // Lock index array
        let mut indices = glob.indices.lock().unwrap();

        // Load data from body
        let mut payload = String::new();
        req.body.read_to_string(&mut payload).unwrap();

        let data = if !payload.is_empty() {
            Some(match Json::from_str(&payload) {
                Ok(data) => data,
                Err(error) => {
                    // TODO: What specifically is bad about the JSON?
                    let mut response = Response::with((status::BadRequest, "{\"message\": \"Couldn't parse JSON\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    return Ok(response);
                }
            })
        } else {
            None
        };

        // Create index
        indices.insert(index_name.clone().to_owned(), Index::new());

        let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    });

    router.put("/:index/_mapping/:mapping", move |req: &mut Request| -> IronResult<Response> {
        let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

        // URL parameters
        let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
        let ref mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");

        // Lock index array
        let mut indices = glob.indices.lock().unwrap();

        // Find index
        let mut index = match indices.get_mut(index_name) {
            Some(index) => index,
            None => {
                return Ok(index_not_found_response());
            }
        };

        // Load data from body
        let mut payload = String::new();
        req.body.read_to_string(&mut payload).unwrap();

        let data = if !payload.is_empty() {
            Some(match Json::from_str(&payload) {
                Ok(data) => data,
                Err(error) => {
                    // TODO: What specifically is bad about the JSON?
                    let mut response = Response::with((status::BadRequest, "{\"message\": \"Couldn't parse JSON\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    return Ok(response);
                }
            })
        } else {
            None
        };

        // Insert mapping
        index.mappings.insert(mapping_name.clone().to_owned(), Mapping::new());

        let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    });

    router.post("/_bulk", move |req: &mut Request| -> IronResult<Response> {
        let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

        // Lock index array
        let mut indices = glob.indices.lock().unwrap();

        // Load data from body
        let mut payload = String::new();
        req.body.read_to_string(&mut payload).unwrap();

        for payload_part in payload.split('\n') {
            let data = if !payload.is_empty() {
                Some(Json::from_str(&payload))
            } else {
                None
            };
        }

        let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    });

    let mut chain = Chain::new(router);
    chain.link(persistent::Read::<Globals>::both(Globals::new()));

    Iron::new(chain).http("localhost:9200").unwrap();
}
