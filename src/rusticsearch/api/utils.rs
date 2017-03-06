use serde_json;

use api::iron::prelude::*;
use api::iron::status;


macro_rules! get_system {
    ($req: expr) => {{
        use api::Context;

        $req.get::<persistent::Read<Context>>().unwrap().system.clone()
    }}
}


macro_rules! read_path_parameter {
    ($req: expr, $name: expr) => {{
        $req.extensions.get::<Router>().unwrap().find($name)
    }}
}


pub fn json_response(status: status::Status, content: serde_json::Value) -> Response {
    let mut response = Response::with((status, format!("{}", content)));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    response
}


pub fn index_not_found_response() -> Response {
    json_response(status::NotFound, json!({"message": "Index not found"}))
}


macro_rules! get_index_or_404 {
    ($indices: expr, $index_name: expr) => {{
        use api::utils::index_not_found_response;

        let index_ref = match $indices.names.find_canonical($index_name) {
            Some(index_ref) => index_ref,
            None => {
                return Ok(index_not_found_response());
            }
        };

        match $indices.get(&index_ref) {
            Some(index) => index,
            None => {
                return Ok(index_not_found_response());
            }
        }
    }}
}


macro_rules! get_index_or_404_mut {
    ($indices: expr, $index_name: expr) => {{
        use api::utils::index_not_found_response;

        let index_ref = match $indices.names.find_canonical($index_name) {
            Some(index_ref) => index_ref,
            None => {
                return Ok(index_not_found_response());
            }
        };

        match $indices.get_mut(&index_ref) {
            Some(index) => index,
            None => {
                return Ok(index_not_found_response());
            }
        }
    }}
}


macro_rules! parse_json {
    ($string: expr) => {{
        use api::utils::json_response;

        let value: serde_json::Value = match serde_json::from_str($string) {
            Ok(data) => data,
            Err(_) => {
                return Ok(json_response(status::BadRequest, json!({"message": "Couldn't parse JSON"})));
            }
        };

        value
    }}
}


macro_rules! json_from_request_body {
    ($req: expr) => {{
        // Read request body to a string
        let mut payload = String::new();
        $req.body.read_to_string(&mut payload).unwrap();

        if !payload.is_empty() {
            Some(parse_json!(&payload))
        } else {
            None
        }
    }}
}
