use api::iron::prelude::*;
use api::iron::status;
use api::iron::modifier::Modifier;


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


pub fn json_response<T: Modifier<Response>>(status: status::Status, content: T) -> Response {
    let mut response = Response::with((status, content));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    response
}


pub fn index_not_found_response() -> Response {
    json_response(status::NotFound, "{\"message\": \"Index not found\"}")
}


macro_rules! get_index_or_404 {
    ($indices: expr, $index_name: expr) => {{
        use api::utils::index_not_found_response;

        match $indices.get($index_name) {
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

        match $indices.get_mut($index_name) {
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

        match Json::from_str($string) {
            Ok(data) => data,
            Err(_) => {
                return Ok(json_response(status::BadRequest, "{\"message\": \"Couldn't parse JSON\"}"));
            }
        }
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
