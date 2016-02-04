macro_rules! get_globals {
    ($req: expr) => {{
        $req.get::<persistent::Read<Globals>>().unwrap()
    }}
}


macro_rules! read_path_parameter {
    ($req: expr, $name: expr) => {{
        $req.extensions.get::<Router>().unwrap().find($name)
    }}
}


macro_rules! json_response {
    ($status: expr, $content: expr) => {{
        use iron::response::Response;

        let mut response = Response::with(($status, $content));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    }}
}


macro_rules! get_index_or_404 {
    ($indices: expr, $index_name: expr) => {{
        use iron::status;

        match $indices.get($index_name) {
            Some(index) => index,
            None => {
                return json_response!(status::NotFound, "{\"message\": \"Index not found\"}");
            }
        }
    }}
}


macro_rules! get_index_or_404_mut {
    ($indices: expr, $index_name: expr) => {{
        use iron::response::Response;
        use iron::status;

        match $indices.get_mut($index_name) {
            Some(index) => index,
            None => {
                return json_response!(status::NotFound, "{\"message\": \"Index not found\"}");
            }
        }
    }}
}


macro_rules! parse_json {
    ($string: expr) => {{
        match Json::from_str($string) {
            Ok(data) => data,
            Err(error) => {
                return json_response!(status::BadRequest, "{\"message\": \"Couldn't parse JSON\"}");
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
