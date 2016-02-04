macro_rules! get_globals {
    ($req: expr) => {{
        $req.get::<persistent::Read<Globals>>().unwrap()
    }}
}


macro_rules! parse_json {
    ($string: expr) => {{
        match Json::from_str($string) {
            Ok(data) => data,
            Err(error) => {
                // TODO: What specifically is bad about the JSON?
                let mut response = Response::with((status::BadRequest,
                                                   "{\"message\": \"Couldn't parse JSON\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
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
