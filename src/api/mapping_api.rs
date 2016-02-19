use std::io::Read;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::Json;

use super::persistent;
use super::super::{Globals, mapping};


pub fn view_put_mapping(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Get index
    let mut index = get_index_or_404_mut!(indices, *index_name);

    // Load data from body
    let data = json_from_request_body!(req);

    let data = match data {
        Some(data) => data,
        None => {
            // TODO: Better error
            return json_response!(status::BadRequest, "{\"acknowledged\": false}");
        },
    };

    let data = data.as_object().unwrap().get(*mapping_name).unwrap();

    // Insert mapping
    let mapping = mapping::Mapping::from_json(&data);
    debug!("{:#?}", mapping);
    index.mappings.insert(mapping_name.clone().to_owned(), mapping);

    return json_response!(status::Ok, "{\"acknowledged\": true}");
}
