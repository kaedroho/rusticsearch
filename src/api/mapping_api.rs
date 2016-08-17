use std::io::Read;

use rustc_serialize::json::Json;

use mapping;

use api::persistent;
use api::iron::prelude::*;
use api::iron::status;
use api::router::Router;
use api::utils::json_response;


pub fn view_put_mapping(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");

    // Lock index array
    let mut indices = system.indices.write().unwrap();

    // Get index
    let mut index = get_index_or_404_mut!(indices, *index_name);

    // Load data from body
    let data = json_from_request_body!(req);

    let data = match data {
        Some(data) => data,
        None => {
            // TODO: Better error
            return Ok(json_response(status::BadRequest, "{\"acknowledged\": false}"));
        }
    };

    let data = data.as_object().unwrap().get(*mapping_name).unwrap();

    // Insert mapping
    let mapping = mapping::Mapping::from_json(&data);
    debug!("{:#?}", mapping);
    let is_updating = index.mappings.contains_key(*mapping_name);
    index.mappings.insert(mapping_name.clone().to_owned(), mapping);

    if is_updating {
        system.log.info("[api] updated mapping", b!("index" => *index_name, "mapping" => *mapping_name));
    } else {
        system.log.info("[api] created mapping", b!("index" => *index_name, "mapping" => *mapping_name));
    }

    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}
