use serde_json;

use super::{put};


#[test]
fn test_new_index_with_default_settings() {

    let response = put("/new_index", Some(&json!({}))).unwrap();

    assert!(false);
}
