macro_rules! get_index_or_404 {
    ($cluster_metadata: expr, $index_name: expr) => {{
        let index_ref = match $cluster_metadata.names.find_canonical($index_name) {
            Some(index_ref) => index_ref,
            None => {
                return None;
            }
        };

        match $cluster_metadata.indices.get(&index_ref) {
            Some(index) => index,
            None => {
                return None;
            }
        }
    }}
}


macro_rules! get_index_or_404_mut {
    ($cluster_metadata: expr, $index_name: expr) => {{
        let index_ref = match $cluster_metadata.names.find_canonical($index_name) {
            Some(index_ref) => index_ref,
            None => {
                return None;
            }
        };

        match $cluster_metadata.indices.get_mut(&index_ref) {
            Some(index) => index,
            None => {
                return None;
            }
        }
    }}
}
