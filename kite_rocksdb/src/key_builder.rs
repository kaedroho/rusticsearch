pub struct KeyBuilder {
    key: Vec<u8>,
}


impl KeyBuilder {
    pub fn new() -> KeyBuilder {
        KeyBuilder {
            key: Vec::new(),
        }
    }

    pub fn with_capacity(size: usize) -> KeyBuilder {
        KeyBuilder {
            key: Vec::with_capacity(size),
        }
    }

    pub fn stored_field_value(segment: u32, doc_ord: u16, field_ord: u32, value_type: &[u8]) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b'v');
        kb.push_string(segment.to_string().as_bytes());
        kb.separator();
        kb.push_string(doc_ord.to_string().as_bytes());
        kb.separator();
        kb.push_string(field_ord.to_string().as_bytes());
        kb.separator();
        kb.push_string(value_type);
        kb
    }

    pub fn segment_stored_values_prefix(segment: u32) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b'v');
        kb.push_string(segment.to_string().as_bytes());
        kb.separator();
        kb
    }

    pub fn primary_key_index(key: &[u8]) -> KeyBuilder {
        let mut kb = KeyBuilder::with_capacity(1 + key.len());
        kb.push_char(b'k');
        kb.push_string(key);
        kb
    }

    pub fn term_dict_mapping(term: &[u8]) -> KeyBuilder {
        let mut kb = KeyBuilder::with_capacity(1 + term.len());
        kb.push_char(b't');
        kb.push_string(term);
        kb
    }

    pub fn segment_active(segment: u32) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b'a');
        kb.push_string(segment.to_string().as_bytes());
        kb
    }

    pub fn segment_dir_list(segment: u32, field_ord: u32, term_ord: u32) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b'd');
        kb.push_string(field_ord.to_string().as_bytes());
        kb.separator();
        kb.push_string(term_ord.to_string().as_bytes());
        kb.separator();
        kb.push_string(segment.to_string().as_bytes());
        kb
    }

    pub fn segment_stat_prefix(segment: u32) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b's');
        kb.push_string(segment.to_string().as_bytes());
        kb.separator();
        kb
    }

    pub fn segment_stat(segment: u32, name: &[u8]) -> KeyBuilder {
        let mut kb = KeyBuilder::segment_stat_prefix(segment);
        kb.push_string(name);
        kb
    }

    pub fn segment_stat_term_doc_frequency_stat_name(field_ord: u32, term_ord: u32) -> Vec<u8> {
        let mut stat_name = Vec::new();
        for c in b"tdf" {
            stat_name.push(*c);
        }

        stat_name.push(b'-');

        for c in field_ord.to_string().as_bytes() {
            stat_name.push(*c);
        }

        stat_name.push(b'-');

        for c in term_ord.to_string().as_bytes() {
            stat_name.push(*c);
        }

        stat_name
    }

    pub fn segment_stat_total_field_tokens_stat_name(field_ord: u32) -> Vec<u8> {
        let mut stat_name = Vec::new();
        for c in b"fttok" {
            stat_name.push(*c);
        }

        stat_name.push(b'-');

        for c in field_ord.to_string().as_bytes() {
            stat_name.push(*c);
        }

        stat_name
    }

    pub fn segment_stat_total_field_docs_stat_name(field_ord: u32) -> Vec<u8> {
        let mut stat_name = Vec::new();
        for c in b"ftdoc" {
            stat_name.push(*c);
        }

        stat_name.push(b'-');

        for c in field_ord.to_string().as_bytes() {
            stat_name.push(*c);
        }

        stat_name
    }

    pub fn segment_del_list(segment: u32) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b'x');
        kb.push_string(segment.to_string().as_bytes());
        kb
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key[..]
    }

    #[inline]
    pub fn push_char(&mut self, c: u8) {
        if c == b'/' || c == b'\\' {
            self.key.push(b'\\');
        }
        self.key.push(c);
    }

    pub fn push_string(&mut self, s: &[u8]) {
        for c in s {
            self.push_char(*c);
        }
    }

    pub fn separator(&mut self) {
        self.key.push(b'/');
    }
}
