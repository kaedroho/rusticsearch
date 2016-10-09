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

    pub fn stored_field_value(chunk: u32, field_ord: u32, doc_ord: u16) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b'v');
        kb.push_string(field_ord.to_string().as_bytes());
        kb.separator();
        kb.push_string(chunk.to_string().as_bytes());
        kb.separator();
        kb.push_string(doc_ord.to_string().as_bytes());
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

    pub fn chunk_active(chunk: u32) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b'a');
        kb.push_string(chunk.to_string().as_bytes());
        kb
    }

    pub fn chunk_dir_list(chunk: u32, field_ord: u32, term_ord: u32) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b'd');
        kb.push_string(field_ord.to_string().as_bytes());
        kb.separator();
        kb.push_string(term_ord.to_string().as_bytes());
        kb.separator();
        kb.push_string(chunk.to_string().as_bytes());
        kb
    }

    pub fn chunk_stat(chunk: u32, name: &[u8]) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b's');
        kb.push_string(name);
        kb.separator();
        kb.push_string(chunk.to_string().as_bytes());
        kb
    }

    pub fn chunk_del_list(chunk: u32) -> KeyBuilder {
        let mut kb = KeyBuilder::new();
        kb.push_char(b'x');
        kb.push_string(chunk.to_string().as_bytes());
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