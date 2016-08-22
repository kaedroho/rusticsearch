use std::collections::HashMap;
use std::ops::Deref;


#[derive(Debug, Clone)]
pub enum FieldType {
    Text,
    PlainString,
    I64,
    DateTime,
}


#[derive(Debug, Clone)]
pub struct FieldInfo {
    field_type: FieldType,
}


impl FieldInfo {
    pub fn new(field_type: FieldType) -> FieldInfo {
        FieldInfo {
            field_type: field_type,
        }
    }
}


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct FieldRef(u32);


#[derive(Debug, Clone)]
pub struct Schema {
    next_field_id: u32,
    fields: HashMap<FieldRef, FieldInfo>,
}


impl Schema {
    pub fn new() -> Schema {
        Schema {
            next_field_id: 1,
            fields: HashMap::new(),
        }
    }

    fn new_field_ref(&mut self) -> FieldRef {
        let field_ref = FieldRef(self.next_field_id);
        self.next_field_id += 1;

        field_ref
    }

    pub fn add_field(&mut self, field_type: FieldType) -> FieldRef {
        let field_ref = self.new_field_ref();
        let field_info = FieldInfo::new(field_type);

        self.fields.insert(field_ref, field_info);

        field_ref
    }
}


impl Deref for Schema {
    type Target = HashMap<FieldRef, FieldInfo>;

    fn deref(&self) -> &HashMap<FieldRef, FieldInfo> {
        &self.fields
    }
}
