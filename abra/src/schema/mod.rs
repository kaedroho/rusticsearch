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
    name: String,
    field_type: FieldType,
}


impl FieldInfo {
    pub fn new(name: String, field_type: FieldType) -> FieldInfo {
        FieldInfo {
            name: name,
            field_type: field_type,
        }
    }
}


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct FieldRef(u32);


#[derive(Debug)]
pub enum AddFieldError {
    FieldAlreadyExists(String),
}


#[derive(Debug, Clone)]
pub struct Schema {
    next_field_id: u32,
    fields: HashMap<FieldRef, FieldInfo>,
    field_names: HashMap<String, FieldRef>,
}


impl Schema {
    pub fn new() -> Schema {
        Schema {
            next_field_id: 1,
            fields: HashMap::new(),
            field_names: HashMap::new(),
        }
    }

    fn new_field_ref(&mut self) -> FieldRef {
        let field_ref = FieldRef(self.next_field_id);
        self.next_field_id += 1;

        field_ref
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType) -> Result<FieldRef, AddFieldError> {
        if self.field_names.contains_key(&name) {
            return Err(AddFieldError::FieldAlreadyExists(name));
        }

        let field_ref = self.new_field_ref();
        let field_info = FieldInfo::new(name.clone(), field_type);

        self.fields.insert(field_ref, field_info);
        self.field_names.insert(name, field_ref);

        Ok(field_ref)
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<FieldRef> {
        self.field_names.get(name).cloned()
    }
}


impl Deref for Schema {
    type Target = HashMap<FieldRef, FieldInfo>;

    fn deref(&self) -> &HashMap<FieldRef, FieldInfo> {
        &self.fields
    }
}
