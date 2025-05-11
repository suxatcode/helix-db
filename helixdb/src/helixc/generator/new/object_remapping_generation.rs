use core::fmt;
use std::fmt::Display;

use super::traversal_steps::Traversal;

pub struct TraversalRemapping {
    pub variable_name: String,
    pub new_field: String,
    pub new_value: Traversal,
}
impl Display for TraversalRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "|{}| {{ traversal_remapping!({}, {} => {}) }}",
            self.variable_name, self.variable_name, self.new_field, self.new_value
        )
    }
}
pub struct FieldRemapping {
    pub variable_name: String,
    pub original_name: String,
    pub new_name: String,
}
impl Display for FieldRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "|{}| {{ field_remapping!({}, {} => {}) }}",
            self.variable_name, self.variable_name, self.original_name, self.new_name
        )
    }
}
pub struct ExcludeField {
    pub fields_to_exclude: Vec<String>,
}
impl Display for ExcludeField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            ".map(|item| {{ exclude_fields!({}) }})",
            self.fields_to_exclude.join(", ")
        )
    }
}

pub struct ClosureFieldRemapping {
    pub variable_name: String,
    pub object_remappings: Box<ObjectRemapping>,
}
impl Display for ClosureFieldRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // assert that closure mappings are not excluded fields
        assert!(!matches!(
            *self.object_remappings,
            ObjectRemapping::ExcludeField(_)
        ));
        write!(
            f,
            "|{}| {{ {} }}",
            self.variable_name, *self.object_remappings
        )
    }
}

pub enum ObjectRemapping {
    FieldRemapping(FieldRemapping),
    ClosureFieldRemapping(ClosureFieldRemapping),
    ExcludeField(ExcludeField),
    TraversalRemapping(TraversalRemapping),
}
impl Display for ObjectRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ".map({})", self)
    }
}
