use core::fmt;
use std::fmt::Display;

pub struct FieldRemapping {
    pub original_name: String,
    pub new_name: String,
}
impl Display for FieldRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original_name)
    }
}

pub struct ClosureFieldRemapping {
    pub original_name: String,
    pub new_name: String,
    pub variable_name: String,
}
impl Display for ClosureFieldRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original_name)
    }
}

pub struct ExcludeField {
    pub field_to_exclude: String,
}
impl Display for ExcludeField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.field_to_exclude)
    }
}
