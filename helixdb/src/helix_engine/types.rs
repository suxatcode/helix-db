use core::fmt;
use std::{fmt::format, net::AddrParseError, str::Utf8Error, string::FromUtf8Error};

use heed3::Error;
use crate::helixc::parser::parser_methods::ParserError;
use crate::protocol::traversal_value::TraversalValueError;

#[derive(Debug)]
pub enum GraphError {
    Io(std::io::Error),
    GraphConnectionError(String, std::io::Error),
    StorageConnectionError(String, std::io::Error),
    StorageError(String),
    TraversalError(String),
    ConversionError(String),
    EdgeNotFound,
    NodeNotFound,
    Default,
    New(String),
    Empty,
    MultipleNodesWithSameId,
    MultipleEdgesWithSameId,
    InvalidNode,
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphError::Io(e) => write!(f, "IO error: {}", e),
            GraphError::StorageConnectionError(msg, e) => {
                write!(f, "Error: {}", format!("{} {}", msg, e))
            },
            GraphError::GraphConnectionError(msg, e) => {
                write!(f, "Error: {}", format!("{} {}", msg, e))
            },
            GraphError::TraversalError(msg) => write!(f, "Traversal error: {}", msg),
            GraphError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            GraphError::ConversionError(msg ) => write!(f, "Conversion error: {}", msg),
            GraphError::EdgeNotFound => write!(f, "Edge not found"),
            GraphError::NodeNotFound => write!(f, "Node not found"),
            GraphError::New(msg) => write!(f, "Graph error: {}", msg),
            GraphError::Default => write!(f, "Graph error"),
            GraphError::Empty => write!(f, "No Error"),
            GraphError::MultipleNodesWithSameId => write!(f, "Multiple nodes with same id"),
            GraphError::MultipleEdgesWithSameId => write!(f, "Multiple edges with same id"),
            GraphError::InvalidNode => write!(f, "Invalid node"),
        }
    }
}

// impl From<rocksdb::Error> for GraphError {
//     fn from(error: rocksdb::Error) -> Self {
//         GraphError::New(error.into_string())
//     }
// }

impl From<Error> for GraphError {
    fn from(error: Error) -> Self {
        GraphError::StorageError(error.to_string())
    }
}

impl From<std::io::Error> for GraphError {
    fn from(error: std::io::Error) -> Self {
        GraphError::Io(error)
    }
}


impl From<AddrParseError> for GraphError {
    fn from(error: AddrParseError) -> Self {
        GraphError::ConversionError(format!("AddrParseError: {}", error.to_string()))
    }
}

impl From<sonic_rs::Error> for GraphError {
    fn from(error: sonic_rs::Error) -> Self {
        GraphError::ConversionError(format!("sonic error: {}" , error.to_string()))
    }
}

impl From<FromUtf8Error> for GraphError {
    fn from(error: FromUtf8Error) -> Self {
        GraphError::ConversionError(format!("FromUtf8Error: {}", error.to_string()))
    }
}

impl From<&'static str> for GraphError {
    fn from(error: &'static str) -> Self {
        GraphError::New(error.to_string())
    }
}

impl From<String> for GraphError {
    fn from(error: String) -> Self {
        GraphError::New(error.to_string())
    }
}



impl From<Box<bincode::ErrorKind>> for GraphError {
    fn from(error: Box<bincode::ErrorKind>) -> Self {
        GraphError::ConversionError(format!("bincode error: {}", error.to_string()))
    }
}

impl From<ParserError> for GraphError {
    fn from(error: ParserError) -> Self {
        GraphError::ConversionError(format!("ParserError: {}", error.to_string()))
    }
}

impl From<Utf8Error> for GraphError {
    fn from(error: Utf8Error) -> Self {
        GraphError::ConversionError(format!("Utf8Error: {}", error.to_string()))
    }
}

impl From<uuid::Error> for GraphError {
    fn from(error: uuid::Error) -> Self {
        GraphError::ConversionError(format!("uuid error: {}", error.to_string()))
    }
}   


impl From<TraversalValueError> for GraphError {
    fn from(error: TraversalValueError) -> Self {
        GraphError::ConversionError(format!("TraversalValueError: {}", error.to_string()))
    }
}