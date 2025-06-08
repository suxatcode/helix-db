use crate::{
    helixc::parser::parser_methods::ParserError,
    protocol::traversal_value::TraversalValueError,
};
use core::fmt;
use heed3::Error as HeedError;
use sonic_rs::Error as SonicError;
use std::{
    net::AddrParseError,
    str::Utf8Error,
    string::FromUtf8Error
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GraphError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Graph connection error: {0} {1}")]
    GraphConnectionError(String, std::io::Error),
    #[error("Storage connection error: {0} {1}")]
    StorageConnectionError(String, std::io::Error),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Traversal error: {0}")]
    TraversalError(String),
    #[error("Conversion error: {0}")]
    ConversionError(String),
    #[error("Decode error: {0}")]
    DecodeError(String),
    #[error("Edge not found")]
    EdgeNotFound(u128),
    #[error("Node not found")]
    NodeNotFound(u128),
    #[error("Label not found")]
    LabelNotFound,
    #[error("Wrong traversal value")]
    WrongTraversalValue,
    #[error("Vector error: {0}")]
    VectorError(String),
    #[error("Default graph error")]
    Default,
    #[error("Graph error: {0}")]
    New(String),
    #[error("No error")]
    Empty,
    #[error("Multiple nodes with same id")]
    MultipleNodesWithSameId,
    #[error("Multiple edges with same id")]
    MultipleEdgesWithSameId,
    #[error("Invalid node")]
    InvalidNode,
    #[error("Config file not found")]
    ConfigFileNotFound,
    #[error("Slice length error")]
    SliceLengthError,
    #[error("Shortest path not found")]
    ShortestPathNotFound
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
            GraphError::ConversionError(msg) => write!(f, "Conversion error: {}", msg),
            GraphError::DecodeError(msg) => write!(f, "Decode error: {}", msg),
            GraphError::EdgeNotFound => write!(f, "Edge not found"),
            GraphError::NodeNotFound => write!(f, "Node not found"),
            GraphError::LabelNotFound => write!(f, "Label not found"),
            GraphError::WrongTraversalValue => write!(f, "Wrong traversal value"),
            GraphError::New(msg) => write!(f, "Graph error: {}", msg),
            GraphError::Default => write!(f, "Graph error"),
            GraphError::Empty => write!(f, "No Error"),
            GraphError::MultipleNodesWithSameId => write!(f, "Multiple nodes with same id"),
            GraphError::MultipleEdgesWithSameId => write!(f, "Multiple edges with same id"),
            GraphError::InvalidNode => write!(f, "Invalid node"),
            GraphError::ConfigFileNotFound => write!(f, "Config file not found"),
            GraphError::SliceLengthError => write!(f, "Slice length error"),
            GraphError::VectorError(msg) => write!(f, "Vector error: {}", msg),
            GraphError::ShortestPathNotFound => write!(f, "Shortest path not found"),
        }
    }
}

// impl From<rocksdb::Error> for GraphError {
//     fn from(error: rocksdb::Error) -> Self {
//         GraphError::New(error.into_string())
//     }
// }

impl From<HeedError> for GraphError {
    fn from(error: HeedError) -> Self {
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

impl From<SonicError> for GraphError {
    fn from(error: SonicError) -> Self {
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



impl From<bincode::Error> for GraphError {
    fn from(error: bincode::Error) -> Self {
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

impl From<VectorError> for GraphError {
    fn from(error: VectorError) -> Self {
        GraphError::VectorError(format!("VectorError: {}", error.to_string()))
    }
}

#[derive(Error, Debug)]
pub enum VectorError {
    #[error("Vector not found: {0}")]
    VectorNotFound(String),
    #[error("Invalid vector length")]
    InvalidVectorLength,
    #[error("Invalid vector data")]
    InvalidVectorData,
    #[error("Entry point not found")]
    EntryPointNotFound,
    #[error("Conversion error: {0}")]
    ConversionError(String),
    #[error("Vector core error: {0}")]
    VectorCoreError(String),
}

impl fmt::Display for VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VectorError::VectorNotFound(id) => write!(f, "Vector not found: {}", id),
            VectorError::InvalidVectorLength => write!(f, "Invalid vector length"),
            VectorError::InvalidVectorData => write!(f, "Invalid vector data"),
            VectorError::EntryPointNotFound => write!(f, "Entry point not found"),
            VectorError::ConversionError(msg) => write!(f, "Conversion error: {}", msg),
            VectorError::VectorCoreError(msg) => write!(f, "Vector core error: {}", msg),
        }
    }
}

impl From<HeedError> for VectorError {
    fn from(error: HeedError) -> Self {
        VectorError::VectorCoreError(format!("heed error: {}", error.to_string()))
    }
}

impl From<FromUtf8Error> for VectorError {
    fn from(error: FromUtf8Error) -> Self {
        VectorError::ConversionError(format!("FromUtf8Error: {}", error.to_string()))
    }
}

impl From<Utf8Error> for VectorError {
    fn from(error: Utf8Error) -> Self {
        VectorError::ConversionError(format!("Utf8Error: {}", error.to_string()))
    }
}

impl From<SonicError> for VectorError {
    fn from(error: SonicError) -> Self {
        VectorError::ConversionError(format!("SonicError: {}", error.to_string()))
    }
}

impl From<bincode::Error> for VectorError {
    fn from(error: bincode::Error) -> Self {
        VectorError::ConversionError(format!("bincode error: {}", error.to_string()))
    }
}
