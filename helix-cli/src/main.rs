use args::{CliError, HelixCLI};
use clap::Parser;
use helixc::{
    generator,
    parser::helix_parser::{HelixParser, Source},
};
use runner::RustRunner;
use std::{
    collections::HashMap,
    fs::{self, DirEntry},
};
use tempfile::TempDir;

pub mod args;
pub mod runner;

fn main() {
    let args = HelixCLI::parse();
    let mut source = Source::new();
    match args.command {
        args::CommandType::Lint(command) => {
            match command.path {
                Some(path) => {
                    // call parser
                    let files = match check_and_read_files(&path) {
                        Ok(files) => files,
                        Err(e) => {
                            println!("{}", e);
                            return;
                        }
                    };

                    if files.is_empty() {
                        println!("No queries found, nothing to compile");
                        return;
                    }

                    let numb_of_files = files.len();
                    let mut successes = HashMap::new();
                    let mut errors = HashMap::new();
                    for file in files {
                        let contents = match fs::read_to_string(file.path()) {
                            Ok(contents) => contents,
                            Err(e) => {
                                println!("{}", e);
                                return;
                            }
                        };
                        match HelixParser::parse_source(&contents) {
                            Ok(source) => {
                                successes.insert(
                                    file.file_name().to_string_lossy().into_owned(),
                                    source,
                                );
                                // println!("{:?}", parser);
                            }
                            Err(e) => {
                                errors.insert(file.file_name().to_string_lossy().into_owned(), e);
                            }
                        }
                    }

                    println!("\nLinted {} files!\n", numb_of_files);
                    successes
                        .iter()
                        .for_each(|(name, _)| println!("\t✅ {}: \tNo errors", name));
                    errors
                        .iter()
                        .for_each(|(name, error)| println!("\t❌ {}: \t{}", name, error));
                    println!();
                }
                None => {
                    // current directory
                    let files = match check_and_read_files(".") {
                        Ok(files) => files,
                        Err(e) => {
                            println!("{}", e);
                            return;
                        }
                    };

                    if files.is_empty() {
                        println!("No queries found, nothing to compile");
                        return;
                    }

                    let numb_of_files = files.len();
                    let mut successes = HashMap::new();
                    let mut errors = HashMap::new();
                    for file in files {
                        let contents = match fs::read_to_string(file.path()) {
                            Ok(contents) => contents,
                            Err(e) => {
                                println!("{}", e);
                                return;
                            }
                        };
                        match HelixParser::parse_source(&contents) {
                            Ok(source) => {
                                successes.insert(
                                    file.file_name().to_string_lossy().into_owned(),
                                    source,
                                );
                                // println!("{:?}", parser);
                            }
                            Err(e) => {
                                errors.insert(file.file_name().to_string_lossy().into_owned(), e);
                            }
                        }
                    }

                    println!("\nLinted {} files!\n", numb_of_files);
                    successes
                        .iter()
                        .for_each(|(name, _)| println!("\t✅ {}: \tNo errors", name));
                    errors
                        .iter()
                        .for_each(|(name, error)| println!("\t❌ {}: \t{}", name, error));
                    println!();
                }
            };
        }
        args::CommandType::Test(command) => {
            match command.path {
                Some(path) => {
                    // parse files
                    let files = match check_and_read_files(&path) {
                        Ok(files) => files,
                        Err(e) => {
                            println!("{}", e);
                            return;
                        }
                    };

                    let temp_dir = TempDir::new().unwrap();
                    
                    // parse

                    // interpret 

                    // generate rust code

                    // run against rocksdb

                },
                None => println!("No path provided"),
            };

            match command.test {
                Some(test) => println!("Testing: {:?}", test),
                None => println!("No test provided"),
            }
        }
    }
}

fn check_and_read_files(path: &str) -> Result<Vec<DirEntry>, CliError> {
    // check there is schema and at least one query
    if !fs::read_dir(&path)
        .map_err(CliError::Io)?
        .any(|file| file.unwrap().file_name() == "schema.hx")
    {
        println!("{}", CliError::from("No schema file found"));
        // return Err(CliError::from("No schema file found"));
    }

    let files: Vec<DirEntry> = fs::read_dir(&path)?
        .filter_map(|entry| entry.ok())
        .filter(|file| file.file_name().to_string_lossy().ends_with(".hx"))
        .collect();

    Ok(files)
}

fn check_is_dir(path: &str) -> bool {
    match fs::metadata(&path) {
        Ok(metadata) => metadata.is_dir(),
        Err(e) => {
            println!("{}", CliError::Io(e));
            return false;
        }
    }
}
