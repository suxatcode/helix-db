use args::{CliError, HelixCLI};
use clap::Parser;
use helixdb::helixc::{
    // generator,
    generator::generator::CodeGenerator,
    parser::helix_parser::{HelixParser, Source},
};
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    collections::HashMap,
    fs::{self, DirEntry},
    path,
    process::{Command, Stdio},
    thread::sleep,
    time::Duration,
    net::{TcpListener, SocketAddr},
    io::ErrorKind,
};
use tempfile::TempDir;

use std::path::PathBuf;
pub mod args;
mod instance_manager;

use instance_manager::InstanceManager;

fn create_spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(Duration::from_millis(100));
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈")
            .template("{spinner:.green.bold} {msg}")
            .unwrap(),
    );
    pb.set_message(format!("\t{}", msg));
    pb
}

fn find_available_port(start_port: u16) -> Option<u16> {
    let mut port = start_port;
    while port < 65535 {
        // Try binding to 0.0.0.0 first since that's what the server will use
        let addr = format!("0.0.0.0:{}", port).parse::<SocketAddr>().unwrap();
        match TcpListener::bind(addr) {
            Ok(_) => {
                // Also check localhost to be thorough
                let localhost = format!("127.0.0.1:{}", port).parse::<SocketAddr>().unwrap();
                match TcpListener::bind(localhost) {
                    Ok(_) => return Some(port),
                    Err(e) => {
                        if e.kind() != ErrorKind::AddrInUse {
                            return None;
                        }
                        // Port is in use on localhost, try next port
                        port += 1;
                        continue;
                    }
                }
            }
            Err(e) => {
                if e.kind() != ErrorKind::AddrInUse {
                    return None;
                }
                // Port is in use, try next port
                port += 1;
                continue;
            }
        }
    }
    None
}

fn main() {
    let args = HelixCLI::parse();
    match args.command {
        args::CommandType::Deploy(command) => {
            // check if cargo is installed
            let mut runner = Command::new("cargo");
            match runner.output() {
                Ok(_) =>{},
                Err(e) => {
                    println!("\t❌ Cargo is not installed");
                    println!("\t|");
                    println!("\t└── {}", e);
                    return;
                }
            }
            // check if helix repo exists
            let mut runner = Command::new("ls");
            runner.arg(".helix/repo/helix-container");
            match runner.output() {
                Ok(_) => {},
                Err(_) => {
                    println!("\t❌ Helix repo does not exist");
                    println!("\t|");
                    println!("\t└── Please run `helix install` to install the helix repo");
                    return;
                }
            }

            // path to project
            let path = match command.path {
                Some(path) => path,
                None => ".".to_string(),
            };

            // output path
            let output = match command.output {
                Some(output) => output,
                None => dirs::home_dir()
                    .map(|path| {
                        path.join(".helix/repo/helix-db/helix-container")
                            .to_string_lossy()
                            .into_owned()
                    })
                    .unwrap_or_else(|| "./.helix/repo/helix-db/helix-container".to_string()),
            };

            let start_port = match command.port {
                Some(port) => port,
                None => 6969,
            };

            let port = match find_available_port(start_port) {
                Some(port) => {
                    if port != start_port {
                        println!("\t⚠️  Port {} is in use, using port {} instead", start_port, port);
                    }
                    port
                },
                None => {
                    println!("\t❌ No available ports found starting from {}", start_port);
                    return;
                }
            };

            // local flag
            let local = command.local;

            // TODO: remove this once remote instance is supported
            if !local {
                println!("Building for remote instance is not supported yet, use --local flag to build for local machine");
                println!("Example: helix build --local");
                println!();
                println!("Building for local machine will be available within the next 2 weeks");
                return;
            }

            // check and read files
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

            let spinner = create_spinner("Compiling Helix queries");
            
            // number of files
            let numb_of_files = files.len();
            let mut successes = HashMap::new();
            let mut errors = HashMap::new();
            let mut code = String::new();
            let mut generator = CodeGenerator::new();
            code.push_str(&generator.generate_headers());
            for file in files {
                let contents = match fs::read_to_string(file.path()) {
                    Ok(contents) => contents,
                    Err(e) => {
                        spinner.finish_with_message("❌ Failed to read files");
                        println!("{}", e);
                        return;
                    }
                };
                match HelixParser::parse_source(&contents) {
                    Ok(source) => {
                        code.push_str(&generator.generate_source(&source));
                        successes.insert(file.file_name().to_string_lossy().into_owned(), source);
                    }
                    Err(e) => {
                        errors.insert(file.file_name().to_string_lossy().into_owned(), e);
                    }
                }
            }

            if !errors.is_empty() {
                spinner.finish_with_message("❌ Failed to compile some queries");
                for (name, error) in errors {
                    println!("\t❌ {}: {}", name, error);
                }
                return;
            }

            spinner.finish_with_message(format!("✅ Successfully compiled {} queries", numb_of_files));

            let cache_dir = PathBuf::from(&output);
            fs::create_dir_all(&cache_dir).unwrap();

            // if local overwrite queries file in ~/.helix/repo/helix-container/src/queries.rs
            if local {
                let spinner = create_spinner("Building Helix");
                
                let file_path = PathBuf::from(&output).join("src/queries.rs");
                match fs::write(file_path, code) {
                    Ok(_) => {},
                    Err(e) => {
                        spinner.finish_with_message("❌ Failed to write queries file");
                        println!("\t|");
                        println!("\t└── {}", e);
                        return;
                    }
                }

                // check rust code
                let mut runner = Command::new("cargo");
                runner
                    .arg("check")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .current_dir(PathBuf::from(&output));
                match runner.output() {
                    Ok(_) =>{},
                    Err(e) => {
                        spinner.finish_with_message("❌ Failed to check Rust code");
                        println!("\t|");
                        println!("\t└── {}", e);
                        return;
                    }
                }

                // run rust code
                let mut runner = Command::new("cargo");
                runner
                    .arg("build")
                    .arg("--release")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .current_dir(PathBuf::from(&output));

                match runner.output() {
                    Ok(_) => {
                        spinner.finish_with_message("✅ Successfully built Helix");
                    },
                    Err(e) => {
                        spinner.finish_with_message("\t❌ Failed to build Helix");
                        println!("\t|");
                        println!("\t└── {}", e);
                        return;
                    }
                }

                // After successful build and ping test, start the instance in background
                let spinner = create_spinner("Starting Helix instance");
                let instance_manager = InstanceManager::new().unwrap();
               
                let binary_path = dirs::home_dir()
                    .map(|path| path.join(".helix/repo/helix-db/target/release/helix-container"))
                    .unwrap();

                // Collect query names from successes
                let endpoints: Vec<String> = successes.values()
                    .flat_map(|source| source.queries.iter().map(|q| q.name.clone()))
                    .collect();

                match instance_manager.start_instance(&binary_path, port, endpoints) {
                    Ok(instance) => {
                        spinner.finish_with_message("✅ Successfully started Helix instance");
                        println!(" ");
                        println!("\t└── Instance ID: {}", instance.id);
                        println!("\t└── Port: {}", instance.port);
                        println!("\t└── Available endpoints:");
                        for endpoint in instance.available_endpoints {
                            println!("\t    └── /{}", endpoint);
                        }
                    }
                    Err(e) => {
                        spinner.finish_with_message("\t❌ Failed to start Helix instance");
                        println!("\t└── {}", e);
                    }
                }
            }
        }
        args::CommandType::Instances(_) => {
            let instance_manager = InstanceManager::new().unwrap();
            match instance_manager.list_instances() {
                Ok(instances) => {
                    if instances.is_empty() {
                        println!("No running Helix instances");
                        return;
                    }
                    println!("Running Helix instances:");
                    for instance in instances {
                        println!("ID: {}", instance.id);
                        println!("  Port: {}", instance.port);
                        println!("  Started at: {}", instance.started_at);
                        println!("  Available endpoints:");
                        for endpoint in instance.available_endpoints {
                            println!("    /{}", endpoint);
                        }
                        println!();
                    }
                }
                Err(e) => {
                    println!("Failed to list instances: {}", e);
                }
            }
        }
        args::CommandType::Stop(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            if command.all {
                match instance_manager.stop_all_instances() {
                    Ok(_) => println!("Stopped all Helix instances"),
                    Err(e) => println!("Failed to stop instances: {}", e),
                }
            } else if let Some(instance_id) = command.instance_id {
                match instance_manager.stop_instance(&instance_id) {
                    Ok(_) => println!("Stopped instance {}", instance_id),
                    Err(e) => println!("Failed to stop instance: {}", e),
                }
            } else {
                println!("Please specify --all or provide an instance ID");
            }
        }
        args::CommandType::Compile(command) => {
            let path = match command.path {
                Some(path) => {
                    // call parser
                    path
                }
                None => {
                    // current directory
                    ".".to_string()
                }
            };
            let output = match command.output {
                Some(output) => output,
                None => dirs::home_dir()
                    .map(|path| {
                        path.join(".helix/cache/generated/")
                            .to_string_lossy()
                            .into_owned()
                    })
                    .unwrap_or_else(|| "./.helix/cache/generated/".to_string()),
            };
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
            let mut code = String::new();
            let mut generator = CodeGenerator::new();
            code.push_str(&generator.generate_headers());
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
                        // println!("{:?}", parser);
                        code.push_str(&generator.generate_source(&source));

                        // write to ~/.helix/cache/generated/

                        successes.insert(file.file_name().to_string_lossy().into_owned(), source);
                    }
                    Err(e) => {
                        errors.insert(file.file_name().to_string_lossy().into_owned(), e);
                    }
                }
            }
            let cache_dir = PathBuf::from(&output);
            fs::create_dir_all(&cache_dir).unwrap();

            let file_path = cache_dir.join(format!("queries.rs",));
            fs::write(file_path, code).unwrap();
            println!("\nCompiled {} files!\n", numb_of_files);
            successes
                .iter()
                .for_each(|(name, _)| println!("\t✅ {}: \tNo errors", name));
            errors
                .iter()
                .for_each(|(name, error)| println!("\t❌ {}: \t{}", name, error));
            println!();
        }

        args::CommandType::Check(command) => {
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
        args::CommandType::Install(_command) => {
            // check if cargo is installed
            let mut runner = Command::new("cargo");
            runner.arg("check");
            match runner.output() {
                Ok(_) => {}
                Err(e) => {
                    println!("\t❌ Cargo is not installed");
                    println!("\t|");
                    println!("\t└── {}", e);
                    return;
                }
            }

            // check if git is installed
            let mut runner = Command::new("git");
            runner.arg("version");
            match runner.output() {
                Ok(_) => {}
                Err(e) => {
                    println!("\t❌ Git is not installed");
                    println!("\t|");
                    println!("\t└── {}", e);
                    return;
                }
            }

            // check if helix repo exists
            let mut runner = Command::new("ls");
            runner.arg(".helix/repo/helix-container");
            match runner.output() {
                Ok(_) => {
                    println!("\t✅ Helix repo already exists");
                    return;
                }
                Err(e) => {}
            }

            println!("Installing Helix repo...");
            let home_dir = match dirs::home_dir() {
                Some(dir) => dir,
                None => {
                    println!("\t❌ Could not determine home directory");
                    return;
                }
            };
            let repo_dir = home_dir.join(".helix/repo");

            // Create the directory structure if it doesn't exist
            match fs::create_dir_all(&repo_dir) {
                Ok(_) => println!("\t✅ Created directory structure at {}", repo_dir.display()),
                Err(e) => {
                    println!("\t❌ Failed to create directory structure");
                    println!("\t|");
                    println!("\t└── {}", e);
                    return;
                }
            }

            let mut runner = Command::new("git");
            runner.arg("clone");
            runner.arg("https://github.com/HelixDB/helix-db.git");
            runner.current_dir(&repo_dir);

            match runner.output() {
                Ok(_) => {
                    let home_dir = dirs::home_dir().unwrap();
                    println!(
                        "\t✅ Helix repo installed at {}",
                        home_dir.join(".helix/repo/").to_string_lossy()
                    );
                    println!("\t|");
                    println!("\t└── To get started, begin writing helix queries in your project.");
                    println!("\t|");
                    println!("\t└── Then run `helix check --path <path-to-project>` to check your queries.");
                    println!("\t|");
                    println!("\t└── Then run `helix deploy --path <path-to-project> --local` to build your queries.");
                }
                Err(e) => {
                    println!("\t❌ Failed to install Helix repo");
                    println!("\t|");
                    println!("\t└── {}", e);
                    return;
                }
            }
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
                }
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
