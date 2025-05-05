use helixdb::{
    helix_engine::graph_core::config::Config,
    helixc::{
        generator::generator::CodeGenerator,
        parser::helix_parser::HelixParser,
    },
    ingestion_engine::{
        postgres_ingestion::PostgresIngestor,
        sql_ingestion::SqliteIngestor,
    },
};
use crate::{
    utils::*,
    args::HelixCLI,
    instance_manager::InstanceManager,
};
use clap::Parser;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    fs,
};
use colored::*;

pub mod args;
mod instance_manager;
mod utils;

fn main() {
    let args = HelixCLI::parse();

    match args.command {
        args::CommandType::Deploy(command) => {
            match Command::new("cargo").output() {
                Ok(_) => {},
                Err(_) => {
                    println!("{}", "Cargo is not installed".red().bold());
                    return;
                }
            }

            match check_helix_installation() {
                Ok(_) => {},
                Err(_) => {
                    println!("{}", "Helix is not installed. Please run `helix install` first.".red().bold());
                    return;
                }
            };

            let path = get_cfg_deploy_path(command.path).unwrap();

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
                None => 6969, // TODO: no more 6969
            };

            let port = match find_available_port(start_port) {
                Some(port) => {
                    if port != start_port {
                        println!(
                            "{} {} {} {} {}",
                            "Port".yellow(),
                            start_port,
                            "is in use, using port".yellow(),
                            port,
                            "instead".yellow(),
                        );
                    }
                    port
                }
                None => {
                    println!("{} {}", "No available ports found starting from".red().bold(), start_port);
                    return;
                }
            };

            let local = command.local;

            // TODO: remove this once remote instance is supported
            if !local {
                println!("Building for remote instance is not supported yet, use --local flag to build for local machine");
                println!("Example: helix deploy --local");
                println!();
                println!("Building for local machine will be available within the next 2 weeks");
                return;
            }

            let files = match check_and_read_files(&path) {
                Ok(files) if !files.is_empty() => files,
                Ok(_) => {
                    println!("{}", "No queries found, nothing to compile".red().bold());
                    return;
                }
                Err(e) => {
                    println!("{}", e);
                    return;
                }
            };

            let spinner = create_spinner("Compiling Helix queries");

            let num_files = files.len();
            let mut successes = HashMap::new();
            let mut errors = HashMap::new();
            let mut code = String::new();
            let mut generator = CodeGenerator::new();
            code.push_str(&generator.generate_headers());

            for file in files {
                let contents = match fs::read_to_string(file.path()) {
                    Ok(contents) => contents,
                    Err(e) => {
                        spinner.finish_with_message(format!("{}", "Failed to read files".red().bold()));
                        println!("{}", e);
                        return;
                    }
                };

                match HelixParser::parse_source(&contents) {
                    Ok(source) => {
                        code.push_str(&generator.generate_source(&source));
                        successes.insert(
                            file.file_name()
                                .to_string_lossy()
                                .into_owned(),
                            source,
                        );
                    }
                    Err(e) => {
                        errors.insert(
                            file.file_name()
                                .to_string_lossy()
                                .into_owned(),
                            e,
                        );
                    }
                }
            }

            if !errors.is_empty() {
                spinner.finish_with_message(format!("{}", "Failed to compile some queries".red().bold()));
                for (name, error) in errors {
                    println!("\t{}: {}", name, error);
                }
                return;
            }

            spinner.finish_with_message(
                format!(
                    "{} {} {}",
                    "Successfully compiled".green().bold(),
                    num_files,
                    "query files".green().bold()
                )
            );

            let cache_dir = PathBuf::from(&output);
            fs::create_dir_all(&cache_dir).unwrap();

            // if local overwrite queries file in ~/.helix/repo/helix-container/src/queries.rs
            if local {
                let spinner = create_spinner("Building Helix");
                let file_path = PathBuf::from(&output).join("src/queries.rs");
                match fs::write(file_path, code) {
                    Ok(_) => {
                        spinner.finish_with_message(format!("{}", "Successfuly wrote queries file".green().bold()));
                    }
                    Err(e) => {
                        spinner.finish_with_message(format!("{}", "Filaed to write queries file".red().bold()));
                        println!("└── {}", e);
                        return;
                    }
                }

                // copy config.hx.json to ~/.helix/repo/helix-db/helix-container/config.hx.json
                let config_path = PathBuf::from(&output).join("src/config.hx.json");
                fs::copy(PathBuf::from(path + "/config.hx.json"), config_path).unwrap();

                // check rust code
                let mut runner = Command::new("cargo");
                runner
                    .arg("check")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .current_dir(PathBuf::from(&output));

                match runner.output() {
                    Ok(_) => {}
                    Err(e) => {
                        spinner.finish_with_message(format!("{}", "Failed to check Rust code".red().bold()));
                        println!("└── {}", e);
                        return;
                    }
                }

                let mut runner = Command::new("cargo");
                runner
                    .arg("build")
                    .arg("--release")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .current_dir(PathBuf::from(&output)); // TODO: build only in helix-container/ dir

                match runner.output() {
                    Ok(_) => {
                        spinner.finish_with_message(format!("{}", "Successfully built Helix".green().bold()));
                    }
                    Err(e) => {
                        spinner.finish_with_message(format!("{}", "Failed to build Helix".red().bold()));
                        println!("└── {}", e);
                        return;
                    }
                }

                let spinner = create_spinner("Starting Helix instance");
                let instance_manager = InstanceManager::new().unwrap();

                let binary_path = dirs::home_dir()
                    .map(|path| path.join(".helix/repo/helix-db/target/release/helix-container"))
                    .unwrap();

                let endpoints: Vec<String> = successes
                    .values()
                    .flat_map(|source| source.queries.iter().map(|q| to_snake_case(&q.name)))
                    .collect();

                match instance_manager.start_instance(&binary_path, port, endpoints) {
                    Ok(instance) => {
                        spinner.finish_with_message(format!("{}", "Successfully started Helix instance".green().red()));
                        println!("└── Instance ID: {}", instance.id);
                        println!("└── Port: {}", instance.port);
                        println!("└── Available endpoints:");
                        for endpoint in instance.available_endpoints {
                            println!("    └── /{}", endpoint);
                        }
                    }
                    Err(e) => {
                        spinner.finish_with_message(format!("{}", "Failed to start Helix instance".red().green()));
                        println!("└── {}", e);
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
                        println!("\tID: {}", instance.id);
                        println!("\t  Port: {}", instance.port);
                        println!("\t  Started at: {}", instance.started_at);
                        println!("\t  Available endpoints:");
                        for endpoint in instance.available_endpoints {
                            println!("\t    /{}", endpoint);
                        }
                        println!();
                    }
                }
                Err(e) => {
                    println!("{} {}", "Failed to list instances:".red().bold(), e);
                }
            }
        }

        args::CommandType::Stop(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            match instance_manager.list_instances() {
                Ok(instances) => {
                    if instances.is_empty() {
                        println!("No running Helix instances");
                        return;
                    }
                    if command.all {
                        match instance_manager.stop_all_instances() {
                            Ok(_) => println!("{}", "Stopped all Helix instances".green().bold()),
                            Err(e) => println!("{} {}", "Failed to stop instances:".red().bold(), e),
                        }
                    } else if let Some(instance_id) = command.instance_id {
                        match instance_manager.stop_instance(&instance_id) {
                            Ok(_) => println!("{} {}", "Stopped instance".green().bold(), instance_id),
                            Err(e) => println!("{} {}", "Failed to stop instance:".red().bold(), e),
                        }
                    } else {
                        println!("Please specify --all or provide an instance ID");
                    }

                }
                Err(e) => {
                    println!("{} {}", "Failed to find instances:".red().bold(), e);
                }
            }
        }

        args::CommandType::Start(command) => {
            let instance_manager = InstanceManager::new().unwrap();
            let spinner = create_spinner("\tStarting Helix instance");

            match instance_manager.restart_instance(&command.instance_id) {
                Ok(Some(instance)) => {
                    spinner.finish_with_message(format!("{}", "Successfully restarted Helix instance".green().bold()));
                    println!("└── Instance ID: {}", instance.id);
                    println!("└── Port: {}", instance.port);
                    println!("└── Available endpoints:");
                    for endpoint in instance.available_endpoints {
                        println!("    └── /{}", endpoint);
                    }
                }
                Ok(None) => {
                    spinner.finish_with_message(format!("{}", "Instance not found or binary missing".red().bold()));
                    println!("└── Could not find instance with ID: {}", command.instance_id);
                }
                Err(e) => {
                    spinner.finish_with_message(format!("{}", "Failed to restart instance".red().bold()));
                    println!("└── {}", e);
                }
            }
        }

        args::CommandType::Compile(command) => {
            let path = if let Some(p) = &command.path {
                p
            } else {
                println!("\tNo path provided, defaulting to '{}'", DB_DIR);
                DB_DIR
            };

            let output = match &command.output {
                Some(output) => output.to_owned(),
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
                println!("\tNo queries found, nothing to compile");
                return;
            }

            let source = match compile_hql_to_source(&files) {
                Ok(source) => source,
                Err(e) => {
                    println!("{}", "Failed to parse source".red().bold());
                    println!("|");
                    println!("└─── {}", e);
                    return;
                }
            };

            println!("{} {:?}", "Successfully parsed source".green().bold(), source);

            let mut code = String::new();
            let mut generator = CodeGenerator::new();
            code.push_str(&generator.generate_headers());
            code.push_str(&generator.generate_source(&source));

            // write source to file
            let file_path = PathBuf::from(&output).join("queries.rs");
            fs::write(file_path, code).unwrap();
            println!("{} {}", "Successfully compiled queries to".green().bold(), output);
        }

        args::CommandType::Check(command) => {
            let path = if let Some(p) = &command.path {
                p
            } else {
                println!("\tNo path provided, defaulting to '{}'", DB_DIR);
                DB_DIR
            };

            let files = match check_and_read_files(&path) {
                Ok(files) => files,
                Err(e) => {
                    println!("\t❌ {}", e);
                    return;
                }
            };

            if files.is_empty() {
                println!("\tNo queries found, nothing to compile");
                return;
            }

            match compile_hql_to_source(&files) {
                Ok(_) => {
                    println!("\t✅ Successfully parsed source");
                }
                Err(e) => {
                    println!("\n\t❌ Failed to parse source");
                    println!("\t|");
                    println!("\t└─ {}", e);
                    return;
                }
            }
        }

        args::CommandType::Install(command) => {
            // check if cargo is installed
            let mut runner = Command::new("cargo");
            runner.arg("check");
            match runner.output() {
                Ok(_) => {}
                Err(e) => {
                    println!("\t❌ Cargo is not installed");
                    println!("\t|");
                    println!("\t└── {}", e);
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
                }
            }

            let repo_path = match command.path {
                Some(path) => {
                    let path = PathBuf::from(path);
                    if !path.is_dir() {
                        println!("\t❌ Path is not a directory");
                        return;
                    }
                    if !path.exists() {
                        println!("\t❌ Path does not exist");
                        return;
                    }
                    path
                }
                None => {
                    // check if helix repo exists
                    let home_dir = match dirs::home_dir() {
                        Some(dir) => dir,
                        None => {
                            println!("\t❌ Could not determine home directory");
                            return;
                        }
                    };
                    home_dir.join(".helix/repo")
                }
            };

            if repo_path.clone().join("helix-db").exists()
                && repo_path.clone().join("helix-db").is_dir()
            {
                println!(
                    "\t✅ Helix repo already exists at {}",
                    repo_path.join("helix-db").display()
                );
                return;
            }

            // Create the directory structure if it doesn't exist
            match fs::create_dir_all(&repo_path) {
                Ok(_) => println!(
                    "\t✅ Created directory structure at {}",
                    repo_path.display()
                ),
                Err(e) => {
                    println!("\t❌ Failed to create directory structure");
                    println!("\t|");
                    println!("\t└── {}", e);
                    return;
                }
            }

            let mut runner = Command::new("git");
            runner.arg("clone");
            runner.arg("--branch");
            runner.arg("graph-engine-pipelining");
            runner.arg("https://github.com/HelixDB/helix-db.git");
            runner.current_dir(&repo_path);

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
            let path = if let Some(p) = command.path {
                p
            } else {
                println!("\tNo path provided, defaulting to '{}'", DB_DIR);
                DB_DIR.to_string()
            };

            let _ = match check_and_read_files(&path) {
                Ok(files) => files,
                Err(e) => {
                    println!("\t❌ {}", e);
                    return;
                }
            };

            //let temp_dir = TempDir::new().unwrap();
            // parse
            // interpret
            // generate rust code
            // run against rocksdb

            match command.test {
                Some(test) => println!("\tTesting: {:?}", test),
                None => println!("\t❌ No test provided"),
            }
        }
        args::CommandType::Init(command) => {
            println!("\tInitialising Helix project...");
            let path = match command.path {
                Some(path) => PathBuf::from(path),
                None => PathBuf::from(DB_DIR),
            };
            let path_str = path.to_str().unwrap();

            let _ = match check_and_read_files(path_str) {
                Ok(files) if !files.is_empty() => {
                    println!("\t❌ Queries already exist in {}", path_str);
                    return;
                }
                Ok(_) => {}
                Err(_) => {}
            };

            fs::create_dir_all(&path).unwrap();

            let schema_path = path.join("schema.hx");
            fs::write(
                &schema_path,
                DEFAULT_SCHEMA,
            ).unwrap();

            let main_path = path.join("queries.hx");
            fs::write(
                main_path,
                DEFAULT_QUERIES,
            ).unwrap();

            let config_path = path.join("config.hx.json");
            fs::write(config_path, Config::init_config()).unwrap();

            println!("\t✅ Helix project initialised at {}", path.display());
        }

        args::CommandType::Ingest(command) => {
            match command.db_type.as_str() {
                "sqlite" => {
                    let path_str = command.db_url; // Database path for SQLite
                    let instance = command.instance;

                    let path = Path::new(&path_str);
                    if !path.exists() {
                        println!("❌The file '{}' does not exist", path.display());
                        return;
                    }

                    let valid_extensions = [".sqlite", ".db", ".sqlite3"];
                    let is_valid_extension = path
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| valid_extensions.iter().any(|&valid_ext| valid_ext == ext))
                        .unwrap_or(false);

            if !is_valid_extension {
                println!(
                    "❌The file '{}' must have a .sqlite, .db, or .sqlite3 extension.",
                    path.display()
                );
                return;
            }

                    let instance_manager = InstanceManager::new().unwrap();
                    match instance_manager.list_instances() {
                        Ok(instances) => {
                            if instances.is_empty() {
                                println!("There are no running Helix instances!");
                                return;
                            }
                            let mut is_valid_instance = false;
                            for iter_instance in instances {
                                if iter_instance.id == instance {
                                    is_valid_instance = true;
                                    break;
                                }
                            }
                            if !is_valid_instance {
                                println!("No Helix instance found with id: '{}'!", instance);
                                return;
                            } else {
                                println!("Helix instance found with id: '{}'!", instance);
                            }
                        }
                        Err(e) => {
                            println!("Error while searching for Helix instances: {}", e);
                        }
                    }

                    let mut ingestor = SqliteIngestor::new(&path_str, None, 5).unwrap();
                    // TODO: Add ingestion logic
                }
                "pg" | "postgres" => {
                    let spinner = create_spinner("Connecting to PostgreSQL database...");

                    // Create output directory if specified
                    let output_dir = command.output_dir.as_deref().unwrap_or("./");
                    if !Path::new(output_dir).exists() {
                        fs::create_dir_all(output_dir).unwrap_or_else(|e| {
                            finish_spinner_with_message(&spinner, false, &format!("Failed to create output directory: {}", e));
                            std::process::exit(1);
                        });
                    }

                    // Run the PostgreSQL ingestion
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    let result = rt.block_on(async {
                        let mut ingestor = PostgresIngestor::new(&command.db_url, Some(command.instance.clone()), command.batch_size, command.use_ssl).await;

                        match ingestor {
                            Ok(mut ingestor) => {
                                finish_spinner_with_message(&spinner, true, "Connected to PostgreSQL database");

                                let dump_spinner = create_spinner("Dumping data to JSONL files...");
                                match ingestor.dump_to_json(output_dir).await {
                                    Ok(_) => {
                                        finish_spinner_with_message(&dump_spinner, true, "Successfully dumped data to JSONL files");

                                        // Create schema file
                                        let schema_path = Path::new(output_dir).join("schema.hx");
                                        println!("Schema file created at: {}", schema_path.display());

                                        println!("PostgreSQL ingestion completed successfully!");
                                        println!("Press ENTER to open the Helix dashboard in your browser...");
                                        let mut input = String::new();
                                        std::io::stdin().read_line(&mut input).unwrap();

                                        #[cfg(target_os = "macos")]
                                        {
                                            if let Err(e) = std::process::Command::new("open")
                                                .arg("https://helix-db.com/dashboard")
                                                .spawn()
                                            {
                                                println!("Failed to open url");
                                                println!("Please visit https://helix-db.com/dashboard");
                                            }
                                        }

                                        #[cfg(not(target_os = "macos"))]
                                        {
                                            if let Err(e) = open::that("https://helix-db.com/dashboard") {
                                                println!("Failed to open url");
                                                println!("Please visit https://helix-db.com/dashboard");
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        finish_spinner_with_message(&dump_spinner, false, &format!("Failed to dump data: {}", e));
                                        std::process::exit(1);
                                    }
                                }
                            },
                            Err(e) => {
                                finish_spinner_with_message(&spinner, false, &format!("Failed to connect to PostgreSQL: {}", e));
                                std::process::exit(1);
                            }
                        }
                    });
                }
                _ => {
                    println!("❌ Invalid database type. Must be either 'sqlite' or 'pg/postgres'");
                    return;
                }
            }
        }
    }
}