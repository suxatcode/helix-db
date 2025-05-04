use std::{
    path::PathBuf,
    net::{SocketAddr, TcpListener},
    io::ErrorKind,
};

const DB_DIR: &str = "helixdb-cfg/";

pub fn check_helix_installation() -> Result<PathBuf, String> {
    let home_dir = dirs::home_dir().ok_or("Could not determine home directory")?;
    let repo_path = home_dir.join(".helix/repo/helix-db");
    let container_path = repo_path.join("helix-container");
    let cargo_path = container_path.join("Cargo.toml");

    if !repo_path.exists() ||
        !repo_path.is_dir() ||
        !container_path.exists() ||
        !container_path.is_dir()  ||
        !cargo_path.exists()
    {
        return Err("run `helix install` first.".to_string());
    }

    Ok(container_path)
}

pub fn get_cfg_deploy_path(cmd_path: Option<String>) -> Result<String, CliError> {
    if let Some(path) = cmd_path {
        return Ok(path);
    }

    let cwd = ".";
    let files = match check_and_read_files(cwd) {
        Ok(files) => files,
        Err(_) => {
            return Ok(DB_DIR.to_string());
        }
    };

    if !files.is_empty() {
        return Ok(cwd.to_string());
    }

    Ok(DB_DIR.to_string())
}

pub fn find_available_port(start_port: u16) -> Option<u16> {
    let mut port = start_port;
    while port < 65535 {
        let addr = format!("0.0.0.0:{}", port).parse::<SocketAddr>().unwrap();
        match TcpListener::bind(addr) {
            Ok(listener) => {
                drop(listener);
                let localhost = format!("127.0.0.1:{}", port).parse::<SocketAddr>().unwrap();
                match TcpListener::bind(localhost) {
                    Ok(local_listener) => {
                        drop(local_listener);
                        return Some(port);
                    }
                    Err(e) => {
                        if e.kind() != ErrorKind::AddrInUse {
                            return None;
                        }
                        port += 1;
                        continue;
                    }
                }
            }
            Err(e) => {
                if e.kind() != ErrorKind::AddrInUse {
                    return None;
                }
                port += 1;
                continue;
            }
        }
    }
    None
}




pub fn create_spinner(message: &str) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{prefix:>10.cyan.bold} {spinner:.green} {wide_msg}")
            .unwrap(),
    );
    spinner.set_message(message.to_string());
    spinner.set_prefix("🔄");
    spinner.enable_steady_tick(Duration::from_millis(80));
    spinner
}

pub fn finish_spinner_with_message(spinner: &ProgressBar, success: bool, message: &str) {
    let prefix = if success { "✅" } else { "❌" };
    spinner.set_prefix(prefix);
    spinner.finish_with_message(message.to_string());
}

pub fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut prev_is_uppercase = false;

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 && !prev_is_uppercase {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
            prev_is_uppercase = true;
        } else {
            result.push(c);
            prev_is_uppercase = false;
        }
    }
    result
}

pub fn update_cli(spinner: &ProgressBar) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("curl")
        .args(&["-sSL", "https://install.helix-db.com"])
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| {
            finish_spinner_with_message(&spinner, false, "Failed to start curl");
            e
        })?
        .stdout
        .ok_or_else(|| {
            finish_spinner_with_message(&spinner, false, "Failed to capture curl output");
            "Failed to capture curl output"
        })?;

    let status = Command::new("bash").stdin(status).status().map_err(|e| {
        finish_spinner_with_message(&spinner, false, "Failed to execute install script");
        e
    })?;

    if status.success() {
        finish_spinner_with_message(&spinner, true, "Successfully updated Helix CLI");
        Ok(())
    } else {
        finish_spinner_with_message(&spinner, false, "Update script failed");
        Err(format!("Exit code: {}", status).into())
    }
}

pub fn check_and_read_files(path: &str) -> Result<Vec<DirEntry>, CliError> {
    // check there is schema and at least one query
    if !fs::read_dir(&path)
        .map_err(CliError::Io)?
        .any(|file| file.unwrap().file_name() == "schema.hx")
    {
        println!("{}", CliError::from("\t❌ No schema file found"));
        // return Err(CliError::from("No schema file found"));
    }

    if !fs::read_dir(&path)
        .map_err(CliError::Io)?
        .any(|file| file.unwrap().file_name() == "config.hx.json")
    {
        println!("{}", CliError::from("\t❌ No config.hx.json file found"));
        // return Err(CliError::from("No config.hx.json file found"));
    }

    let files: Vec<DirEntry> = fs::read_dir(&path)?
        .filter_map(|entry| entry.ok())
        .filter(|file| file.file_name().to_string_lossy().ends_with(".hx"))
        .collect();

    // Check for query files (exclude schema.hx)
    let has_queries = files
        .iter()
        .any(|file| file.file_name() != "schema.hx");
    if !has_queries {
        return Err(CliError::from("No query files (.hx) found"));
    }

    Ok(files)
}

pub fn check_is_dir(path: &str) -> bool {
    match fs::metadata(&path) {
        Ok(metadata) => metadata.is_dir(),
        Err(e) => {
            println!("{}", CliError::Io(e));
            return false;
        }
    }
}

pub fn format_rust_file(file_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("rustfmt").arg(file_path).status()?;

    if !status.success() {
        return Err(format!("rustfmt failed with exit code: {}", status).into());
    }

    Ok(())
}

pub fn check_hql_files(files: &Vec<DirEntry>) -> Result<(), CliError> {
    for file in files {
        let contents = fs::read_to_string(file.path()).unwrap();
        match HelixParser::parse_source(&contents) {
            Ok(_) => (),
            Err(e) => {
                return Err(CliError::from(format!("{}\n", e)));
            }
        }
    }
    Ok(())
}

pub fn compile_hql_to_source(files: &Vec<DirEntry>) -> Result<Source, CliError> {
    // let numb_of_files = files.len();
    // let mut code = String::new();
    // let mut generator = CodeGenerator::new();

    let contents: String = files
        .iter()
        .map(|file| -> String {
            match fs::read_to_string(file.path()) {
                Ok(contents) => contents,
                Err(e) => {
                    panic!("{}", e);
                }
            }
        })
        .fold(String::new(), |acc, contents| acc + &contents);

    let source = match HelixParser::parse_source(&contents) {
        Ok(source) => {
            // println!("{:?}", parser);
            source
        }
        Err(e) => {
            return Err(CliError::from(format!("{}\n", e)));
        }
    };

    Ok(source)
}