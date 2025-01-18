use crate::args::CliError;
use std::path::Path;
use std::process::Command;
pub struct RustRunner {
    working_dir: String,
}

impl RustRunner {
    pub fn new(working_dir: String) -> Self {
        Self { working_dir }
    }

    pub fn compile_and_run(&self, file_name: &str) -> Result<(), CliError> {
        let compile_status = Command::new("rustc")
            .arg(file_name)
            .current_dir(&self.working_dir)
            .status()?;

        if !compile_status.success() {
            return Err(CliError::from("Compilation failed"));
        }

        let executable = Path::new(file_name).file_stem().unwrap().to_str().unwrap();

        let run_status = Command::new(format!("./{}", executable))
            .current_dir(&self.working_dir)
            .status()?;

        if !run_status.success() {
            return Err(CliError::from("Execution failed"));
        }

        Ok(())
    }

    pub fn run_cargo_project(&self, project_path: &str) -> std::io::Result<()> {
        Command::new("cargo")
            .args([
                "run",
                "--manifest-path",
                &format!("{}/Cargo.toml", project_path),
            ])
            .current_dir(&self.working_dir)
            .status()
            .map(|_| ())
    }
}