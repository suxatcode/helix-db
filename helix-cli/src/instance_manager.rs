use dirs;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InstanceInfo {
    pub id: String,
    pub pid: u32,
    pub port: u16,
    pub started_at: String,
    pub available_endpoints: Vec<String>,
    pub binary_path: PathBuf,
    pub label: String,
    pub running: bool,
}

pub struct InstanceManager {
    instances_file: PathBuf,
    cache_dir: PathBuf,
    logs_dir: PathBuf,
}

impl InstanceManager {
    pub fn new() -> io::Result<Self> {
        let home_dir = dirs::home_dir().expect("Could not find home directory");
        let helix_dir = home_dir.join(".helix");
        let cache_dir = helix_dir.join("cached_builds");
        let logs_dir = helix_dir.join("logs");
        fs::create_dir_all(&helix_dir)?;
        fs::create_dir_all(&cache_dir)?;
        fs::create_dir_all(&logs_dir)?;

        Ok(Self {
            instances_file: helix_dir.join("instances.json"),
            cache_dir,
            logs_dir,
        })
    }

    pub fn init_start_instance(
        &self,
        source_binary: &Path,
        port: u16,
        endpoints: Vec<String>,
    ) -> io::Result<InstanceInfo> {
        let instance_id = Uuid::new_v4().to_string();
        let cached_binary = self.cache_dir.join(&instance_id);
        fs::copy(source_binary, &cached_binary)?;

        // make sure data dir exists
        // make it .cached_builds/data/instance_id/
        let data_dir = self.cache_dir.join("data").join(&instance_id);
        fs::create_dir_all(&data_dir)?;

        // Create log file for this instance
        let log_file = self.logs_dir.join(format!("instance_{}.log", instance_id));
        let log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(log_file)?;
        let error_log_file = self
            .logs_dir
            .join(format!("instance_{}_error.log", instance_id));
        let error_log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(error_log_file)?;

        let mut command = Command::new(&cached_binary);
        command.env("PORT", port.to_string());
        command
            .env("HELIX_DAEMON", "1")
            .env("HELIX_DATA_DIR", data_dir.to_str().unwrap())
            .env("HELIX_PORT", port.to_string())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(error_log_file));

        let child = command.spawn()?;

        let instance = InstanceInfo {
            id: instance_id,
            pid: child.id(),
            port,
            started_at: chrono::Local::now().to_rfc3339(),
            available_endpoints: endpoints,
            binary_path: cached_binary,
            label: "".to_string(),
            running: true,
        };

        let mut instances = self.list_instances()?;
        instances.push(instance.clone());
        let _ = self.save_instances(&instances);

        Ok(instance)
    }

    pub fn start_instance(&self, instance_id: &str) -> io::Result<Option<InstanceInfo>> {
        if let Some(mut instance) = self.get_instance(instance_id)? {
            if !instance.binary_path.exists() { return Ok(None); }
            //let data_dir = instance.binary_path.clone().join("data");
            // make sure data dir exists
            let data_dir = self.cache_dir.join("data").join(&instance_id);
            fs::create_dir_all(&data_dir)?;

            // create log file for this instance
            let log_file = self.logs_dir.join(format!("instance_{}.log", instance_id));
            let log_file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(log_file)?;

            let mut command = Command::new(&instance.binary_path);
            command.env("PORT", instance.port.to_string());
            command
                .env("HELIX_DAEMON", "1")
                .env("HELIX_DATA_DIR", data_dir.to_str().unwrap())
                .env("HELIX_PORT", instance.port.to_string())
                .stdout(Stdio::from(log_file.try_clone()?))
                .stderr(Stdio::from(log_file));

            let child = command.spawn()?;

            instance.running = true;

            // TODO: update "self.instances" here

            Ok(Some(instance))
        } else {
            Ok(None)
        }
    }

    fn get_instance(&self, instance_id: &str) -> io::Result<Option<InstanceInfo>> {
        let instances = self.list_instances()?;
        Ok(instances.into_iter().find(|i| i.id == instance_id))
    }

    pub fn list_instances(&self) -> io::Result<Vec<InstanceInfo>> {
        if !self.instances_file.exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(&self.instances_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        if contents.is_empty() {
            return Ok(Vec::new());
        }

        let instances: Vec<InstanceInfo> = sonic_rs::from_str(&contents)?;
        Ok(instances)
    }

    pub fn stop_instance(&self, instance_id: &str) -> io::Result<bool> {
        let mut instances = self.list_instances()?;
        if let Some(pos) = instances.iter().position(|i| i.id == instance_id) {
            if !instances[pos].running {
                return Ok(false);
            }
            instances[pos].running = false;
            #[cfg(unix)]
            unsafe {
                libc::kill(instances[pos].pid as i32, libc::SIGTERM);
            }
            #[cfg(windows)]
            {
                use windows::Win32::System::Threading::{
                    OpenProcess, TerminateProcess, PROCESS_TERMINATE,
                };
                let handle = unsafe { OpenProcess(PROCESS_TERMINATE, false.into(), instance.pid) };
                if let Ok(handle) = handle {
                    unsafe { TerminateProcess(handle, 0) };
                }
            }
            let _ = self.save_instances(&instances)?;
        }
        Ok(true)
    }

    pub fn running_instances(&self) -> io::Result<bool> {
        let instances = self.list_instances()?;
        for instance in instances {
            if instance.running {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn stop_all_instances(&self) -> io::Result<()> {
        let instances = self.list_instances()?;
        for instance in instances {
            let _ = self.stop_instance(&instance.id)?;
        }
        Ok(())
    }

    fn save_instances(&self, instances: &[InstanceInfo]) -> io::Result<()> {
        let contents = sonic_rs::to_string(instances)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.instances_file)?;
        file.write_all(contents.as_bytes())?;
        Ok(())
    }
}

