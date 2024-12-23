use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct ProjectGenerator {
    project_name: String,
    output_dir: PathBuf,
    dependencies: Vec<(String, String)>,
    queries: HashMap<String, String>,
}

impl ProjectGenerator {
    pub fn new(project_name: &str, output_dir: impl Into<PathBuf>) -> Self {
        Self {
            project_name: project_name.to_string(),
            output_dir: output_dir.into(),
            dependencies: vec![],
            queries: HashMap::new(),
        }
    }

    pub fn with_queries(mut self, queries: HashMap<String, String>) -> Self {
        self.queries = queries;
        self
    }

    pub fn add_dependency(&mut self, name: &str, version: &str) {
        self.dependencies
            .push((name.to_string(), version.to_string()));
    }

    pub fn generate(&self) -> std::io::Result<()> {
        // Create project directory
        let project_dir = self.output_dir.join(&self.project_name);
        fs::create_dir_all(&project_dir)?;
        fs::create_dir_all(project_dir.join("src"))?;

        // Generate Cargo.toml
        self.generate_cargo_toml(&project_dir)?;

        // Generate lib.rs
        self.generate_lib_rs(&project_dir)?;

        // Generate traversal module
        self.generate_traversal_module(&project_dir)?;

        Ok(())
    }

    /// toml
    /// protocol = { path = "../protocol" }
    /// helix-gateway = { path = "../helix-gateway" }
    /// get_routes = { path = "../get_routes" }
    fn generate_cargo_toml(&self, project_dir: &Path) -> std::io::Result<()> {
        let mut cargo_toml = fs::File::create(project_dir.join("Cargo.toml"))?;

        writeln!(cargo_toml, "[package]")?;
        writeln!(cargo_toml, "name = \"{}\"", self.project_name)?;
        writeln!(cargo_toml, "version = \"0.1.0\"")?;
        writeln!(cargo_toml, "edition = \"2021\"")?;
        writeln!(cargo_toml)?;

        writeln!(cargo_toml, "[dependencies]")?;
        writeln!(cargo_toml, "inventory = \"0.3.15\"")?;
        writeln!(
            cargo_toml,
            "helix-engine = {{ path = \"../helix-engine\" }}"
        )?;
        writeln!(
            cargo_toml,
            "helix-gateway = {{ path = \"../helix-gateway\" }}"
        )?;
        writeln!(cargo_toml, "protocol = {{ path = \"../protocol\" }}")?;
        writeln!(cargo_toml, "get_routes = {{ path = \"../get_routes\" }}")?;

        for (name, version) in &self.dependencies {
            writeln!(cargo_toml, "{} = \"{}\"", name, version)?;
        }

        writeln!(cargo_toml).unwrap();
        writeln!(cargo_toml, "[profile.release]").unwrap();
        writeln!(cargo_toml, "strip = \"debuginfo\"").unwrap();
        writeln!(cargo_toml, "lto = true").unwrap();
        writeln!(cargo_toml, "opt-level = \"z\"").unwrap();

        Ok(())
    }

    fn generate_lib_rs(&self, project_dir: &Path) -> std::io::Result<()> {
        let mut lib_rs = fs::File::create(project_dir.join("src/lib.rs"))?;

        writeln!(lib_rs, "pub mod traversals;")?;
        writeln!(lib_rs)?;
        Ok(())
    }

    fn generate_traversal_module(&self, project_dir: &Path) -> std::io::Result<()> {
        let mut traversals_rs = fs::File::create(project_dir.join("src/traversals.rs"))?;
        writeln!(
            traversals_rs,
            "use helix_engine::graph_core::traversal::TraversalBuilder;"
        )?;
        writeln!(traversals_rs, "use helix_engine::graph_core::traversal_steps::{{SourceTraversalSteps, TraversalSteps}};")?;
        writeln!(traversals_rs, "use get_routes::handler;")?;
        writeln!(
            traversals_rs,
            "use helix_gateway::router::router::{{HandlerInput, RouterError}};"
        )?;
        writeln!(traversals_rs, "use protocol::response::Response;")?;

        writeln!(traversals_rs)?;
        self.queries.iter().for_each(|(_, query)| {
            // match writeln!(traversals_rs, "{}", query_body) {
            //     Ok(_) => (),
            //     Err(err) => return Err(err)
            // }
            writeln!(traversals_rs, "#[handler]").unwrap();
            writeln!(traversals_rs, "{}", query).unwrap();
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_project_generation() {
        let temp_dir = TempDir::new().unwrap();

        let traversal_code = r#"
            pub fn test_function2(input: &HandlerInput, response: &mut Response) -> Result<(), RouterError> {
                let storage = &input.graph.storage;
                let mut traversal = TraversalBuilder::new(vec![]);
                traversal.v(storage);
                traversal.out(storage, "knows");
                response.body = input.graph.result_to_utf8(&traversal);
                Ok(())
            }
            "#
        .to_string();

        let mut queries = HashMap::new();
        queries.insert("test_query".to_string(), traversal_code);

        let generator =
            ProjectGenerator::new("test_project", temp_dir.path()).with_queries(queries);

        generator.generate().unwrap();

        // Verify project structure
        assert!(temp_dir.path().join("test_project/Cargo.toml").exists());
        assert!(temp_dir.path().join("test_project/src/lib.rs").exists());
        assert!(temp_dir
            .path()
            .join("test_project/src/traversals.rs")
            .exists());

        // Verify Cargo.toml contents
        let cargo_toml =
            fs::read_to_string(temp_dir.path().join("test_project/Cargo.toml")).unwrap();
        assert!(cargo_toml.contains("[package]"));
        assert!(cargo_toml.contains("name = \"test_project\""));
        assert!(cargo_toml.contains("helix-engine"));

        // Verify lib.rs contents
        let lib_rs = fs::read_to_string(temp_dir.path().join("test_project/src/lib.rs")).unwrap();
        assert!(lib_rs.contains("mod traversal"));
        assert!(lib_rs.contains("pub use traversal::test_query"));
    }
}
