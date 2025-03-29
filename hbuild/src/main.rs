use std::{net::{SocketAddr, TcpListener}, process::Command};

use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use helixdb::protocol::{request::Request, response::Response};
use socket2::{Domain, Socket, Type};

async fn download_s3_folder(
    client: &Client,
    bucket: &str,
    prefix: &str,
    local_path: &str,
) -> Result<()> {
    // List objects in the bucket with the given prefix
    let objects = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await?;

    if let Some(contents) = objects.contents {
        for object in contents {
            if let Some(key) = &object.key {
                // Create the local file path
                let file_name = key.split('/').last().unwrap_or(key);
                let local_file_path = format!("{}/{}", local_path, file_name);

                // Get the object
                let get_obj = client.get_object().bucket(bucket).key(key).send().await?;

                // Create the local file and write the contents
                let data = get_obj.body.collect().await?;
                std::fs::write(&local_file_path, data.into_bytes())?;
            }
        }
    }

    Ok(())
}

// make sure build is run in sudo mode
#[tokio::main]
async fn main() -> Result<(), AdminError> {
    // Initialize AWS SDK
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = Client::new(&config);

    // run server on specified port
    let port = std::env::var("PORT").unwrap_or("443".to_string());

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None)
        .map_err(|e| AdminError::AdminConnectionError("Failed to create socket".to_string(), e))?;

    // Set socket options
    socket.set_recv_buffer_size(32 * 1024).map_err(|e| {
        AdminError::AdminConnectionError("Failed to set recv buffer".to_string(), e)
    })?;
    socket.set_send_buffer_size(32 * 1024).map_err(|e| {
        AdminError::AdminConnectionError("Failed to set send buffer".to_string(), e)
    })?;

    // Enable reuse
    socket.set_reuse_address(true).map_err(|e| {
        AdminError::AdminConnectionError("Failed to set reuse address".to_string(), e)
    })?;

    // Bind and listen
    socket
        .bind(&addr.into())
        .map_err(|e| AdminError::AdminConnectionError("Failed to bind".to_string(), e))?;
    socket
        .listen(1024)
        .map_err(|e| AdminError::AdminConnectionError("Failed to listen".to_string(), e))?;

    let listener: TcpListener = socket.into();
    let s3_client_clone = s3_client.clone();

    tokio::spawn(async move {
        let result: Result<(), AdminError> = async {
            let (mut conn, _) = listener.accept().unwrap();
            let request = Request::from_stream(&conn).unwrap();
            let mut response = Response::new();

            // pull latest query files from s3
            let bucket = std::env::var("S3_BUCKET").unwrap_or("helix-queries".to_string());
            let prefix = std::env::var("S3_PREFIX")
                .map_err(|e| AdminError::S3Error("Failed to get S3 prefix".to_string(), e))?;
            let local_path = std::env::var("LOCAL_QUERY_PATH").unwrap_or("/tmp/queries".to_string());

            // Create local directory if it doesn't exist
            std::fs::create_dir_all(&local_path).unwrap();

            // Download files from S3
            if let Err(e) = download_s3_folder(&s3_client_clone, &bucket, &prefix, &local_path).await {
                eprintln!("Failed to download files from S3: {:?}", e);
                response.status = 500;
                response.body = format!("Failed to download files from S3: {:?}", e).into_bytes();
            } else {
                // Run helix compile command
                let compile_result = Command::new("helix")
                    .arg("compile")
                    .arg("--output")
                    .arg("~/.helix/repo/helix-container/src")
                    .output();

                // build db
                let build_result = Command::new("cargo")
                    .arg("build")
                    .arg("--release")
                    .arg("--target-dir")
                    .arg("~/.helix/bin")
                    .arg("--bin")
                    .arg("helix-server")
                    .output();

                match compile_result {
                    Ok(output) if output.status.success() => {
                        // Restart the helix service
                        let restart_result = Command::new("sudo")
                            .arg("systemctl")
                            .arg("restart")
                            .arg("helix")
                            .output();

                        match restart_result {
                            Ok(output) if output.status.success() => {
                                response.status = 200;
                                response.body = "Successfully compiled queries and restarted helix service".as_bytes().to_vec();
                            }
                            Ok(output) => {
                                response.status = 500;
                                response.body = format!("Failed to restart helix service: {}", String::from_utf8_lossy(&output.stderr)).into_bytes();
                            }
                            Err(e) => {
                                response.status = 500;
                                response.body = format!("Failed to execute systemctl command: {:?}", e).into_bytes();
                            }
                        }
                    }
                    Ok(output) => {
                        response.status = 500;
                        response.body = format!("Failed to compile queries: {}", String::from_utf8_lossy(&output.stderr)).into_bytes();
                    }
                    Err(e) => {
                        response.status = 500;
                        response.body = format!("Failed to execute helix compile command: {:?}", e).into_bytes();
                    }
                }
            }

            // Send response back to client
            response.send(&mut conn).unwrap();
            Ok(())
        }
        .await;

        if let Err(e) = result {
            eprintln!("Error in handler: {:?}", e);
        }
    });

    Ok(())
}

#[derive(Debug)]
pub enum AdminError {
    AdminConnectionError(String, std::io::Error),
    S3Error(String, std::env::VarError),
}



// replace binary 
// run