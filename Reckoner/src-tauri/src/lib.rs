use std::sync::Mutex;
use std::time::Duration;
use tauri::Manager;
use tauri::async_runtime::spawn;
use tauri_plugin_shell::ShellExt;
use tauri_plugin_shell::process::CommandEvent;

struct SidecarState(Mutex<Option<tauri_plugin_shell::process::CommandChild>>);

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(SidecarState(Mutex::new(None)))
        .setup(|app| {
            let handle = app.handle().clone();
            spawn(async move {
                let exe_dir = std::env::current_exe()
                    .expect("failed to get exe path")
                    .parent()
                    .expect("failed to get exe dir")
                    .to_path_buf();

                let log_path = exe_dir.join("reckoner_sidecar.log");
                let _ = std::fs::write(&log_path, format!("exe_dir: {:?}\n", exe_dir));

                let substrates_dir = handle.path().app_local_data_dir()
                    .expect("failed to get app local data dir")
                    .join("substrates");

                let _ = std::fs::create_dir_all(&substrates_dir);

                let _ = std::fs::write(&log_path, format!(
                    "exe_dir: {:?}\nsubstrates_dir: {:?}\n",
                    exe_dir, substrates_dir
                ));

                eprintln!("[sidecar] exe_dir: {:?}", exe_dir);
                eprintln!("[sidecar] substrates_dir: {:?}", substrates_dir);

                let sidecar_command = match handle.shell().sidecar("reckoner_api") {
                    Ok(cmd) => cmd
                        .current_dir(&exe_dir)
                        .env("SNF_SUBSTRATES_DIR", &substrates_dir),
                    Err(e) => {
                        let _ = std::fs::write(&log_path, format!(
                            "exe_dir: {:?}\nsubstrates_dir: {:?}\nfailed to create sidecar: {}\n",
                            exe_dir, substrates_dir, e
                        ));
                        eprintln!("[sidecar] failed to create command: {}", e);
                        return;
                    }
                };

                match sidecar_command.spawn() {
                    Ok((mut rx, child)) => {
                        let _ = std::fs::write(&log_path, format!(
                            "exe_dir: {:?}\nsubstrates_dir: {:?}\nspawned successfully\n",
                            exe_dir, substrates_dir
                        ));
                        eprintln!("[sidecar] spawned successfully");
                        handle
                            .state::<SidecarState>()
                            .0
                            .lock()
                            .unwrap()
                            .replace(child);
                        tauri::async_runtime::spawn(async move {
                            while let Some(event) = rx.recv().await {
                                match event {
                                    CommandEvent::Stdout(line) => eprintln!("[sidecar out] {}", String::from_utf8_lossy(&line)),
                                    CommandEvent::Stderr(line) => eprintln!("[sidecar err] {}", String::from_utf8_lossy(&line)),
                                    CommandEvent::Error(e) => eprintln!("[sidecar error] {}", e),
                                    CommandEvent::Terminated(status) => eprintln!("[sidecar terminated] {:?}", status),
                                    _ => {}
                                }
                            }
                        });
                    }
                    Err(e) => {
                        let _ = std::fs::write(&log_path, format!(
                            "exe_dir: {:?}\nsubstrates_dir: {:?}\nfailed to spawn: {}\n",
                            exe_dir, substrates_dir, e
                        ));
                        eprintln!("[sidecar] failed to spawn: {}", e);
                        return;
                    }
                }

                eprintln!("[sidecar] waiting for backend...");
                for _ in 0..120 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if let Ok(resp) = reqwest::get("http://localhost:8000/api/health").await {
                        if resp.status().is_success() {
                            eprintln!("[sidecar] backend ready");
                            if let Some(window) = handle.get_webview_window("main") {
                                let _ = window.show();
                            }
                            return;
                        }
                    }
                }
                eprintln!("[sidecar] backend did not start in time");
                if let Some(window) = handle.get_webview_window("main") {
                    let _ = window.show();
                }
            });
            Ok(())
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::Destroyed = event {
                if let Some(child) = window
                    .state::<SidecarState>()
                    .0
                    .lock()
                    .unwrap()
                    .take()
                {
                    let _ = child.kill();
                }
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}