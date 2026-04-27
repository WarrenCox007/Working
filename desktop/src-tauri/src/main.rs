#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

mod issuecentre;

use issuecentre::{EndpointMappingResult, MonitorConfig, MonitorService, MonitorSnapshot};
use std::sync::Arc;
use tauri::State;

struct AppState {
    monitor: Arc<MonitorService>,
}

#[tauri::command]
fn ping() -> String {
    "pong".to_string()
}

#[tauri::command]
async fn start_issuecentre_monitor(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    config: MonitorConfig,
) -> Result<(), String> {
    state
        .monitor
        .start(app, config)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn stop_issuecentre_monitor(state: State<'_, AppState>) {
    state.monitor.stop().await;
}

#[tauri::command]
async fn get_issuecentre_snapshot(state: State<'_, AppState>) -> MonitorSnapshot {
    state.monitor.snapshot().await
}

#[tauri::command]
async fn map_issuecentre_messages_endpoint(
    config: MonitorConfig,
) -> Result<EndpointMappingResult, String> {
    let client = issuecentre::IssueCentreClient::new(&config).map_err(|e| e.to_string())?;
    client
        .map_messages_endpoint_async()
        .await
        .map_err(|e| e.to_string())
}

fn main() {
    tauri::Builder::default()
        .manage(AppState {
            monitor: Arc::new(MonitorService::new()),
        })
        .invoke_handler(tauri::generate_handler![
            ping,
            start_issuecentre_monitor,
            stop_issuecentre_monitor,
            get_issuecentre_snapshot,
            map_issuecentre_messages_endpoint,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
