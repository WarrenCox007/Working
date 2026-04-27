use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rand::{thread_rng, Rng};
use regex::Regex;
use reqwest::{cookie::Jar, Url};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use tauri::{api::notification::Notification, AppHandle, Manager};
use tokio::sync::{Mutex, RwLock};

const DEFAULT_INBOX_URL: &str = "emailinbox.do?pageURL=emailInboxes";
const DEFAULT_MESSAGES_ENDPOINT: &str = "GetEmailMessagesXML";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorConfig {
    pub base_url: String,
    pub username: String,
    pub password: String,
    pub poll_seconds: u64,
    pub watched_folders: Vec<String>,
    pub play_sound: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InboxFolder {
    pub id: String,
    pub name: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InboxMessageSummary {
    pub message_id: String,
    pub from: Option<String>,
    pub subject: Option<String>,
    pub received: Option<String>,
    pub folder: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MonitorSnapshot {
    pub timestamp: DateTime<Utc>,
    pub total_count: usize,
    pub folders: Vec<InboxFolder>,
    pub new_messages: Vec<InboxMessageSummary>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointMappingResult {
    pub messages_endpoint: String,
    pub candidate_params: Vec<String>,
    pub examples: Vec<String>,
}

pub struct IssueCentreClient {
    http: reqwest::Client,
    base: Url,
    username: String,
    password: String,
}

impl IssueCentreClient {
    pub fn new(config: &MonitorConfig) -> Result<Self> {
        let base = Url::parse(&config.base_url).context("invalid base URL")?;
        let cookies = Arc::new(Jar::default());
        let http = reqwest::Client::builder()
            .cookie_provider(cookies)
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .context("failed to build http client")?;
        Ok(Self {
            http,
            base,
            username: config.username.clone(),
            password: config.password.clone(),
        })
    }

    pub async fn login_async(&self) -> Result<()> {
        let login_url = self.base.join("login.do")?;
        let form = [
            ("username", self.username.as_str()),
            ("password", self.password.as_str()),
        ];
        let response = self.http.post(login_url).form(&form).send().await?;
        if !response.status().is_success() {
            return Err(anyhow!("login failed with status {}", response.status()));
        }
        Ok(())
    }

    pub async fn ensure_session_async(&self) -> Result<()> {
        let inbox = self.base.join(DEFAULT_INBOX_URL)?;
        let body = self.http.get(inbox).send().await?.text().await?;
        if body.contains("IssueCentre - Login") || body.contains("Username") {
            self.login_async().await?;
        }
        Ok(())
    }

    pub async fn get_inbox_tree_async(&self) -> Result<Vec<InboxFolder>> {
        self.ensure_session_async().await?;
        let inbox = self.base.join(DEFAULT_INBOX_URL)?;
        let body = self.http.get(inbox).send().await?.text().await?;
        parse_inbox_tree(&body)
    }

    pub async fn map_messages_endpoint_async(&self) -> Result<EndpointMappingResult> {
        self.ensure_session_async().await?;
        let inbox = self.base.join(DEFAULT_INBOX_URL)?;
        let body = self.http.get(inbox).send().await?.text().await?;
        map_messages_endpoint_from_html(&body)
    }

    pub async fn get_message_list_async(
        &self,
        folder_id: &str,
        extra_query: &HashMap<String, String>,
    ) -> Result<Vec<InboxMessageSummary>> {
        self.ensure_session_async().await?;
        let mut url = self.base.join(DEFAULT_MESSAGES_ENDPOINT)?;
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("folderId", folder_id);
            for (k, v) in extra_query {
                qp.append_pair(k, v);
            }
        }
        let payload = self.http.get(url).send().await?.text().await?;
        parse_messages_xml(folder_id, &payload)
    }
}

pub fn parse_inbox_tree(html: &str) -> Result<Vec<InboxFolder>> {
    let doc = Html::parse_document(html);
    let selector = Selector::parse("a, span, li").map_err(|_| anyhow!("invalid selector"))?;
    let re = Regex::new(r"(?P<name>.+?)\s*\((?P<count>\d+)\)\s*$")?;
    let mut seen = HashSet::new();
    let mut folders = Vec::new();
    for node in doc.select(&selector) {
        let text = node.text().collect::<Vec<_>>().join(" ").trim().to_string();
        if let Some(cap) = re.captures(&text) {
            let name = cap["name"].trim().to_string();
            let count = cap["count"].parse::<usize>().unwrap_or_default();
            if seen.insert(name.clone()) {
                folders.push(InboxFolder {
                    id: slugify(&name),
                    name,
                    count,
                });
            }
        }
    }
    Ok(folders)
}

pub fn map_messages_endpoint_from_html(html: &str) -> Result<EndpointMappingResult> {
    let endpoint_re = Regex::new(r#"GetEmailMessagesXML\??[^\"'\s<]*"#)?;
    let key_re = Regex::new(r"([A-Za-z][A-Za-z0-9_]+)=")?;
    let mut examples = endpoint_re
        .find_iter(html)
        .map(|m| m.as_str().trim_matches('"').trim_matches('\'').to_string())
        .collect::<Vec<_>>();
    examples.sort();
    examples.dedup();

    let mut params = HashSet::new();
    for ex in &examples {
        for cap in key_re.captures_iter(ex) {
            params.insert(cap[1].to_string());
        }
    }

    Ok(EndpointMappingResult {
        messages_endpoint: DEFAULT_MESSAGES_ENDPOINT.to_string(),
        candidate_params: {
            let mut values = params.into_iter().collect::<Vec<_>>();
            values.sort();
            values
        },
        examples,
    })
}

pub fn parse_messages_xml(folder: &str, xml_or_json: &str) -> Result<Vec<InboxMessageSummary>> {
    // Handle XML-like payloads and lightweight JSON fallback.
    let id_re = Regex::new(r#"(?:(?:email|message)Id\s*=\s*[\"']?)(\d+)"#)?;
    let from_re = Regex::new(r#"<from>(.*?)</from>|\"from\"\s*:\s*\"(.*?)\""#)?;
    let subject_re = Regex::new(r#"<subject>(.*?)</subject>|\"subject\"\s*:\s*\"(.*?)\""#)?;
    let received_re = Regex::new(r#"<received>(.*?)</received>|\"received\"\s*:\s*\"(.*?)\""#)?;

    let mut ids = Vec::new();
    for cap in id_re.captures_iter(xml_or_json) {
        ids.push(cap[1].to_string());
    }
    ids.sort();
    ids.dedup();

    let froms = collect_first_group(&from_re, xml_or_json);
    let subjects = collect_first_group(&subject_re, xml_or_json);
    let received = collect_first_group(&received_re, xml_or_json);

    let messages = ids
        .into_iter()
        .enumerate()
        .map(|(idx, id)| InboxMessageSummary {
            message_id: id,
            from: froms.get(idx).cloned(),
            subject: subjects.get(idx).cloned(),
            received: received.get(idx).cloned(),
            folder: folder.to_string(),
        })
        .collect::<Vec<_>>();
    Ok(messages)
}

fn collect_first_group(re: &Regex, source: &str) -> Vec<String> {
    re.captures_iter(source)
        .filter_map(|c| {
            c.get(1)
                .or_else(|| c.get(2))
                .map(|m| html_escape::decode_html_entities(m.as_str()).to_string())
        })
        .collect::<Vec<_>>()
}

fn slugify(input: &str) -> String {
    input
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

#[derive(Default)]
pub struct MonitorService {
    state: Arc<RwLock<MonitorSnapshot>>,
    stop_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl MonitorService {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn start(&self, app: AppHandle, config: MonitorConfig) -> Result<()> {
        self.stop().await;
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        *self.stop_tx.lock().await = Some(tx);
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut last_seen: HashMap<String, HashSet<String>> = HashMap::new();
            let mut backoff = Duration::from_secs(config.poll_seconds.max(30));
            loop {
                let tick = tokio::time::sleep(backoff);
                tokio::pin!(tick);
                tokio::select! {
                    _ = &mut rx => break,
                    _ = &mut tick => {
                        let update = poll_once(&config, &mut last_seen).await;
                        match update {
                            Ok(mut snapshot) => {
                                if !snapshot.new_messages.is_empty() {
                                    let body = format!("{} new unprocessed email(s) in watched queues", snapshot.new_messages.len());
                                    let _ = Notification::new(&app.config().tauri.bundle.identifier)
                                        .title("IssueCentre inbox")
                                        .body(&body)
                                        .show();
                                    if config.play_sound {
                                        let _ = app.emit_all("issuecentre://play-sound", ());
                                    }
                                }
                                snapshot.last_error = None;
                                *state.write().await = snapshot;
                                backoff = Duration::from_secs(config.poll_seconds.max(30) + thread_rng().gen_range(0..5));
                            }
                            Err(err) => {
                                let mut snapshot = state.read().await.clone();
                                snapshot.timestamp = Utc::now();
                                snapshot.last_error = Some(err.to_string());
                                *state.write().await = snapshot;
                                backoff = (backoff * 2).min(Duration::from_secs(300));
                            }
                        }
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn stop(&self) {
        if let Some(tx) = self.stop_tx.lock().await.take() {
            let _ = tx.send(());
        }
    }

    pub async fn snapshot(&self) -> MonitorSnapshot {
        self.state.read().await.clone()
    }
}

async fn poll_once(
    config: &MonitorConfig,
    last_seen: &mut HashMap<String, HashSet<String>>,
) -> Result<MonitorSnapshot> {
    let client = IssueCentreClient::new(config)?;
    let folders = client.get_inbox_tree_async().await?;
    let watched = if config.watched_folders.is_empty() {
        folders.iter().map(|f| f.id.clone()).collect::<HashSet<_>>()
    } else {
        config
            .watched_folders
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
    };

    let mut new_messages = Vec::new();
    for folder in folders.iter().filter(|f| watched.contains(&f.id)) {
        let messages = client
            .get_message_list_async(&folder.id, &HashMap::new())
            .await
            .unwrap_or_default();
        let entry = last_seen.entry(folder.id.clone()).or_default();
        for msg in messages {
            if entry.insert(msg.message_id.clone()) {
                new_messages.push(msg);
            }
        }
    }

    Ok(MonitorSnapshot {
        timestamp: Utc::now(),
        total_count: folders.iter().map(|f| f.count).sum(),
        folders,
        new_messages,
        last_error: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_get_email_messages_params_from_javascript() {
        let html = r#"
            <script>
                var url = 'GetEmailMessagesXML?contract=CSC&page=1&rowsPerPage=20&sort=received&dir=desc';
            </script>
        "#;
        let mapped = map_messages_endpoint_from_html(html).expect("mapped");
        assert!(mapped
            .examples
            .iter()
            .any(|e| e.contains("GetEmailMessagesXML")));
        assert!(mapped.candidate_params.contains(&"contract".to_string()));
        assert!(mapped.candidate_params.contains(&"rowsPerPage".to_string()));
    }

    #[test]
    fn parses_tree_counts() {
        let html = r#"<li>Inboxes (6)</li><li>CSC (3)</li><li>NexGen Cloud (1)</li>"#;
        let folders = parse_inbox_tree(html).expect("folders");
        assert_eq!(folders.len(), 3);
        assert_eq!(folders[1].name, "CSC");
        assert_eq!(folders[1].count, 3);
    }
}
