use providers::ProviderRegistry;

#[derive(Debug, Clone)]
pub struct ClassificationInput {
    pub text: String,
    pub metadata: serde_json::Value,
    pub provider: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClassificationOutcome {
    pub label: String,
    pub confidence: f32,
}

pub async fn classify(
    input: ClassificationInput,
    registry: &ProviderRegistry,
) -> anyhow::Result<ClassificationOutcome> {
    // Fast path: heuristics.
    if let Some(label) = heuristic_label(&input.metadata) {
        return Ok(ClassificationOutcome {
            label,
            confidence: 0.9,
        });
    }

    // TODO: extend with kNN before LLM call.
    let provider = registry.llm(input.provider.as_deref());
    if let Ok(llm) = provider {
        let prompt = format!(
            "Classify the file with metadata {:?}. Text:\n{}",
            input.metadata, input.text
        );
        if let Ok(resp) = llm.classify(&prompt).await {
            return Ok(ClassificationOutcome {
                label: resp.label,
                confidence: resp.confidence,
            });
        }
    }

    Ok(ClassificationOutcome {
        label: "unknown".to_string(),
        confidence: 0.0,
    })
}

fn heuristic_label(meta: &serde_json::Value) -> Option<String> {
    let mime = meta.get("mime").and_then(|m| m.as_str()).unwrap_or("");
    let label = if mime.contains("pdf") {
        "document/pdf"
    } else if mime.contains("msword") || mime.contains("officedocument") {
        "document/office"
    } else if mime.starts_with("image/") {
        "image"
    } else if mime.starts_with("text/") {
        "text"
    } else {
        ""
    };

    if label.is_empty() {
        None
    } else {
        Some(label.to_string())
    }
}
