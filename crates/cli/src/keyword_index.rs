#[cfg(feature = "keyword-index")]
pub mod enabled {
    use anyhow::{anyhow, Result};
    use std::path::Path;
    use tantivy::collector::TopDocs;
    use tantivy::doc;
    use tantivy::schema::{Schema, STORED, TEXT};
    use tantivy::{Index, IndexWriter, ReloadPolicy, Term};

    pub fn build_index(path: &Path, docs: &[(String, String)]) -> Result<()> {
        std::fs::create_dir_all(path)?;
        let index = Index::open_in_dir(path).or_else(|_| {
            let mut schema_builder = Schema::builder();
            schema_builder.add_text_field("path", STORED);
            schema_builder.add_text_field("text", TEXT);
            let schema = schema_builder.build();
            Index::create_in_dir(path, schema)
        })?;
        let schema = index.schema();
        let path_field = schema
            .get_field("path")
            .ok_or_else(|| anyhow!("path field missing in index schema"))?;
        let text_field = schema
            .get_field("text")
            .ok_or_else(|| anyhow!("text field missing in index schema"))?;
        let mut writer: IndexWriter = index.writer(50_000_000)?;
        // Reset before refresh to keep the index in sync with the DB snapshot.
        writer.delete_all();
        for (p, t) in docs {
            writer.add_document(doc!(path_field=>p.as_str(), text_field=>t.as_str()));
        }
        writer.commit()?;
        index.directory().sync_directory()?;
        Ok(())
    }

    pub fn upsert_docs(path: &Path, docs: &[(String, String)]) -> Result<()> {
        let index = Index::open_in_dir(path)?;
        let schema = index.schema();
        let path_field = schema
            .get_field("path")
            .ok_or_else(|| anyhow!("path field missing in index schema"))?;
        let text_field = schema
            .get_field("text")
            .ok_or_else(|| anyhow!("text field missing in index schema"))?;
        let mut writer: IndexWriter = index.writer(50_000_000)?;
        for (p, t) in docs {
            writer.delete_term(Term::from_field_text(path_field, p));
            writer.add_document(doc!(path_field=>p.as_str(), text_field=>t.as_str()));
        }
        writer.commit()?;
        index.directory().sync_directory()?;
        Ok(())
    }

    pub fn delete_docs(path: &Path, paths: &[String]) -> Result<()> {
        let index = Index::open_in_dir(path)?;
        let schema = index.schema();
        let path_field = schema
            .get_field("path")
            .ok_or_else(|| anyhow!("path field missing in index schema"))?;
        let mut writer: IndexWriter = index.writer(50_000_000)?;
        for p in paths {
            writer.delete_term(Term::from_field_text(path_field, p));
        }
        writer.commit()?;
        index.directory().sync_directory()?;
        Ok(())
    }

    pub fn search(path: &Path, query_str: &str, limit: usize) -> Result<Vec<String>> {
        let index = Index::open_in_dir(path)?;
        let schema = index.schema();
        let path_field = schema.get_field("path").unwrap();
        let text_field = schema.get_field("text").unwrap();
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        let searcher = reader.searcher();
        let parser = tantivy::query::QueryParser::for_index(&index, vec![text_field]);
        let query = parser.parse_query(query_str)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;
        let mut results = Vec::new();
        for (_score, addr) in top_docs {
            let doc = searcher.doc(addr)?;
            if let Some(val) = doc.get_first(path_field) {
                if let Some(text) = val.text() {
                    results.push(text.to_string());
                }
            }
        }
        Ok(results)
    }
}

#[cfg(not(feature = "keyword-index"))]
pub mod enabled {
    use anyhow::Result;
    use std::path::Path;
    pub fn build_index(_path: &Path, _docs: &[(String, String)]) -> Result<()> {
        Ok(())
    }
    pub fn search(_path: &Path, _query: &str, _limit: usize) -> Result<Vec<String>> {
        Ok(vec![])
    }
    pub fn upsert_docs(_path: &Path, _docs: &[(String, String)]) -> Result<()> {
        Ok(())
    }
    pub fn delete_docs(_path: &Path, _paths: &[String]) -> Result<()> {
        Ok(())
    }
}
