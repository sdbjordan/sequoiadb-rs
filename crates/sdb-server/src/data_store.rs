use std::path::{Path, PathBuf};

use sdb_bson::{Decode, Document, Encode};
use sdb_cat::CatalogManager;
use sdb_common::{Result, SdbError};

/// File-backed persistence for catalog data.
///
/// Directory layout:
///   base_path/
///     cs_name/
///       cl_name.dat  — concatenated BSON documents
pub struct DataStore {
    base_path: PathBuf,
}

impl DataStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: path.into(),
        }
    }

    /// Save all catalog data to disk.
    pub fn save(&self, catalog: &CatalogManager) -> Result<()> {
        // Ensure base dir exists
        std::fs::create_dir_all(&self.base_path).map_err(|_| SdbError::IoError)?;

        for cs_name in catalog.list_collection_spaces() {
            let cs_dir = self.base_path.join(&cs_name);
            std::fs::create_dir_all(&cs_dir).map_err(|_| SdbError::IoError)?;

            let collections = catalog.list_collections(&cs_name)?;
            for cl_name in &collections {
                let file_path = cs_dir.join(format!("{}.dat", cl_name));
                let (storage, _) = catalog.collection_handle(&cs_name, cl_name)?;
                let rows = storage.scan();

                let mut buf = Vec::new();
                for (_, doc) in &rows {
                    doc.encode(&mut buf).map_err(|_| SdbError::InvalidBson)?;
                }

                std::fs::write(&file_path, &buf).map_err(|_| SdbError::IoError)?;
            }
        }

        Ok(())
    }

    /// Load catalog data from disk, returning a populated CatalogManager.
    pub fn load(&self) -> Result<CatalogManager> {
        let mut catalog = CatalogManager::new();

        if !self.base_path.exists() {
            return Ok(catalog);
        }

        let entries = std::fs::read_dir(&self.base_path).map_err(|_| SdbError::IoError)?;
        for entry in entries {
            let entry = entry.map_err(|_| SdbError::IoError)?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let cs_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or(SdbError::IoError)?
                .to_string();

            catalog.create_collection_space(&cs_name)?;

            let cl_entries = std::fs::read_dir(&path).map_err(|_| SdbError::IoError)?;
            for cl_entry in cl_entries {
                let cl_entry = cl_entry.map_err(|_| SdbError::IoError)?;
                let cl_path = cl_entry.path();
                if cl_path.extension().and_then(|e| e.to_str()) != Some("dat") {
                    continue;
                }

                let cl_name = cl_path
                    .file_stem()
                    .and_then(|n| n.to_str())
                    .ok_or(SdbError::IoError)?
                    .to_string();

                catalog.create_collection(&cs_name, &cl_name)?;

                // Load documents
                let data = std::fs::read(&cl_path).map_err(|_| SdbError::IoError)?;
                let mut off = 0;
                while off < data.len() {
                    let doc =
                        Document::decode(&data[off..]).map_err(|_| SdbError::InvalidBson)?;
                    let doc_bytes =
                        doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
                    off += doc_bytes.len();
                    catalog.insert_document(&cs_name, &cl_name, &doc)?;
                }
            }
        }

        Ok(catalog)
    }

    /// Return the base path.
    pub fn path(&self) -> &Path {
        &self.base_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::Value;

    #[test]
    fn save_and_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("sdb_ds_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let store = DataStore::new(&dir);

        // Create catalog with data
        let mut catalog = CatalogManager::new();
        catalog.create_collection_space("mycs").unwrap();
        catalog.create_collection("mycs", "mycl").unwrap();

        let mut doc1 = Document::new();
        doc1.insert("x", Value::Int32(1));
        catalog.insert_document("mycs", "mycl", &doc1).unwrap();

        let mut doc2 = Document::new();
        doc2.insert("x", Value::Int32(2));
        catalog.insert_document("mycs", "mycl", &doc2).unwrap();

        // Save
        store.save(&catalog).unwrap();

        // Load into fresh catalog
        let loaded = store.load().unwrap();
        let (storage, _) = loaded.collection_handle("mycs", "mycl").unwrap();
        let rows = storage.scan();
        assert_eq!(rows.len(), 2);

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_empty_dir() {
        let dir = std::env::temp_dir().join(format!("sdb_ds_empty_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let store = DataStore::new(&dir);
        let catalog = store.load().unwrap();
        assert!(catalog.list_collection_spaces().is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_nonexistent_dir() {
        let store = DataStore::new("/tmp/sdb_nonexistent_99999");
        let catalog = store.load().unwrap();
        assert!(catalog.list_collection_spaces().is_empty());
    }

    #[test]
    fn save_multiple_collections() {
        let dir = std::env::temp_dir().join(format!("sdb_ds_multi_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let store = DataStore::new(&dir);

        let mut catalog = CatalogManager::new();
        catalog.create_collection_space("cs1").unwrap();
        catalog.create_collection("cs1", "cl1").unwrap();
        catalog.create_collection("cs1", "cl2").unwrap();
        catalog.create_collection_space("cs2").unwrap();
        catalog.create_collection("cs2", "cl3").unwrap();

        let mut d = Document::new();
        d.insert("k", Value::String("v".into()));
        catalog.insert_document("cs1", "cl1", &d).unwrap();
        catalog.insert_document("cs2", "cl3", &d).unwrap();

        store.save(&catalog).unwrap();

        let loaded = store.load().unwrap();
        let css = loaded.list_collection_spaces();
        assert_eq!(css.len(), 2);
        let (s1, _) = loaded.collection_handle("cs1", "cl1").unwrap();
        assert_eq!(s1.scan().len(), 1);
        let (s3, _) = loaded.collection_handle("cs2", "cl3").unwrap();
        assert_eq!(s3.scan().len(), 1);
        // cl2 exists but empty
        let (s2, _) = loaded.collection_handle("cs1", "cl2").unwrap();
        assert_eq!(s2.scan().len(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
