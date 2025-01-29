//! scan
#![allow(dead_code)]

use anyhow::{Context, Result};
use glob::glob;
use libra_backwards_compatibility::version_five::{
    state_snapshot_v5::v5_read_from_snapshot_manifest,
    transaction_manifest_v5::v5_read_from_transaction_manifest,
};
use libra_storage::read_snapshot::load_snapshot_manifest;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
};
#[derive(Clone, Debug)]
pub struct ArchiveMap(pub BTreeMap<PathBuf, ManifestInfo>);

#[derive(Clone, Debug)]

pub struct ManifestInfo {
    /// the enclosing directory of the local .manifest file
    pub archive_dir: PathBuf,
    /// the name of the directory, as a unique archive identifier
    pub archive_id: String,
    /// what libra version were these files encoded with (v5 etc)
    pub version: FrameworkVersion,
    /// contents of the manifest
    pub contents: BundleContent,
    /// processed
    pub processed: bool,
}

impl ManifestInfo {
    pub fn new(archive_dir: &Path) -> Self {
        let archive_id = archive_dir
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        ManifestInfo {
            archive_dir: archive_dir.to_path_buf(),
            archive_id,
            version: FrameworkVersion::Unknown,
            contents: BundleContent::Unknown,
            processed: false,
        }
    }

    pub fn set_info(&mut self) -> Result<()> {
        self.set_contents()?;
        self.try_set_framework_version();
        Ok(())
    }

    /// find out the type of content in the manifest
    pub fn set_contents(&mut self) -> Result<()> {
        // filenames may be in .gz format
        let pattern = format!(
            "{}/*.manifest*", // also try .gz
            self.archive_dir
                .to_str()
                .context("cannot parse starting dir")?
        );

        if let Some(man_file) = glob(&pattern)?.flatten().next() {
            self.contents = BundleContent::new_from_man_file(&man_file);
        }
        Ok(())
    }

    pub fn try_set_framework_version(&mut self) -> FrameworkVersion {
        match self.contents {
            BundleContent::Unknown => return FrameworkVersion::Unknown,
            BundleContent::StateSnapshot => {
                let man_path = self.archive_dir.join(self.contents.filename());
                // first check if the v7 manifest will parse
                if let Ok(_bak) = load_snapshot_manifest(&man_path) {
                    self.version = FrameworkVersion::V7;
                } else if v5_read_from_snapshot_manifest(&self.archive_dir.join("state.manifest"))
                    .is_ok()
                {
                    self.version = FrameworkVersion::V5;
                }
            }
            BundleContent::Transaction => {
                // TODO: v5 manifests appear to have the same format this is a noop
                if v5_read_from_transaction_manifest(&self.archive_dir).is_ok() {
                    self.version = FrameworkVersion::V5;
                }
            }
            BundleContent::EpochEnding => {}
        }

        FrameworkVersion::Unknown
    }
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum FrameworkVersion {
    #[default]
    Unknown,
    V5,
    V6,
    V7,
}

impl fmt::Display for FrameworkVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

#[derive(Clone, Debug, clap::ValueEnum, PartialEq)]
pub enum BundleContent {
    Unknown,
    StateSnapshot,
    Transaction,
    EpochEnding,
}
impl BundleContent {
    pub fn new_from_man_file(man_file: &Path) -> Self {
        let s = man_file.to_str().expect("invalid path");
        if s.contains("transaction.manifest") {
            return BundleContent::Transaction;
        };
        if s.contains("epoch_ending.manifest") {
            return BundleContent::EpochEnding;
        };
        if s.contains("state.manifest") {
            return BundleContent::StateSnapshot;
        };
        BundleContent::Unknown
    }
    pub fn filename(&self) -> String {
        match self {
            BundleContent::Unknown => "*.manifest".to_string(),
            BundleContent::StateSnapshot => "state.manifest".to_string(),
            BundleContent::Transaction => "transaction.manifest".to_string(),
            BundleContent::EpochEnding => "epoch_ending.manifest".to_string(),
        }
    }
}

/// Crawl a directory and find all .manifest files.
/// Optionally find
pub fn scan_dir_archive(
    parent_dir: &Path,
    content_opt: Option<BundleContent>,
) -> Result<ArchiveMap> {
    let path = parent_dir.canonicalize()?;
    // filenames may be in .gz format
    let filename = content_opt.unwrap_or(BundleContent::Unknown).filename();
    let pattern = format!(
        "{}/**/{}*", // also try .gz
        path.to_str().context("cannot parse starting dir")?,
        filename,
    );

    let mut archive = BTreeMap::new();

    for manifest_path in glob(&pattern)?.flatten() {
        let archive_dir = manifest_path
            .parent()
            .expect("can't find manifest dir, weird");
        let mut man = ManifestInfo::new(archive_dir);
        man.set_info()?;
        archive.insert(archive_dir.to_path_buf(), man);
    }
    Ok(ArchiveMap(archive))
}

// /// find out the type of content in the manifest
// fn test_content(manifest_path: &Path) -> BundleContent {
//     let s = manifest_path.to_str().expect("path invalid");
//     if s.contains("transaction.manifest") {
//         return BundleContent::Transaction;
//     };
//     if s.contains("epoch_ending.manifest") {
//         return BundleContent::EpochEnding;
//     };
//     if s.contains("state.manifest") {
//         return BundleContent::StateSnapshot;
//     };

//     BundleContent::Unknown
// }
