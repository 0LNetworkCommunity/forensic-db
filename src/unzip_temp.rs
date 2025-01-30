use anyhow::{Context, Result};
use diem_temppath::TempPath;
use flate2::read::GzDecoder;
use glob::glob;
// use libra_storage::read_tx_chunk::load_tx_chunk_manifest;
use log::{info, warn};
use std::{
    fs::File,
    io::copy,
    path::{Path, PathBuf},
};
use tar::Archive;

/// Decompresses a gzip-compressed file at `src_path` and saves the decompressed contents
/// to `dst_dir` with the same file name, but without the `.gz` extension.
fn decompress_file(src_path: &Path, dst_dir: &Path, tar_opt: bool) -> Result<PathBuf> {
    // Open the source file in read-only mode
    let src_file = File::open(src_path)?;

    // Create a GzDecoder to handle the decompression
    let mut decoder = GzDecoder::new(src_file);

    // Generate the destination path with the destination directory and new file name
    let file_stem = src_path.file_stem().unwrap(); // removes ".gz"
    let dst_path = dst_dir.join(file_stem); // combines dst_dir with file_stem

    if tar_opt {
        let mut archive = Archive::new(decoder);
        // archive.unpack(".")?;
        for file in archive.entries().unwrap() {
            // Make sure there wasn't an I/O error
            let file = file.unwrap();

            // Inspect metadata about the file
            println!("{:?}", file.header().path().unwrap());
            println!("{}", file.header().size().unwrap());

            // files implement the Read trait
            // let mut s = String::new();
            // file.read_to_string(&mut s).unwrap();
            // println!("{}", s);
        }
    } else {
        // Open the destination file in write mode
        let mut dst_file = File::create(&dst_path)?;

        // Copy the decompressed data into the destination file
        copy(&mut decoder, &mut dst_file)?;
    }

    Ok(dst_path)
}

/// decompress a tar.gz or .tgz file (and Archive of Deflate compressed files)
pub fn decompress_tar_archive(src_path: &Path, dst_dir: &Path) -> Result<()> {
    // Open the source file in read-only mode
    let src_file = File::open(src_path)?;

    // Create a GzDecoder to handle the decompression
    let decoder = GzDecoder::new(src_file);

    let mut archive = Archive::new(decoder);
    archive.unpack(dst_dir)?;

    Ok(())
}

/// Unzip all .gz files into the same directory
/// Warning: this will take up a lot of disk space, should not be used in production for all files. Use for on the fly decompression.
/// NOTE: Not for tarballs
pub fn decompress_all_gz(parent_dir: &Path, dst_dir: &Path) -> Result<()> {
    let path = parent_dir.canonicalize()?;

    let pattern = format!(
        "{}/**/*.gz",
        path.to_str().context("cannot parse starting dir")?
    );

    for entry in glob(&pattern)? {
        match entry {
            Ok(src_path) => {
                let _ = decompress_file(&src_path, dst_dir, false);
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }
    Ok(())
}

// The manifest file might have written as .gz, when then should not be.
// TODO: Deprecate when archives sources fixed (currently some epochs in V7 broken for epochs in Jan 2025)
fn maybe_fix_manifest(archive_path: &Path) -> Result<()> {
    let pattern = format!("{}/**/*.manifest", archive_path.display());
    for manifest_path in glob(&pattern)?.flatten() {
        let literal = std::fs::read_to_string(&manifest_path)?.replace(".gz", "");
        // let mut manifest = load_tx_chunk_manifest(&manifest_path)?;
        // debug!("old manifest:\n{:#}", &serde_json::to_string(&manifest)?);

        // manifest.chunks.iter_mut().for_each(|e| {
        //     if e.proof.contains(".gz") {
        //         e.proof = e.proof.trim_end_matches(".gz").to_string();
        //     }
        //     if e.transactions.contains(".gz") {
        //         e.transactions = e.transactions.trim_end_matches(".gz").to_string();
        //     }
        // });
        // let literal = serde_json::to_string(&manifest)?;

        warn!(
            "rewriting .manifest file to remove .gz paths, {}, {:#}",
            manifest_path.display(),
            &literal
        );
        std::fs::write(&manifest_path, literal.as_bytes())?;
    }
    Ok(())
}

/// If we are using this tool with .gz files, we will unzip on the fly
/// If the user prefers to not do on the fly, then they need to update
/// their workflow to `gunzip -r` before starting this.
pub fn maybe_handle_gz(archive_path: &Path) -> Result<(PathBuf, Option<TempPath>)> {
    // maybe stuff isn't unzipped yet
    let pattern = format!("{}/*.*.gz", archive_path.display());
    if glob(&pattern)?.count() > 0 {
        let mut temp_dir = TempPath::new();
        temp_dir.create_as_dir()?;
        temp_dir.persist();

        // need to preserve the parent dir name in temp, since the manifest files reference it.
        let dir_name = archive_path.file_name().unwrap().to_str().unwrap();
        let new_archive_path = temp_dir.path().join(dir_name);

        info!("Decompressing a temp folder. If you do not want to decompress files on the fly (which are not saved), then you workflow to do a `gunzip -r` before starting this. Temp folder: {}", &new_archive_path.display());

        std::fs::create_dir_all(&new_archive_path)?;
        decompress_all_gz(archive_path, &new_archive_path)?;
        // fix the manifest in the TEMP path
        maybe_fix_manifest(temp_dir.path())?;
        return Ok((new_archive_path, Some(temp_dir)));
    }
    // maybe the user unzipped the files

    let pattern = format!("{}/*.chunk", archive_path.display());
    assert!(
        glob(&pattern)?.count() > 0,
        "are you sure you decompressed everything here?"
    );
    maybe_fix_manifest(archive_path)?;

    Ok((archive_path.to_path_buf(), None))
}

// take a single archive file, and get the temp location of the unzipped file
// NOTE: you must return the TempPath to the caller so otherwise when it
// drops out of scope the files will be deleted, this is intentional.
pub fn test_helper_temp_unzipped(
    archive_file: &Path,
    tar_opt: bool,
) -> Result<(PathBuf, TempPath)> {
    let temp_dir = TempPath::new();
    temp_dir.create_as_dir()?;

    let path = decompress_file(archive_file, temp_dir.path(), tar_opt)?;

    Ok((path, temp_dir))
}
