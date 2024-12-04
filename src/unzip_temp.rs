use anyhow::{Context, Result};
use diem_temppath::TempPath;
use flate2::read::GzDecoder;
use glob::glob;
use std::{
    fs::File,
    io::copy,
    path::{Path, PathBuf},
};
use tar::Archive;

// TODO: decompress the files on demand, and don't take up the disk space

// take a single archive file, and get the temp location of the unzipped file
// NOTE: you must return the TempPath to the caller so otherwise when it
// drops out of scope the files will be deleted, this is intentional.
pub fn make_temp_unzipped(archive_file: &Path, tar_opt: bool) -> Result<(PathBuf, TempPath)> {
    let temp_dir = TempPath::new();
    temp_dir.create_as_dir()?;

    let path = decompress_file(archive_file, temp_dir.path(), tar_opt)?;

    Ok((path, temp_dir))
}

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
/// Warning: this will take up a lot of disk space, should not be used in production
pub fn decompress_all_gz(parent_dir: &Path) -> Result<()> {
    let path = parent_dir.canonicalize()?;

    let pattern = format!(
        "{}/**/*.gz",
        path.to_str().context("cannot parse starting dir")?
    );

    for entry in glob(&pattern)? {
        match entry {
            Ok(src_path) => {
                let _ = decompress_file(&src_path, src_path.parent().unwrap(), false);
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }
    Ok(())
}
