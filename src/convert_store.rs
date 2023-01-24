use terminus_store_10::storage::directory as directory_10;
use terminus_store_10::storage::name_to_string;
use terminus_store_10::storage::string_to_name;
use terminus_store_11::storage::archive as archive_11;
use tokio::fs::OpenOptions;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;

use crate::convert_layer::*;
use crate::reachable::*;

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;

use tokio::fs;

use thiserror::*;

#[derive(Error, Debug)]
#[error(transparent)]
pub enum StoreConversionError {
    LayerConversion(#[from] LayerConversionError),
    #[error("Some layer conversions failed")]
    LayerConversionsFailed(Vec<[u32; 5]>),
    Io(#[from] io::Error),
}

pub async fn convert_store(
    from: &str,
    to: &str,
    work: &str,
    naive: bool,
    keep_going: bool,
) -> Result<(), StoreConversionError> {
    let v10_layer_store = directory_10::DirectoryLayerStore::new(from);
    let v10_label_store = directory_10::DirectoryLabelStore::new(from);
    let v11_layer_store = archive_11::ArchiveLayerStore::new(to);

    let reachable = find_reachable_layers(&v10_layer_store, &v10_label_store).await?;

    let mut options = OpenOptions::new();
    options.create(true);
    options.write(true);
    let mut error_path = PathBuf::from(work);
    std::fs::create_dir_all(&error_path)?;
    error_path.push("error.log");
    let mut error_log = options.open(error_path).await?;
    println!("error log opened");

    let status_hashmap = get_status_hashmap(work).await?;
    println!("read status map");
    let mut status_log = status_log(work).await?;
    println!("opened status log");

    let mut visit_queue = Vec::new();
    visit_queue.extend(reachable[&None].clone());

    let mut failures = Vec::new();

    while let Some(layer) = visit_queue.pop() {
        let status = status_hashmap.get(&layer);
        match status {
            Some(ConversionStatus::Completed) => {
                println!("skipping: {}", name_to_string(layer));
                // even though we skip this layer, its children still
                // might need to be converted, so here they are added
                // to the visit queue.
                if let Some(children) = reachable.get(&Some(layer)) {
                    visit_queue.extend(children.clone());
                }
                continue;
            }
            Some(_) => layer_cleanup(to, layer).await?,
            None => (),
        }
        write_status(&mut status_log, layer, ConversionStatus::Started).await?;
        let result =
            convert_layer_with_stores(&v10_layer_store, &v11_layer_store, work, naive, layer).await;
        if let Ok(()) = result {
            write_status(&mut status_log, layer, ConversionStatus::Completed).await?;
            if let Some(children) = reachable.get(&Some(layer)) {
                visit_queue.extend(children.clone());
            }
        } else if let Err(e) = result {
            write_status(&mut status_log, layer, ConversionStatus::Error).await?;
            eprintln!("ERROR: {e}");
            error_log.write_all(e.to_string().as_bytes()).await?;
            error_log.write_all(b"\n").await?;
            error_log.flush().await?;
            if keep_going {
                failures.push(layer);
            } else {
                return Err(e.into());
            }
        }
    }

    if !failures.is_empty() && !keep_going {
        return Err(StoreConversionError::LayerConversionsFailed(failures));
    }
    convert_labels(from, to).await?;

    write_version_file(to).await?;
    println!("version file written");
    Ok(())
}

pub enum ConversionStatus {
    Error,
    Completed,
    Started,
}

impl ToString for ConversionStatus {
    fn to_string(&self) -> String {
        match self {
            ConversionStatus::Error => "Error".to_string(),
            ConversionStatus::Completed => "Completed".to_string(),
            ConversionStatus::Started => "Started".to_string(),
        }
    }
}

impl FromStr for ConversionStatus {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Error" => Ok(ConversionStatus::Error),
            "Completed" => Ok(ConversionStatus::Completed),
            "Started" => Ok(ConversionStatus::Started),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown conversion status encountered",
            )),
        }
    }
}

pub async fn get_status_hashmap(work: &str) -> io::Result<HashMap<[u32; 5], ConversionStatus>> {
    let mut status_options = OpenOptions::new();
    status_options.read(true);
    status_options.create(false);
    let mut status_path = PathBuf::from(work);
    status_path.push("status.log");
    let status_log = status_options.open(status_path).await;
    match status_log {
        Ok(status_log) => {
            let mut hashmap = HashMap::new();
            let status_log_buf = BufReader::new(status_log);
            let mut lines = status_log_buf.lines();
            while let Some(line) = lines.next_line().await? {
                let elts = line.split(' ').collect::<Vec<&str>>();
                let layer = string_to_name(elts[0])?;
                let status = ConversionStatus::from_str(elts[1])?;
                hashmap.insert(layer, status);
            }
            Ok(hashmap)
        }
        Err(e) => {
            if e.kind() == io::ErrorKind::NotFound {
                Ok(HashMap::with_capacity(0))
            } else {
                Err(e)
            }
        }
    }
}

pub async fn write_status(
    f: &mut fs::File,
    layer: [u32; 5],
    s: ConversionStatus,
) -> Result<(), io::Error> {
    f.write_all(format!("{} {}\n", name_to_string(layer), s.to_string(),).as_bytes())
        .await?;
    f.flush().await?;
    Ok(())
}

pub async fn status_log(work: &str) -> io::Result<fs::File> {
    let mut completed_options = OpenOptions::new();
    completed_options.create(true);
    completed_options.append(true);
    let mut completed_path = PathBuf::from(work);
    std::fs::create_dir_all(&completed_path)?;
    completed_path.push("status.log");
    let completed_log = completed_options.open(completed_path).await?;
    println!("error log opened");
    Ok(completed_log)
}

pub async fn convert_labels(from: &str, to: &str) -> io::Result<()> {
    let v11_store_path = PathBuf::from(to);
    let mut stream = fs::read_dir(from).await?;
    while let Some(direntry) = stream.next_entry().await? {
        if direntry.file_type().await?.is_file() {
            let os_name = direntry.file_name();
            let name = os_name.to_str().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected non-utf8 directory name",
                )
            })?;
            if name.ends_with(".label") {
                let mut to_path = v11_store_path.clone();
                to_path.push(name);
                fs::copy(direntry.path(), to_path).await?;
            }
        }
    }

    Ok(())
}

pub async fn layer_cleanup(to: &str, layer: [u32; 5]) -> Result<(), io::Error> {
    let name = name_to_string(layer);
    println!("layer cleanup: {name}");
    let larch = format!("{name}.larch");
    let rollup = format!("{name}.rollup.hex");
    let prefix = &name[..3];
    let mut path = PathBuf::from(to);
    path.push(prefix);
    let mut rollup_path = path.clone();
    rollup_path.push(rollup);
    let mut larch_path = path;
    larch_path.push(larch);
    let rollup_res = tokio::fs::remove_file(rollup_path).await;
    let larch_res = tokio::fs::remove_file(larch_path).await;
    if let Err(e) = rollup_res {
        if e.kind() != io::ErrorKind::NotFound {
            return Err(e);
        }
    }
    if let Err(e) = larch_res {
        if e.kind() != io::ErrorKind::NotFound {
            return Err(e);
        }
    }
    Ok(())
}

pub async fn write_version_file(to: &str) -> Result<(), io::Error> {
    let mut options = OpenOptions::new();
    options.create(true);
    options.write(true);

    let mut path = PathBuf::from(to);
    path.push("STORAGE_VERSION");
    let mut file = options.open(path).await?;
    file.write_all(b"2").await?;
    file.flush().await
}
