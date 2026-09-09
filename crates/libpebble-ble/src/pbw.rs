//! PBW archive parsing and watch-variant selection.

use std::io::{Cursor, Read};

use serde::Deserialize;
use uuid::Uuid;
use zip::{ZipArchive, result::ZipError};

use crate::{PebbleError, WatchType};

const MAX_PBW_SIZE: usize = 32 * 1024 * 1024;
const MAX_BLOB_SIZE: u64 = 16 * 1024 * 1024;
const BINARY_HEADER_SIZE: usize = 120;
const BINARY_SENTINEL: &[u8; 8] = b"PBLAPP\0\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PbwInfo {
    pub uuid: Uuid,
    pub name: String,
    pub version: String,
    pub watchface: bool,
    pub configurable: bool,
    pub platform: WatchType,
}

#[derive(Debug)]
pub struct PbwBundle {
    pub info: PbwInfo,
    pub executable: Vec<u8>,
    pub resources: Option<Vec<u8>>,
    pub worker: Option<Vec<u8>>,
    header: BinaryHeader,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AppInfo {
    uuid: String,
    short_name: String,
    #[serde(default)]
    long_name: String,
    version_label: String,
    #[serde(default)]
    watchapp: WatchApp,
    #[serde(default)]
    capabilities: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WatchApp {
    #[serde(default)]
    watchface: bool,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    application: ManifestBlob,
    #[serde(default)]
    resources: Option<ManifestBlob>,
    #[serde(default)]
    worker: Option<ManifestBlob>,
}

#[derive(Debug, Deserialize)]
struct ManifestBlob {
    name: String,
    size: u64,
}

#[derive(Debug, Clone, Copy)]
struct BinaryHeader {
    sdk_major: u8,
    sdk_minor: u8,
    flags: u32,
    icon: u32,
}

impl PbwBundle {
    pub fn parse(bytes: &[u8], watch_type: WatchType) -> Result<Self, PebbleError> {
        if watch_type == WatchType::Unknown {
            return Err(PebbleError::Other(
                "watch reported an unknown hardware platform".into(),
            ));
        }

        let app_info = parse_app_info(bytes)?;
        let uuid = Uuid::parse_str(&app_info.uuid)
            .map_err(|error| PebbleError::Other(format!("invalid PBW app UUID: {error}")))?;

        let (platform, prefix, manifest) = select_manifest(bytes, watch_type)?;
        let executable = manifest_entry(bytes, &prefix, &manifest.application)?;
        let header = parse_binary_header(&executable)?;
        let resources = manifest
            .resources
            .as_ref()
            .map(|blob| manifest_entry(bytes, &prefix, blob))
            .transpose()?;
        let worker = manifest
            .worker
            .as_ref()
            .map(|blob| manifest_entry(bytes, &prefix, blob))
            .transpose()?;

        Ok(Self {
            info: PbwInfo {
                uuid,
                name: if app_info.long_name.is_empty() {
                    app_info.short_name
                } else {
                    app_info.long_name
                },
                version: app_info.version_label,
                watchface: app_info.watchapp.watchface,
                configurable: app_info
                    .capabilities
                    .iter()
                    .any(|capability| capability == "configurable"),
                platform,
            },
            executable,
            resources,
            worker,
            header,
        })
    }

    /// Read the app-level configurable capability without selecting a watch
    /// binary. This is used to backfill metadata for already-retained PBWs.
    pub fn is_configurable(bytes: &[u8]) -> Result<bool, PebbleError> {
        Ok(parse_app_info(bytes)?
            .capabilities
            .iter()
            .any(|capability| capability == "configurable"))
    }

    /// BlobDB App metadata value matching libpebble3's `AppMetadata` layout.
    pub(crate) fn metadata_blob(&self) -> [u8; 126] {
        let mut out = [0; 126];
        out[0..16].copy_from_slice(self.info.uuid.as_bytes());
        out[16..20].copy_from_slice(&self.header.flags.to_le_bytes());
        out[20..24].copy_from_slice(&self.header.icon.to_le_bytes());
        let (version_major, version_minor) = version_bytes(&self.info.version);
        out[24] = version_major;
        out[25] = version_minor;
        out[26] = self.header.sdk_major;
        out[27] = self.header.sdk_minor;
        // bytes 28 and 29 are legacy app-face fields and remain zero.
        let name = self.info.name.as_bytes();
        let mut name_len = name.len().min(95);
        while !self.info.name.is_char_boundary(name_len) {
            name_len -= 1;
        }
        out[30..30 + name_len].copy_from_slice(&name[..name_len]);
        out
    }
}

fn parse_app_info(bytes: &[u8]) -> Result<AppInfo, PebbleError> {
    if bytes.len() > MAX_PBW_SIZE {
        return Err(PebbleError::Other(format!(
            "PBW is too large ({} bytes; maximum is {MAX_PBW_SIZE})",
            bytes.len()
        )));
    }
    serde_json::from_slice(&required_entry(bytes, "appinfo.json")?)
        .map_err(|error| PebbleError::Other(format!("invalid PBW appinfo.json: {error}")))
}

fn select_manifest(
    bytes: &[u8],
    watch_type: WatchType,
) -> Result<(WatchType, String, Manifest), PebbleError> {
    for &candidate in watch_type.compatible_app_variants() {
        let paths: &[(&str, &str)] = if candidate == WatchType::Aplite {
            &[("aplite/manifest.json", "aplite/"), ("manifest.json", "")]
        } else {
            // Filled below because the codename is dynamic.
            &[]
        };
        if candidate == WatchType::Aplite {
            for &(path, prefix) in paths {
                if let Some(manifest) = optional_json_entry(bytes, path)? {
                    return Ok((candidate, prefix.to_owned(), manifest));
                }
            }
        } else {
            let prefix = format!("{}/", candidate.codename());
            let path = format!("{prefix}manifest.json");
            if let Some(manifest) = optional_json_entry(bytes, &path)? {
                return Ok((candidate, prefix, manifest));
            }
        }
    }
    Err(PebbleError::Other(format!(
        "PBW has no variant compatible with {}",
        watch_type.codename()
    )))
}

fn optional_json_entry(bytes: &[u8], name: &str) -> Result<Option<Manifest>, PebbleError> {
    let Some(data) = optional_entry(bytes, name)? else {
        return Ok(None);
    };
    serde_json::from_slice(&data)
        .map(Some)
        .map_err(|error| PebbleError::Other(format!("invalid PBW {name}: {error}")))
}

fn manifest_entry(bytes: &[u8], prefix: &str, blob: &ManifestBlob) -> Result<Vec<u8>, PebbleError> {
    if blob.name.contains('/') || blob.name.contains('\\') || blob.name == "." || blob.name == ".."
    {
        return Err(PebbleError::Other(format!(
            "invalid PBW manifest entry name {:?}",
            blob.name
        )));
    }
    let path = format!("{prefix}{}", blob.name);
    let data = required_entry(bytes, &path)?;
    if data.len() as u64 != blob.size {
        return Err(PebbleError::Other(format!(
            "PBW {path} size mismatch: manifest says {}, archive contains {}",
            blob.size,
            data.len()
        )));
    }
    Ok(data)
}

fn required_entry(bytes: &[u8], name: &str) -> Result<Vec<u8>, PebbleError> {
    optional_entry(bytes, name)?
        .ok_or_else(|| PebbleError::Other(format!("PBW does not contain required entry {name}")))
}

fn optional_entry(bytes: &[u8], name: &str) -> Result<Option<Vec<u8>>, PebbleError> {
    let mut archive = ZipArchive::new(Cursor::new(bytes))
        .map_err(|error| PebbleError::Other(format!("invalid PBW ZIP archive: {error}")))?;
    let mut entry = match archive.by_name(name) {
        Ok(entry) => entry,
        Err(ZipError::FileNotFound) => return Ok(None),
        Err(error) => {
            return Err(PebbleError::Other(format!(
                "failed to read PBW entry {name}: {error}"
            )));
        }
    };
    if entry.is_dir() {
        return Err(PebbleError::Other(format!(
            "PBW entry {name} is a directory"
        )));
    }
    if entry.size() > MAX_BLOB_SIZE {
        return Err(PebbleError::Other(format!(
            "PBW entry {name} is too large ({} bytes)",
            entry.size()
        )));
    }
    let mut data = Vec::with_capacity(entry.size() as usize);
    entry
        .read_to_end(&mut data)
        .map_err(|error| PebbleError::Other(format!("failed to decompress PBW {name}: {error}")))?;
    Ok(Some(data))
}

fn parse_binary_header(data: &[u8]) -> Result<BinaryHeader, PebbleError> {
    if data.len() < BINARY_HEADER_SIZE || &data[..8] != BINARY_SENTINEL {
        return Err(PebbleError::Other(
            "PBW application has an invalid PBLAPP header".into(),
        ));
    }
    Ok(BinaryHeader {
        sdk_major: data[10],
        sdk_minor: data[11],
        icon: u32::from_le_bytes(data[88..92].try_into().unwrap()),
        flags: u32::from_le_bytes(data[96..100].try_into().unwrap()),
    })
}

fn version_bytes(version: &str) -> (u8, u8) {
    let mut parts = version.split(['.', '-']);
    let byte = |value: Option<&str>| {
        value
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(0)
            .min(u8::MAX as u16) as u8
    };
    (byte(parts.next()), byte(parts.next()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::{ZipWriter, write::SimpleFileOptions};

    fn binary(icon: u32) -> Vec<u8> {
        let mut data = vec![0; BINARY_HEADER_SIZE];
        data[..8].copy_from_slice(BINARY_SENTINEL);
        data[10..12].copy_from_slice(&[5, 19]);
        data[88..92].copy_from_slice(&icon.to_le_bytes());
        data
    }

    fn add_file(zip: &mut ZipWriter<Cursor<Vec<u8>>>, name: &str, data: &[u8]) {
        zip.start_file(name, SimpleFileOptions::default()).unwrap();
        zip.write_all(data).unwrap();
    }

    fn variant_pbw() -> Vec<u8> {
        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        add_file(
            &mut zip,
            "appinfo.json",
            br#"{"uuid":"5bfacb04-9449-461e-b3e6-7637d490ed53","shortName":"Variants","versionLabel":"1.0","watchapp":{"watchface":false},"capabilities":["configurable"]}"#,
        );
        for (prefix, icon) in [("", 11), ("chalk/", 22)] {
            let binary = binary(icon);
            let manifest = format!(
                r#"{{"application":{{"name":"pebble-app.bin","size":{}}},"resources":null}}"#,
                binary.len()
            );
            add_file(
                &mut zip,
                &format!("{prefix}manifest.json"),
                manifest.as_bytes(),
            );
            add_file(&mut zip, &format!("{prefix}pebble-app.bin"), &binary);
        }
        zip.finish().unwrap().into_inner()
    }

    #[test]
    fn header_and_metadata_match_wire_layout() {
        let uuid = Uuid::parse_str("5bfacb04-9449-461e-b3e6-7637d490ed53").unwrap();
        let bundle = PbwBundle {
            info: PbwInfo {
                uuid,
                name: "Test App".into(),
                version: "300.7-beta".into(),
                watchface: false,
                configurable: false,
                platform: WatchType::Basalt,
            },
            executable: vec![],
            resources: None,
            worker: None,
            header: BinaryHeader {
                sdk_major: 5,
                sdk_minor: 19,
                flags: 0x1234_5678,
                icon: 42,
            },
        };
        let metadata = bundle.metadata_blob();
        assert_eq!(&metadata[..16], uuid.as_bytes());
        assert_eq!(&metadata[16..20], &0x1234_5678_u32.to_le_bytes());
        assert_eq!(&metadata[20..24], &42_u32.to_le_bytes());
        assert_eq!(&metadata[24..28], &[255, 7, 5, 19]);
        assert_eq!(&metadata[30..38], b"Test App");
        assert!(metadata[38..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn metadata_name_is_nul_terminated_and_truncated_at_utf8_boundary() {
        let mut bundle = PbwBundle {
            info: PbwInfo {
                uuid: Uuid::nil(),
                name: "é".repeat(48),
                version: "1.0".into(),
                watchface: false,
                configurable: false,
                platform: WatchType::Basalt,
            },
            executable: vec![],
            resources: None,
            worker: None,
            header: BinaryHeader {
                sdk_major: 5,
                sdk_minor: 19,
                flags: 0,
                icon: 0,
            },
        };

        let metadata = bundle.metadata_blob();
        assert_eq!(&metadata[30..124], "é".repeat(47).as_bytes());
        assert_eq!(&metadata[124..126], &[0, 0]);

        bundle.info.name = "a".repeat(96);
        let metadata = bundle.metadata_blob();
        assert_eq!(&metadata[30..125], "a".repeat(95).as_bytes());
        assert_eq!(metadata[125], 0);
    }

    #[test]
    fn parses_binary_header_fields() {
        let mut data = [0; BINARY_HEADER_SIZE];
        data[..8].copy_from_slice(BINARY_SENTINEL);
        data[10..12].copy_from_slice(&[3, 8]);
        data[88..92].copy_from_slice(&11_u32.to_le_bytes());
        data[96..100].copy_from_slice(&0xaabb_ccdd_u32.to_le_bytes());
        let header = parse_binary_header(&data).unwrap();
        assert_eq!((header.sdk_major, header.sdk_minor), (3, 8));
        assert_eq!(header.icon, 11);
        assert_eq!(header.flags, 0xaabb_ccdd);
    }

    #[test]
    fn selects_native_variant_before_compatible_fallback() {
        let pbw = variant_pbw();
        assert!(PbwBundle::is_configurable(&pbw).unwrap());
        let chalk = PbwBundle::parse(&pbw, WatchType::Chalk).unwrap();
        assert_eq!(chalk.info.platform, WatchType::Chalk);
        assert!(chalk.info.configurable);
        assert_eq!(chalk.header.icon, 22);

        let basalt = PbwBundle::parse(&pbw, WatchType::Basalt).unwrap();
        assert_eq!(basalt.info.platform, WatchType::Aplite);
        assert_eq!(basalt.header.icon, 11);
    }
}
