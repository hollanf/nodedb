//! External sort infrastructure: sort helpers, run files, and k-way merge.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read as _, Write as _};

use tracing::debug;

use crate::data::executor::core_loop::CoreLoop;

use crate::bridge::scan_filter::compare_json_values;

impl CoreLoop {
    /// External sort: split filtered rows into sorted runs, spill each run
    /// to a temp file, then k-way merge to produce the final sorted output.
    pub(super) fn external_sort(
        &self,
        rows: Vec<(String, Vec<u8>)>,
        sort_keys: &[(String, bool)],
        output_limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        // Spill directory for temporary sort run files. Temp files are
        // auto-deleted on Drop; the directory persists but is cleaned up
        // on the next external_sort call or server restart.
        let spill_dir = self
            .data_dir
            .join(format!("sort-spill/core-{}", self.core_id));
        std::fs::create_dir_all(&spill_dir).map_err(|e| crate::Error::Storage {
            engine: "sort".into(),
            detail: format!("failed to create sort spill dir: {e}"),
        })?;

        let total_rows = rows.len();

        let mut run_files = Vec::new();
        for chunk in rows.chunks(self.query_tuning.sort_run_size) {
            let mut run: Vec<(String, Vec<u8>)> = chunk.to_vec();
            sort_rows(&mut run, sort_keys);

            let file = tempfile::tempfile_in(&spill_dir).map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("failed to create sort temp file: {e}"),
            })?;
            let mut writer = BufWriter::new(file);

            let count = run.len() as u32;
            writer
                .write_all(&count.to_le_bytes())
                .map_err(|e| crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("sort spill write: {e}"),
                })?;
            for (id, val) in &run {
                let id_bytes = id.as_bytes();
                writer
                    .write_all(&(id_bytes.len() as u32).to_le_bytes())
                    .map_err(|e| crate::Error::Storage {
                        engine: "sort".into(),
                        detail: format!("sort spill write: {e}"),
                    })?;
                writer
                    .write_all(id_bytes)
                    .map_err(|e| crate::Error::Storage {
                        engine: "sort".into(),
                        detail: format!("sort spill write: {e}"),
                    })?;
                writer
                    .write_all(&(val.len() as u32).to_le_bytes())
                    .map_err(|e| crate::Error::Storage {
                        engine: "sort".into(),
                        detail: format!("sort spill write: {e}"),
                    })?;
                writer.write_all(val).map_err(|e| crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("sort spill write: {e}"),
                })?;
            }
            writer.flush().map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("sort spill flush: {e}"),
            })?;

            let mut file = writer.into_inner().map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("sort spill into_inner: {e}"),
            })?;
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(0))
                .map_err(|e| crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("sort spill seek: {e}"),
                })?;

            run_files.push(file);
        }

        debug!(
            core = self.core_id,
            runs = run_files.len(),
            total_rows,
            "external sort: spilled runs"
        );

        let mut readers: Vec<RunReader> = run_files
            .into_iter()
            .enumerate()
            .filter_map(|(idx, file)| RunReader::new(file, idx).ok())
            .collect();

        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        for reader in &mut readers {
            if let Some(row) = reader.next_row() {
                heap.push(Reverse(MergeEntry {
                    row,
                    run_idx: reader.run_idx,
                    sort_keys: sort_keys.to_vec(),
                }));
            }
        }

        let mut result = Vec::with_capacity(output_limit.min(total_rows));
        while let Some(Reverse(entry)) = heap.pop() {
            result.push(entry.row);
            if result.len() >= output_limit {
                break;
            }
            if let Some(next_row) = readers[entry.run_idx].next_row() {
                heap.push(Reverse(MergeEntry {
                    row: next_row,
                    run_idx: entry.run_idx,
                    sort_keys: sort_keys.to_vec(),
                }));
            }
        }

        Ok(result)
    }
}

/// Compare two JSON documents by a list of sort keys.
///
/// Used by both in-memory sort and external merge sort to ensure
/// consistent ordering across all code paths.
pub(super) fn compare_docs_by_keys(
    a_doc: &serde_json::Value,
    b_doc: &serde_json::Value,
    sort_keys: &[(String, bool)],
) -> std::cmp::Ordering {
    for (field, asc) in sort_keys {
        let a_val = a_doc.get(field.as_str());
        let b_val = b_doc.get(field.as_str());
        let cmp = compare_json_values(a_val, b_val);
        let ordered = if *asc { cmp } else { cmp.reverse() };
        if ordered != std::cmp::Ordering::Equal {
            return ordered;
        }
    }
    std::cmp::Ordering::Equal
}

pub(super) fn sort_rows(rows: &mut [(String, Vec<u8>)], sort_keys: &[(String, bool)]) {
    rows.sort_by(|(_, a_bytes), (_, b_bytes)| {
        let a_doc = super::super::super::doc_format::decode_document(a_bytes)
            .unwrap_or(serde_json::Value::Null);
        let b_doc = super::super::super::doc_format::decode_document(b_bytes)
            .unwrap_or(serde_json::Value::Null);
        compare_docs_by_keys(&a_doc, &b_doc, sort_keys)
    });
}

pub(super) struct RunReader {
    pub(super) reader: BufReader<std::fs::File>,
    pub(super) remaining: u32,
    pub(super) run_idx: usize,
}

impl RunReader {
    pub(super) fn new(file: std::fs::File, run_idx: usize) -> crate::Result<Self> {
        let mut reader = BufReader::new(file);
        let mut buf4 = [0u8; 4];
        reader
            .read_exact(&mut buf4)
            .map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("run reader init: {e}"),
            })?;
        let count = u32::from_le_bytes(buf4);
        Ok(Self {
            reader,
            remaining: count,
            run_idx,
        })
    }

    pub(super) fn next_row(&mut self) -> Option<(String, Vec<u8>)> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        let mut buf4 = [0u8; 4];

        if self.reader.read_exact(&mut buf4).is_err() {
            return None;
        }
        let id_len = u32::from_le_bytes(buf4) as usize;
        let mut id_buf = vec![0u8; id_len];
        if self.reader.read_exact(&mut id_buf).is_err() {
            return None;
        }
        let id = String::from_utf8(id_buf).unwrap_or_default();

        if self.reader.read_exact(&mut buf4).is_err() {
            return None;
        }
        let val_len = u32::from_le_bytes(buf4) as usize;
        let mut val_buf = vec![0u8; val_len];
        if self.reader.read_exact(&mut val_buf).is_err() {
            return None;
        }

        Some((id, val_buf))
    }
}

pub(super) struct MergeEntry {
    pub(super) row: (String, Vec<u8>),
    pub(super) run_idx: usize,
    pub(super) sort_keys: Vec<(String, bool)>,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let a_doc = super::super::super::doc_format::decode_document(&self.row.1)
            .unwrap_or(serde_json::Value::Null);
        let b_doc = super::super::super::doc_format::decode_document(&other.row.1)
            .unwrap_or(serde_json::Value::Null);
        compare_docs_by_keys(&a_doc, &b_doc, &self.sort_keys)
    }
}
