use bytes::{Bytes, BytesMut};
use std::cmp::Ordering;
use std::collections::BTreeMap;

pub type Result<T> = anyhow::Result<T>;

const CRC_64: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_ECMA_182);

#[derive(Debug)]
struct Frame {
    pgno: i32,
    bytes: BytesMut,
    crc: u64,
}

#[derive(Debug)]
pub struct Replicator {
    pub bucket: s3::bucket::Bucket,
    write_buffer: BTreeMap<u32, Frame>,
    pub page_size: usize,
    generation: uuid::Uuid,
    pub commits_in_current_generation: u32,
    next_frame: u32,
    verify_crc: bool,
    last_frame_crc: u64,
    last_transaction_crc: u64,
    pub db_path: String,
    pub db_name: String,
}

#[derive(Debug)]
pub struct FetchedResults {
    pub pages: Vec<(i32, Bytes)>,
    pub next_marker: Option<String>,
}

#[derive(Debug)]
pub enum RestoreAction {
    None,
    SnapshotMainDbFile,
    ReuseGeneration(uuid::Uuid),
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub create_bucket_if_not_exists: bool,
    pub verify_crc: bool,
}

impl Replicator {
    pub const UNSET_PAGE_SIZE: usize = usize::MAX;

    pub fn new() -> Result<Self> {
        Self::create(Options {
            create_bucket_if_not_exists: false,
            verify_crc: true,
        })
    }

    pub fn create(options: Options) -> Result<Self> {
        let write_buffer = BTreeMap::new();
        let bucket_name =
            std::env::var("LIBSQL_BOTTOMLESS_BUCKET").unwrap_or_else(|_| "bottomless".to_string());
        let region = match std::env::var("LIBSQL_BOTTOMLESS_ENDPOINT") {
            Ok(endpoint) => s3::region::Region::Custom {
                region: "dummy".to_owned(),
                endpoint,
            },
            Err(_) => {
                std::env::var("AWS_REGION")
                    .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                    .unwrap_or_else(|_| "us-east-1".to_string())
                    .parse()? // FIXME: get it from .aws profile
            }
        };
        let credentials = s3::creds::Credentials::from_env()
            .or_else(|_| s3::creds::Credentials::from_profile(None))?;
        let bucket = s3::bucket::Bucket::new(&bucket_name, region, credentials)?.with_path_style();
        let generation = Self::generate_generation();
        tracing::debug!("Generation {}", generation);

        Ok(Self {
            bucket,
            write_buffer,
            page_size: Self::UNSET_PAGE_SIZE,
            generation,
            commits_in_current_generation: 0,
            next_frame: 1,
            verify_crc: options.verify_crc,
            last_frame_crc: 0,
            last_transaction_crc: 0,
            db_path: String::new(),
            db_name: String::new(),
        })
    }

    // The database can use different page size - as soon as it's known,
    // it should be communicated to the replicator via this call.
    // NOTICE: in practice, WAL journaling mode does not allow changing page sizes,
    // so verifying that it hasn't changed is a panic check. Perhaps in the future
    // it will be useful, if WAL ever allows changing the page size.
    pub fn set_page_size(&mut self, page_size: usize) -> Result<()> {
        tracing::trace!("Setting page size from {} to {}", self.page_size, page_size);
        if self.page_size != Self::UNSET_PAGE_SIZE && self.page_size != page_size {
            return Err(anyhow::anyhow!(
                "Cannot set page size to {}, it was already set to {}",
                page_size,
                self.page_size
            ));
        }
        self.page_size = page_size;
        Ok(())
    }

    // Generates a new generation UUID v7, which contains a timestamp and is binary-sortable.
    // This timestamp goes back in time - that allows us to list newest generations
    // first in the S3-compatible bucket, under the assumption that fetching newest generations
    // is the most common operation.
    // NOTICE: at the time of writing, uuid v7 is an unstable feature of the uuid crate
    fn generate_generation() -> uuid::Uuid {
        let (seconds, nanos) = uuid::timestamp::Timestamp::now(uuid::NoContext).to_unix();
        let (seconds, nanos) = (253370761200 - seconds, 999999999 - nanos);
        let synthetic_ts = uuid::Timestamp::from_unix(uuid::NoContext, seconds, nanos);
        uuid::Uuid::new_v7(synthetic_ts)
    }

    // Starts a new generation for this replicator instance
    pub fn new_generation(&mut self) {
        tracing::debug!("New generation started: {}", self.generation);
        self.set_generation(Self::generate_generation());
    }

    // Sets a generation for this replicator instance. This function
    // should be called if a generation number from S3-compatible storage
    // is reused in this session.
    pub fn set_generation(&mut self, generation: uuid::Uuid) {
        self.generation = generation;
        self.commits_in_current_generation = 0;
        self.next_frame = 1; // New generation marks a new WAL
        tracing::debug!("Generation set to {}", self.generation);
    }

    // Registers a database path for this replicator.
    pub fn register_db(&mut self, db_path: impl Into<String>) {
        let db_path = db_path.into();
        let name = match db_path.rfind('/') {
            Some(index) => db_path[index + 1..].to_string(),
            None => db_path.to_string(),
        };
        self.db_path = db_path;
        self.db_name = name;
        tracing::trace!("Registered {} (full path: {})", self.db_name, self.db_path);
    }

    // Returns the next free frame number for the replicated log
    fn next_frame(&mut self) -> u32 {
        self.next_frame += 1;
        self.next_frame - 1
    }

    // Returns the current last valid frame in the replicated log
    pub fn peek_last_valid_frame(&self) -> u32 {
        self.next_frame.saturating_sub(1)
    }

    // Sets the last valid frame in the replicated log.
    pub fn register_last_valid_frame(&mut self, frame: u32) {
        if frame != self.peek_last_valid_frame() {
            if self.next_frame != 1 {
                tracing::error!(
                    "[BUG] Local max valid frame is {}, while replicator thinks it's {}",
                    frame,
                    self.peek_last_valid_frame()
                );
            }
            self.next_frame = frame + 1
        }
    }

    // Writes pages to a local in-memory buffer
    pub fn write(&mut self, pgno: i32, data: &[u8]) {
        let frame = self.next_frame();
        let mut crc = CRC_64.digest_with_initial(self.last_frame_crc);
        crc.update(data);
        let crc = crc.finalize();
        tracing::trace!(
            "Writing page {}:{} at frame {}, crc: {}",
            pgno,
            data.len(),
            frame,
            crc
        );
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(data);
        self.write_buffer.insert(frame, Frame { pgno, bytes, crc });
        self.last_frame_crc = crc;
    }

    // Sends pages participating in current transaction to S3.
    // Returns the frame number holding the last flushed page.
    pub fn flush(&mut self) -> Result<u32> {
        if self.write_buffer.is_empty() {
            tracing::trace!("Attempting to flush an empty buffer");
            return Ok(0);
        }
        tracing::trace!("Flushing {} frames", self.write_buffer.len());
        self.commits_in_current_generation += 1;
        let last_frame_in_transaction_crc = self.write_buffer.iter().last().unwrap().1.crc;
        for (frame, Frame { pgno, bytes, crc }) in self.write_buffer.iter() {
            let data: &[u8] = bytes;
            if data.len() != self.page_size {
                tracing::warn!("Unexpected truncated page of size {}", data.len())
            }
            let mut compressor = flate2::bufread::GzEncoder::new(data, flate2::Compression::best());
            let mut compressed: Vec<u8> = Vec::with_capacity(self.page_size);
            std::io::copy(&mut compressor, &mut compressed)?;
            let key = format!(
                "{}-{}/{:012}-{:012}-{:016x}",
                self.db_name, self.generation, frame, pgno, crc
            );
            tracing::trace!("Flushing {} (compressed size: {})", key, compressed.len());
            self.bucket.put_object(key, &compressed)?;
        }
        self.write_buffer.clear();
        self.last_transaction_crc = last_frame_in_transaction_crc;
        tracing::trace!("Last transaction crc: {}", self.last_transaction_crc);
        Ok(self.next_frame - 1)
    }

    // Marks all recently flushed pages as committed and updates the frame number
    // holding the newest consistent committed transaction.
    pub fn finalize_commit(&mut self, last_frame: u32, checksum: [u32; 2]) -> Result<()> {
        // Last consistent frame is persisted in S3 in order to be able to recover
        // from failured that happen in the middle of a commit, when only some
        // of the pages that belong to a transaction are replicated.
        let last_consistent_frame_key = format!("{}-{}/.consistent", self.db_name, self.generation);
        tracing::trace!("Finalizing frame: {}, checksum: {:?}", last_frame, checksum);
        // Information kept in this entry: [last consistent frame number: 4 bytes][last checksum: 8 bytes]
        let mut consistent_info = Vec::with_capacity(12);
        consistent_info.extend_from_slice(&last_frame.to_be_bytes());
        consistent_info.extend_from_slice(&checksum[0].to_be_bytes());
        consistent_info.extend_from_slice(&checksum[1].to_be_bytes());
        self.bucket
            .put_object(last_consistent_frame_key, &consistent_info)?;
        tracing::trace!("Commit successful");
        Ok(())
    }

    // Drops uncommitted frames newer than given last valid frame
    pub fn rollback_to_frame(&mut self, last_valid_frame: u32) {
        // NOTICE: O(size), can be optimized to O(removed) if ever needed
        self.write_buffer.retain(|&k, _| k <= last_valid_frame);
        self.next_frame = last_valid_frame + 1;
        self.last_frame_crc = self
            .write_buffer
            .iter()
            .next_back()
            .map(|entry| entry.1.crc)
            .unwrap_or(self.last_transaction_crc);
        tracing::debug!(
            "Rolled back to {}, crc {} (last transaction crc = {})",
            self.next_frame - 1,
            self.last_frame_crc,
            self.last_transaction_crc,
        );
    }

    // Tries to read the local change counter from the given database file
    fn read_change_counter(reader: &mut std::fs::File) -> Result<[u8; 4]> {
        use std::io::{Read, Seek};
        let mut counter = [0u8; 4];
        reader.seek(std::io::SeekFrom::Start(24))?;
        reader.read_exact(&mut counter)?;
        Ok(counter)
    }

    // Tries to read the local page size from the given database file
    fn read_page_size(reader: &mut std::fs::File) -> Result<usize> {
        use byteorder::ReadBytesExt;
        use std::io::Seek;
        reader.seek(std::io::SeekFrom::Start(16))?;
        let page_size = reader.read_u16::<byteorder::BigEndian>()?;
        if page_size == 1 {
            Ok(65536)
        } else {
            Ok(page_size as usize)
        }
    }

    // Returns the compressed database file path and its change counter, extracted
    // from the header of page1 at offset 24..27 (as per SQLite documentation).
    pub fn compress_main_db_file(&self) -> Result<(Vec<u8>, [u8; 4])> {
        let mut reader = std::fs::File::open(&self.db_path)?;
        let mut compressed = std::io::Cursor::new(Vec::new());
        let mut writer =
            flate2::write::GzEncoder::new(&mut compressed, flate2::Compression::best());
        std::io::copy(&mut reader, &mut writer)?;
        drop(writer);
        let change_counter = Self::read_change_counter(&mut reader)?;
        Ok((compressed.into_inner(), change_counter))
    }

    // Replicates local WAL pages to S3, if local WAL is present.
    // This function is called under the assumption that if local WAL
    // file is present, it was already detected to be newer than its
    // remote counterpart.
    pub fn maybe_replicate_wal(&mut self) -> Result<()> {
        use byteorder::ReadBytesExt;
        use std::io::{Read, Seek};
        let mut wal_file = match std::fs::File::open(format!("{}-wal", &self.db_path)) {
            Ok(file) => file,
            Err(_) => {
                tracing::info!("Local WAL not present - not replicating");
                return Ok(());
            }
        };
        let len = match wal_file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(_) => 0,
        };
        if len < 32 {
            tracing::info!("Local WAL is empty, not replicating");
            return Ok(());
        }
        if self.page_size == Self::UNSET_PAGE_SIZE {
            tracing::trace!("Page size not detected yet, not replicated");
            return Ok(());
        }
        tracing::trace!("Local WAL pages: {}", (len - 32) / self.page_size as u64);
        wal_file.seek(std::io::SeekFrom::Start(24))?;
        let checksum: [u32; 2] = [
            wal_file.read_u32::<byteorder::BigEndian>()?,
            wal_file.read_u32::<byteorder::BigEndian>()?,
        ];
        tracing::trace!("Local WAL checksum: {:?}", checksum);
        let mut last_written_frame = 0;
        for offset in (32..len).step_by(self.page_size + 24) {
            wal_file.seek(std::io::SeekFrom::Start(offset))?;
            let pgno = wal_file.read_i32::<byteorder::BigEndian>()?;
            let size_after = wal_file.read_u32::<byteorder::BigEndian>()?;
            tracing::trace!("Size after transaction for {}: {}", pgno, size_after);
            wal_file.seek(std::io::SeekFrom::Start(offset + 24))?;
            let mut data = vec![0u8; self.page_size];
            wal_file.read_exact(&mut data)?;
            self.write(pgno, &data);
            // In multi-page transactions, only the last page in the transaction contains
            // the size_after_transaction field. If it's zero, it means it's an uncommited
            // page.
            if size_after != 0 {
                last_written_frame = self.flush()?;
            }
        }
        if last_written_frame > 0 {
            self.finalize_commit(last_written_frame, checksum)?;
        }
        if !self.write_buffer.is_empty() {
            tracing::warn!("Uncommited WAL entries: {}", self.write_buffer.len());
        }
        self.write_buffer.clear();
        tracing::info!("Local WAL replicated");
        Ok(())
    }

    // Check if the local database file exists and contains data
    fn main_db_exists_and_not_empty(&self) -> bool {
        let file = match std::fs::File::open(&self.db_path) {
            Ok(file) => file,
            Err(_) => return false,
        };
        match file.metadata() {
            Ok(metadata) => metadata.len() > 0,
            Err(_) => false,
        }
    }

    // Sends the main database file to S3 - if -wal file is present, it's replicated
    // too - it means that the local file was detected to be newer than its remote
    // counterpart.
    pub fn snapshot_main_db_file(&mut self) -> Result<()> {
        if !self.main_db_exists_and_not_empty() {
            tracing::debug!("Not snapshotting, the main db file does not exist or is empty");
            return Ok(());
        }
        tracing::debug!("Snapshotting {}", self.db_path);

        let (compressed_db, change_counter) = self.compress_main_db_file()?;
        let key = format!("{}-{}/db.gz", self.db_name, self.generation);
        self.bucket.put_object(key, &compressed_db)?;
        /* FIXME: we can't rely on the change counter in WAL mode:
         ** "In WAL mode, changes to the database are detected using the wal-index and
         ** so the change counter is not needed. Hence, the change counter might not be
         ** incremented on each transaction in WAL mode."
         ** Instead, we need to consult WAL checksums.
         */
        let change_counter_key = format!("{}-{}/.changecounter", self.db_name, self.generation);
        let response = self
            .bucket
            .put_object(change_counter_key, &change_counter)?;
        if response.status_code() != 200 {
            return Err(anyhow::anyhow!(
                "Failed to snapshot main db file, status {}",
                response.status_code()
            ));
        }
        tracing::debug!("Main db snapshot complete");
        Ok(())
    }

    // Returns newest replicated generation, or None, if one is not found.
    // FIXME: assumes that this bucket stores *only* generations for databases,
    // it should be more robust and continue looking if the first item does not
    // match the <db-name>-<generation-uuid>/ pattern.
    pub fn find_newest_generation(&self) -> Option<uuid::Uuid> {
        let prefix = format!("{}-", self.db_name);
        let (response, _status) = self
            .bucket
            .list_page(prefix, None, None, None, Some(1))
            .ok()?;
        let key = &response.contents.first()?.key;
        let key = match key.find('/') {
            Some(index) => &key[self.db_name.len() + 1..index],
            None => key,
        };
        tracing::debug!("Generation candidate: {}", key);
        uuid::Uuid::parse_str(key).ok()
    }

    // Tries to fetch the remote database change counter from given generation
    pub fn get_remote_change_counter(&self, generation: &uuid::Uuid) -> Result<[u8; 4]> {
        use bytes::Buf;
        let mut remote_change_counter = [0u8; 4];
        if let Ok(response) = self
            .bucket
            .get_object(format!("{}-{}/.changecounter", self.db_name, generation))
        {
            if response.status_code() == 200 {
                response.bytes().copy_to_slice(&mut remote_change_counter)
            }
        }
        Ok(remote_change_counter)
    }

    // Tries to fetch the last consistent frame number stored in the remote generation
    pub fn get_last_consistent_frame(&self, generation: &uuid::Uuid) -> Result<(u32, u64)> {
        use bytes::Buf;
        Ok(
            match self
                .bucket
                .get_object(format!("{}-{}/.consistent", self.db_name, generation))
                .ok()
            {
                Some(response) => {
                    if response.status_code() == 200 {
                        let mut collected = response.bytes();
                        (collected.get_u32(), collected.get_u64())
                    } else {
                        (0, 0)
                    }
                }
                None => (0, 0),
            },
        )
    }

    // Returns the number of pages stored in the local WAL file, or 0, if there aren't any.
    fn get_local_wal_page_count(&mut self) -> u32 {
        use byteorder::ReadBytesExt;
        use std::io::Seek;
        match std::fs::File::open(format!("{}-wal", &self.db_path)) {
            Ok(mut file) => {
                let metadata = match file.metadata() {
                    Ok(metadata) => metadata,
                    Err(_) => return 0,
                };
                let len = metadata.len();
                if len >= 32 {
                    // Page size is stored in WAL file at offset [8-12)
                    if file.seek(std::io::SeekFrom::Start(8)).is_err() {
                        return 0;
                    };
                    let page_size = match file.read_u32::<byteorder::BigEndian>() {
                        Ok(size) => size,
                        Err(_) => return 0,
                    };
                    if self.set_page_size(page_size as usize).is_err() {
                        return 0;
                    }
                    // Each WAL file consists of a 32-byte WAL header and N entries of size (page size + 24)
                    (len / (self.page_size + 24) as u64) as u32
                } else {
                    0
                }
            }
            Err(_) => 0,
        }
    }

    // Parses the frame and page number from given key.
    // Format: <db-name>-<generation>/<frame-number>-<page-number>-<crc64>
    fn parse_frame_page_crc(key: &str) -> Option<(u32, i32, u64)> {
        let checksum_delim = key.rfind('-')?;
        let page_delim = key[0..checksum_delim].rfind('-')?;
        let frame_delim = key[0..page_delim].rfind('/')?;
        let frameno = key[frame_delim + 1..page_delim].parse::<u32>().ok()?;
        let pgno = key[page_delim + 1..checksum_delim].parse::<i32>().ok()?;
        let crc = u64::from_str_radix(&key[checksum_delim + 1..], 16).ok()?;
        tracing::debug!(frameno, pgno, crc);
        Some((frameno, pgno, crc))
    }

    // Restores the database state from given remote generation
    pub fn restore_from(&mut self, generation: uuid::Uuid) -> Result<RestoreAction> {
        use std::io::{Seek, Write};

        // Check if the database needs to be restored by inspecting the database
        // change counter and the WAL size.
        let local_counter = match std::fs::File::open(&self.db_path) {
            Ok(mut db) => {
                // While reading the main database file for the first time,
                // page size from an existing database should be set.
                if let Ok(page_size) = Self::read_page_size(&mut db) {
                    self.set_page_size(page_size)?;
                }
                Self::read_change_counter(&mut db).unwrap_or([0u8; 4])
            }
            Err(_) => [0u8; 4],
        };

        let remote_counter = self.get_remote_change_counter(&generation)?;
        tracing::debug!("Counters: l={:?}, r={:?}", local_counter, remote_counter);

        let (last_consistent_frame, checksum) = self.get_last_consistent_frame(&generation)?;
        tracing::debug!(
            "Last consistent remote frame: {}; checksum: {:x}",
            last_consistent_frame,
            checksum
        );

        let wal_pages = self.get_local_wal_page_count();
        match local_counter.cmp(&remote_counter) {
            Ordering::Equal => {
                tracing::debug!(
                    "Consistent: {}; wal pages: {}",
                    last_consistent_frame,
                    wal_pages
                );
                match wal_pages.cmp(&last_consistent_frame) {
                    Ordering::Equal => {
                        tracing::info!(
                            "Remote generation is up-to-date, reusing it in this session"
                        );
                        self.next_frame = wal_pages + 1;
                        return Ok(RestoreAction::ReuseGeneration(generation));
                    }
                    Ordering::Greater => {
                        tracing::info!("Local change counter matches the remote one, but local WAL contains newer data, which needs to be replicated");
                        return Ok(RestoreAction::SnapshotMainDbFile);
                    }
                    Ordering::Less => (),
                }
            }
            Ordering::Greater => {
                tracing::info!("Local change counter is larger than its remote counterpart - a new snapshot needs to be replicated");
                return Ok(RestoreAction::SnapshotMainDbFile);
            }
            Ordering::Less => (),
        }

        std::fs::rename(&self.db_path, format!("{}.bottomless.backup", self.db_path)).ok(); // Best effort
        let mut main_db_writer = std::fs::File::create(&self.db_path)?;
        // If the db file is not present, the database could have been empty
        if let Ok(db_file) = self
            .bucket
            .get_object(format!("{}-{}/db.gz", self.db_name, generation))
        {
            if db_file.status_code() == 200 {
                let mut decompress_reader =
                    flate2::bufread::GzDecoder::new(std::io::BufReader::new(db_file.bytes()));
                std::io::copy(&mut decompress_reader, &mut main_db_writer)?;
                main_db_writer.flush()?;
                tracing::info!("Restored the main database file");
            }
        }

        let mut next_marker = None;
        let prefix = format!("{}-{}/", self.db_name, generation);
        tracing::debug!("Overwriting any existing WAL file: {}-wal", &self.db_path);
        std::fs::remove_file(format!("{}-wal", &self.db_path)).ok();
        std::fs::remove_file(format!("{}-shm", &self.db_path)).ok();

        let mut applied_wal_frame = false;
        loop {
            let (response, _status) =
                self.bucket
                    .list_page(prefix.clone(), None, next_marker.clone(), None, None)?;
            let objs = response.contents;
            let mut prev_crc = 0;
            let mut page_buffer = Vec::with_capacity(65536); // best guess for the page size - it will certainly not be more than 64KiB
            for obj in &objs {
                let key = &obj.key;
                tracing::debug!("Loading {}", key);
                let frame = self.bucket.get_object(key)?;

                let (frameno, pgno, crc) = match Self::parse_frame_page_crc(key) {
                    Some(result) => result,
                    None => {
                        if !key.ends_with(".gz")
                            && !key.ends_with(".consistent")
                            && !key.ends_with(".changecounter")
                        {
                            tracing::warn!("Failed to parse frame/page from key {}", key);
                        }
                        continue;
                    }
                };
                if frameno > last_consistent_frame {
                    tracing::warn!("Remote log contains frame {} larger than last consistent frame ({}), stopping the restoration process",
                                frameno, last_consistent_frame);
                    break;
                }
                let mut decompress_reader =
                    flate2::bufread::GzDecoder::new(std::io::BufReader::new(frame.bytes()));
                // If page size is unknown *or* crc verification is performed,
                // a page needs to be loaded to memory first
                if self.verify_crc || self.page_size == Self::UNSET_PAGE_SIZE {
                    let page_size = std::io::copy(&mut decompress_reader, &mut page_buffer)?;
                    if self.verify_crc {
                        let mut expected_crc = CRC_64.digest_with_initial(prev_crc);
                        expected_crc.update(&page_buffer);
                        let expected_crc = expected_crc.finalize();
                        tracing::debug!(crc, expected_crc);
                        if crc != expected_crc {
                            tracing::warn!(
                                "CRC check failed: {:016x} != {:016x} (expected)",
                                crc,
                                expected_crc
                            );
                        }
                        prev_crc = crc;
                    }
                    self.set_page_size(page_size as usize)?;
                    let offset = (pgno - 1) as u64 * page_size;
                    main_db_writer.seek(std::io::SeekFrom::Start(offset))?;
                    std::io::copy(&mut &page_buffer[..], &mut main_db_writer)?;
                    page_buffer.clear();
                } else {
                    let offset = (pgno - 1) as u64 * self.page_size as u64;
                    main_db_writer.seek(std::io::SeekFrom::Start(offset))?;
                    // FIXME: we only need to overwrite with the newest page,
                    // no need to replay the whole WAL
                    std::io::copy(&mut decompress_reader, &mut main_db_writer)?;
                }
                main_db_writer.flush()?;
                tracing::debug!("Written frame {} as main db page {}", frameno, pgno);
                applied_wal_frame = true;
            }
            next_marker = response
                .is_truncated
                .then(|| objs.last().map(|elem| elem.key.clone()))
                .flatten();
            if next_marker.is_none() {
                break;
            }
        }

        if applied_wal_frame {
            Ok::<_, anyhow::Error>(RestoreAction::SnapshotMainDbFile)
        } else {
            Ok::<_, anyhow::Error>(RestoreAction::None)
        }
    }

    // Restores the database state from newest remote generation
    pub fn restore(&mut self) -> Result<RestoreAction> {
        let newest_generation = match self.find_newest_generation() {
            Some(gen) => gen,
            None => {
                tracing::debug!("No generation found, nothing to restore");
                return Ok(RestoreAction::SnapshotMainDbFile);
            }
        };

        tracing::info!("Restoring from generation {}", newest_generation);
        self.restore_from(newest_generation)
    }
}

pub struct Context {
    pub replicator: Replicator,
}
