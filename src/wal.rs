use std::fs::{File, OpenOptions, read_dir, remove_file};
use std::io;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::memtable::MemTable;

/// The WAL acts as an on-disk backup for the MemTable by keeping a running record of
/// all of the database operations. In the event of a restart, the MemTable can be
/// fully recovered by replaying the operations in the WAL. When a MemTable reaches
/// capacity and is transformed into a SSTable, the WAL is wiped from the disk to
/// make room for a new WAL.

/// PacaDB's WAL implementation utilizes a buffered I/O model. This
/// comes at the risk of losing writes in the instance of a server/instance
/// crash. However, we get the tradeoff of thousand times as fast async writes
/// according to Google's LevelDB.

// TODO: For future improvement, we can look to improve our WAL with
//   block models and checksums for storage just like RocksDB utilizes.
//   As an example, this is what RocksDB block format looks like:
//   +----------+-----------+-----------+----------------+--- ... ---+
//   | CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
//   +----------+-----------+-----------+----------------+--- ... ---+
//   CRC = 32bit hash computed over the payload using CRC
//   Size = Length of the payload data
//   Type = Type of record
//           (kZeroType, kFullType, kFirstType, kLastType, kMiddleType )
//           The type is used to group a bunch of records together to represent
//           blocks that are larger than kBlockSize
//   Payload = Byte stream as long as specified by the payload size
//   Log number = 32bit log file number, so that we can distinguish between
//   records written by the most recent log writer vs a previous one.

/// The entry in the WAL. This mirrors what a `MemTableEntry` looks like.
///
/// In our file, an entry looks as so:
/// +---------------+---------------+-----------------+-...-+--...--+-----------------+
/// | Key Size (8B) | Tombstone(1B) | Value Size (8B) | Key | Value | Timestamp (16B) |
/// +---------------+---------------+-----------------+-...-+--...--+-----------------+
/// Key Size = Length of the Key data
/// Tombstone = If this record was deleted and has a value
/// Value Size = Length of the Value data
/// Key = Key data
/// Value = Value data
/// Timestamp = Timestamp of the operation in microseconds
pub struct WALEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

/// The WALIterator starts at the beginning of a WAL file and iterates over
/// each entry. This aids in reconstruction of MemTable in event of restart.
///
/// Leveraging Rust's standard library `BufReader` allows for us to focus on
/// deserializing the entries. As an added bonus in the case of efficiency,
/// it reduces the amount of disk ops by reading files in 8KB chunks.
pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    pub fn new(path: PathBuf) -> io::Result<WALIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WALIterator { reader })
    }
}

// TODO: another improvement here would be Error detection for
//   corrupted WAL Files or partial writes. Error correction would
//   also be a good feature.
impl Iterator for WALIterator {
    type Item = WALEntry;

    /// Iterate over entries in a WAL File.
    fn next(&mut self) -> Option<WALEntry> {
        // get size of key from first 8 bytes
        let mut len_buffer = [0; 8];
        if self.reader.read_exact(&mut len_buffer).is_err() {
            // if the read is an error
            return None;
        }
        let key_len = usize::from_le_bytes(len_buffer);

        // get Tombstone marker from next 1 byte
        let mut bool_buffer = [0; 1];
        if self.reader.read_exact(&mut bool_buffer).is_err() {
            //if reading the Tombstone marker errors
            return None;
        }
        let deleted = bool_buffer[0] != 0;

        // get key from n bytes where n = key_length
        let mut key = vec![0; key_len];

        // default value to none
        let mut value = None;
        if deleted {
            if self.reader.read_exact(&mut key).is_err() {
                // if we cannot read the key
                return None;
            } // else fall through to final return
        } else {
            // try and read the value size from next 8 bytes
            if self.reader.read_exact(&mut len_buffer).is_err() {
                // if fail
                return None;
            }
            let value_len = usize::from_le_bytes(len_buffer);
            if self.reader.read_exact(&mut key).is_err() {
                // if we cannot read the key
                return None;
            }
            // get the value from next n bytes where n is value_len
            let mut value_buf = vec![0; value_len];
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            }
            value = Some(value_buf);
        }

        // get timestamp from next 16 bytes
        let mut timestamp_buffer = [0; 16];
        if self.reader.read_exact(&mut timestamp_buffer).is_err() {
            // if we cannot read the timestamp
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);

        Some(WALEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}

/// Write Ahead Log(WAL)
///
/// Append-only file that holds all of the operations performed on the
/// `MemTable`. In the event of a crash or restart, the `MemTable` can be
/// fully-recovered via a read from the WAL.
pub struct WAL {
    path: PathBuf,
    file: BufWriter<File>,
}

impl WAL {
    /// Create a new WAL file in given directory.
    /// WAL file name: `timestamp_in_micros.wal`
    pub fn new(dir: &Path) -> io::Result<WAL> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(WAL { path, file })
    }

    /// Instantiate existing WAL file from path.
    pub fn from_path(path: &Path) -> io::Result<WAL> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(WAL {
            path: path.to_owned(),
            file,
        })
    }

    /// Loads all WAL files within a directory, merges data and creates new WAL file,
    /// and creates `MemTable` from entries. This returns a new WAL file and new `MemTable`.
    ///
    /// The merge of the WAL files happens by file date.
    pub fn load_from_dir_and_recover_mem(dir: &Path) -> io::Result<(WAL, MemTable)> {
        let mut wal_files = files_with_ext(dir, "wal");
        // sort and merge by file date
        wal_files.sort();
        
        let mut new_mem_table = MemTable::new();
        let mut new_wal = WAL::new(dir)?;
        for wal_file in wal_files.iter() {
            if let Ok(wal) = WAL::from_path(wal_file) {
                for entry in wal.into_iter() {
                    if entry.deleted {
                        new_mem_table.delete(entry.key.as_slice(), entry.timestamp);
                        new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
                    } else {
                        new_mem_table.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().unwrap().as_slice(),
                            entry.timestamp,
                        );
                        new_wal.set(
                            entry.key.as_slice(),
                            entry.value.unwrap().as_slice(),
                            entry.timestamp,
                        )?;
                    }
                }
            }
        }
        // flush after bulk operations
        new_wal.flush().unwrap();
        // remove the old wal files as new one has been created
        wal_files.into_iter().for_each(|f| remove_file(f).unwrap());

        Ok((new_wal, new_mem_table))
    }

    /// Appends a set operation to WAL.
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?; // Note: deleted is false for set
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    /// Appends a delete operation to WAL.
    ///
    /// Achieved through inserting a Tombstone.
    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?; //Note: deleted marked as true
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    /// Flushes WAL to disk.
    ///
    /// Allowing for the applying of bulk operations and then flushing
    /// the result to disk will improve performance substantially.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl IntoIterator for WAL {
    type Item = WALEntry;
    type IntoIter = WALIterator;

    fn into_iter(self) -> WALIterator {
        WALIterator::new(self.path).unwrap()
    }
}

// TODO: good candidate for utils package if needed elsewhere.
/// Gets all files with a given extension in a given directory.
fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for file in read_dir(dir).unwrap() {
        let path = file.unwrap().path();
        if path.extension().unwrap() == ext {
            files.push(path);
        }
    }

    files
}

#[cfg(test)]
mod tests {
    use crate::wal::WAL;
    use std::fs::{create_dir, File, metadata, OpenOptions, remove_dir_all};
    use std::io::{BufReader, Read};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use rand::Rng;
    use rstest::rstest;

    //TODO: these tests need redone because if they fail, the test file is not deleted

    fn assert_entry(
        reader: &mut BufReader<File>,
        key: &[u8],
        value: Option<&[u8; 9]>,
        timestamp: u128,
        deleted: bool,
    ) {
        let mut len_buffer = [0; 8];
        reader.read_exact(&mut len_buffer).unwrap();
        let file_key_len = usize::from_le_bytes(len_buffer);
        assert_eq!(file_key_len, key.len());

        let mut bool_buffer = [0; 1];
        reader.read_exact(&mut bool_buffer).unwrap();
        let file_deleted = bool_buffer[0] != 0;
        assert_eq!(file_deleted, deleted);

        if deleted {
            let mut file_key = vec![0; file_key_len];
            reader.read_exact(&mut file_key).unwrap();
            assert_eq!(file_key, key);
        } else {
            reader.read_exact(&mut len_buffer).unwrap();
            let file_value_len = usize::from_le_bytes(len_buffer);
            assert_eq!(file_value_len, value.unwrap().len());
            let mut file_key = vec![0; file_key_len];
            reader.read_exact(&mut file_key).unwrap();
            assert_eq!(file_key, key);
            let mut file_value = vec![0; file_value_len];
            reader.read_exact(&mut file_value).unwrap();
            assert_eq!(file_value, value.unwrap());
        }

        let mut timestamp_buffer = [0; 16];
        reader.read_exact(&mut timestamp_buffer).unwrap();
        let file_timestamp = u128::from_le_bytes(timestamp_buffer);
        assert_eq!(file_timestamp, timestamp);
    }

// TODO: how could I make my cases accept a vec of any length?
    #[rstest]
    #[case(vec![(b"TestKey", Some(b"TestValue"))])]
    #[case(vec![(b"TestKey", Some(b"TestValue")), (b"TestYek", Some(b"TestEulaV"))])]
    fn test_wal_write(#[case] entries: Vec<(&[u8; 7], Option<&[u8; 9]>)>) {
        // create a random made directory
        let mut rng = rand::thread_rng();
        let rand_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&rand_dir).unwrap();

        // test
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let mut wal = WAL::new(&rand_dir).unwrap();

        for e in entries.iter() {
            wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
        }
        wal.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for e in entries.iter() {
            assert_entry(&mut reader, e.0, e.1, timestamp, false);
        }


        // clean up random made directory
        remove_dir_all(&rand_dir).unwrap();
    }

    #[test]
    fn test_wal_write_delete() {
        // create a random made directory
        let mut rng = rand::thread_rng();
        let rand_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&rand_dir).unwrap();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let entries: Vec<(&[u8], Option<&[u8; 9]>)> = vec![
            (b"Key", Some(b"NineChars")),
            (b"Key1", Some(b"NineChars")),
            (b"Key2", Some(b"NineChars")),
        ];

        let mut wal = WAL::new(&rand_dir).unwrap();

        for e in entries.iter() {
            wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
        }
        for e in entries.iter() {
            wal.delete(e.0, timestamp).unwrap();
        }

        wal.flush().unwrap();

        let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for e in entries.iter() {
            assert_entry(&mut reader, e.0, e.1, timestamp, false);
        }
        for e in entries.iter() {
            assert_entry(&mut reader, e.0, None, timestamp, true);
        }

        remove_dir_all(&rand_dir).unwrap();
    }

    #[test]
    fn test_wal_read_none() {
        // create a random made directory
        let mut rng = rand::thread_rng();
        let rand_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&rand_dir).unwrap();

        let (new_wal, new_memtable) = WAL::load_from_dir_and_recover_mem(&rand_dir).unwrap();
        assert_eq!(new_memtable.len(), 0);

        let m = metadata(new_wal.path).unwrap();
        assert_eq!(m.len(), 0);

        remove_dir_all(&rand_dir).unwrap();
    }

    #[test]
    fn test_wal_read_one_file() {
        // create a random made directory
        let mut rng = rand::thread_rng();
        let rand_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&rand_dir).unwrap();

        //test setup
        let entries: Vec<(&[u8], Option<&[u8; 9]>)> = vec![
            (b"Key", Some(b"NineChars")),
            (b"Key1", Some(b"NineChars")),
            (b"Key2", Some(b"NineChars")),
        ];

        let mut wal = WAL::new(&rand_dir).unwrap();

        for (i, e) in entries.iter().enumerate() {
            wal.set(e.0, e.1.unwrap(), i as u128).unwrap();
        }
        wal.flush().unwrap();

        //test
        let (new_wal, new_memtable) = WAL::load_from_dir_and_recover_mem(&rand_dir).unwrap();

        let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for (i, e) in entries.iter().enumerate() {
            assert_entry(&mut reader, e.0, e.1, i as u128, false);

            let mem_entry = new_memtable.get(e.0).unwrap();
            assert_eq!(mem_entry.key, e.0);
            assert_eq!(mem_entry.value.as_ref().unwrap(), e.1.unwrap());
            assert_eq!(mem_entry.timestamp, i as u128);
        }

        remove_dir_all(&rand_dir).unwrap();
    }

    #[test]
    fn test_wal_read_multiple_files() {
        let mut rng = rand::thread_rng();
        let rand_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir(&rand_dir).unwrap();

        // NOTE: the trick in this test is utilizing the index
        //  in the iterators as timestamp for merge testing

        // test setup
        let entries_1: Vec<(&[u8], Option<&[u8; 9]>)> = vec![
            (b"Key", Some(b"NineChars")),
            (b"Key1", Some(b"NineChars")),
            (b"Key2", Some(b"NineChars")),
        ];
        let mut wal_1 = WAL::new(&rand_dir).unwrap();
        for (i, e) in entries_1.iter().enumerate() {
            wal_1.set(e.0, e.1.unwrap(), i as u128).unwrap();
        }
        wal_1.flush().unwrap();

        let entries_2: Vec<(&[u8], Option<&[u8; 9]>)> = vec![
            (b"Key3", Some(b"NineChars")),
            (b"Key4", Some(b"NineChars")),
            (b"Key5", Some(b"NineChars")),
        ];
        let mut wal_2 = WAL::new(&rand_dir).unwrap();
        for (i, e) in entries_2.iter().enumerate() {
            wal_2.set(e.0, e.1.unwrap(), (i + 3) as u128).unwrap();
        }
        wal_2.flush().unwrap();

        // test
        let (new_wal, new_mem_table) = WAL::load_from_dir_and_recover_mem(&rand_dir).unwrap();

        let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for (i, e) in entries_1.iter().enumerate() {
            assert_entry(&mut reader, e.0, e.1, i as u128, false);

            let mem_entry = new_mem_table.get(e.0).unwrap();
            if i != 2 {
                assert_eq!(mem_entry.key, e.0);
                assert_eq!(mem_entry.value.as_ref().unwrap().as_slice(), e.1.unwrap());
                assert_eq!(mem_entry.timestamp, i as u128);
            } else {
                assert_eq!(mem_entry.key, e.0);
                assert_ne!(mem_entry.value.as_ref().unwrap().as_slice(), e.1.unwrap());
                assert_ne!(mem_entry.timestamp, i as u128);
            }
        }
        for (i, e) in entries_2.iter().enumerate() {
            assert_entry(&mut reader, e.0, e.1, (i + 3) as u128, false);

            let mem_e = new_mem_table.get(e.0).unwrap();
            assert_eq!(mem_e.key, e.0);
            assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
            assert_eq!(mem_e.timestamp, (i + 3) as u128);
        }

        remove_dir_all(&rand_dir).unwrap();
    }
}
