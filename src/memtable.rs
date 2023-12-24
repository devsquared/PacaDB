/// This MemTable implementation leverages a vector of entries
/// rather than a HashMap of entries. Doing so allows for
/// O(Log N) searches and O(N) inserts. We sacrifice constant
/// time lookups and edits in order to drastically reduce the
/// complexity of scan operations.

/// MemTableEntry is the stored data entry in the `MemTable`.
///
/// It holds the key-value pairing as bytes (value supporting optionality for Tombstones),
/// a timestamp, and the deleted flag to support deletions and Tombstone inserts.
pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool, // marks it a "Tombstone" record or not
}

/// MemTable holds a sorted list of the latest written records.
///
/// Writes are also duplicated over to the WAL (Write-Ahead-Ledger)
/// for recovery of the MemTable in the event of a restart.
///
/// MemTables have a max capacity. When reached, it is flushed to disk
/// in the form of an SSTable.
///
/// Entries are stored in a Vector as opposed to a HashMap in order
/// to support scan operations.
pub struct MemTable {
    entries: Vec<MemTableEntry>,
    size: usize, // measured in bytes
}

impl MemTable {
    /// Creates a new empty MemTable.
    pub fn new() -> MemTable {
        MemTable{
            entries: Vec::new(),
            size: 0,
        }
    }

    /// Finds a record in the MemTable by performing Binary Search.
    ///
    /// Returns:
    /// - `[Result::Ok]`: contains the index of the record
    /// - `[Result::Err]`: contains the index to insert the record at
    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries.binary_search_by_key(&key, |e|e.key.as_slice())
    }

    /// Sets a new key-value pair in the MemTable.
    ///
    /// Size of the MemTable will be adjusted to accommodate new entry.
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let entry = MemTableEntry{
            key: key.to_owned(),
            value: Some(value.to_owned()),
            timestamp,
            deleted: false,
        };

        // check if the entry is already in the mem table
        match self.get_index(key) {
            Ok(id_index) => {
                // if record existed, we are going to replace; first, we need to check
                // if the record to be replaced has an empty value or not
                if let Some(old_val) = self.entries[id_index].value.as_ref() {
                    // if there is an old value, we need to check if we need to adjust space
                    if value.len() < old_val.len() { // new value is less than old value, free up difference
                        self.size -= old_val.len() - value.len();
                    } else { // old value is less than new, add space
                        self.size += value.len() - old_val.len();
                    }
                }
                self.entries[id_index] = entry; // replace entry
            }
            Err(id_index) => {
                // if record does not exist, allocate space for it and then place it
                self.size += key.len() + value.len() + 16 + 1; // size for key and value, 16(bytes) for timestamp, 1(byte) for bool
                self.entries.insert(id_index, entry);
            }
        }
    }

    /// Deletes a key-value pair from the MemTable.
    ///
    /// Size will be adjusted to accommodate.
    /// Deletion is achieved via inserting Tombstones.
    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        let entry = MemTableEntry {
            key: key.to_owned(),
            value: None, // in a Tombstone, value is left empty
            timestamp,
            deleted: true, // mark as Tombstone
        };

        match self.get_index(key) {
            Ok(id_index) => {
                // if a value is found, we will replace it after freeing space if possible
                if let Some(value) = self.entries[id_index].value.as_ref() {
                    // if there was a value on the found entry, free up size of value
                    self.size -= value.len();
                }
                self.entries[id_index] = entry; // replace entry
            }
            Err(id_index) => {
                self.size += key.len() + 16 + 1; // allocate key size, 16(bytes) for timestamp, and 1(byte) for bool
                self.entries.insert(id_index, entry);
            }
        }
    }

    /// Get the key-value pair from the MemTable.
    ///
    /// If no record exists, simply return None.
    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(id_index) = self.get_index(key) {
            return Some(&self.entries[id_index]);
        }
        None
    }

    /// Gets the number of records in the MemTable.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Gets all of the records from the MemTable.
    pub fn entries(&self) -> &[MemTableEntry] {
        &self.entries
    }

    /// Gets the total size of the records in the MemTable
    pub fn size(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use crate::memtable::MemTable;
    use rstest::rstest;

    fn prep_test_table() -> MemTable {
        let mut table = MemTable::new();
        table.set(b"Test", b"Testerton", 0); // size = 4 + 9 + 16 + 1 = 30
        table.set(b"Key", b"Value", 10); // size = 3 + 5 + 16 + 1 = 25

        table
    }

    #[rstest]
    #[case(b"ATest", b"Something", 20)]
    fn test_memtable_put_start(#[case] key: &[u8], #[case] value: &[u8], #[case] timestamp: u128) {
        // prep a table
        let mut table = prep_test_table();

        table.set(key, value, timestamp);

        // assert tested entry
        assert_eq!(table.entries[0].key, key);
        assert_eq!(table.entries[0].value.as_ref().unwrap(), value);
        assert_eq!(table.entries[0].timestamp, timestamp);
        assert!(!table.entries[0].deleted); // not deleted

        // assert other contents
        assert_eq!(table.entries[1].key, b"Key");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Value");
        assert_eq!(table.entries[1].timestamp, 10);
        assert!(!table.entries[1].deleted); // not deleted
        assert_eq!(table.entries[2].key, b"Test");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Testerton");
        assert_eq!(table.entries[2].timestamp, 0);
        assert!(!table.entries[2].deleted); // not deleted

        assert_eq!(table.size, 86)
    }

    #[rstest]
    #[case(b"LTest", b"Something", 20)]
    fn test_memtable_put_middle(#[case] key: &[u8], #[case] value: &[u8], #[case] timestamp: u128) {
        // prep a table
        let mut table = prep_test_table();

        table.set(key, value, timestamp);

        // assert tested entry
        assert_eq!(table.entries[1].key, key);
        assert_eq!(table.entries[1].value.as_ref().unwrap(), value);
        assert_eq!(table.entries[1].timestamp, timestamp);
        assert!(!table.entries[1].deleted); // not deleted

        // assert other contents
        assert_eq!(table.entries[0].key, b"Key");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Value");
        assert_eq!(table.entries[0].timestamp, 10);
        assert!(!table.entries[0].deleted); // not deleted
        assert_eq!(table.entries[2].key, b"Test");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Testerton");
        assert_eq!(table.entries[2].timestamp, 0);
        assert!(!table.entries[2].deleted); // not deleted

        assert_eq!(table.size, 86)
    }

    #[rstest]
    #[case(b"ZTest", b"Something", 20)]
    fn test_memtable_put_end(#[case] key: &[u8], #[case] value: &[u8], #[case] timestamp: u128) {
        // prep a table
        let mut table = prep_test_table();

        table.set(key, value, timestamp);

        // assert tested entry
        assert_eq!(table.entries[2].key, key);
        assert_eq!(table.entries[2].value.as_ref().unwrap(), value);
        assert_eq!(table.entries[2].timestamp, timestamp);
        assert!(!table.entries[2].deleted); // not deleted

        // assert other contents
        assert_eq!(table.entries[0].key, b"Key");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Value");
        assert_eq!(table.entries[0].timestamp, 10);
        assert!(!table.entries[0].deleted); // not deleted
        assert_eq!(table.entries[1].key, b"Test");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Testerton");
        assert_eq!(table.entries[1].timestamp, 0);
        assert!(!table.entries[1].deleted); // not deleted

        assert_eq!(table.size, 86)
    }

    #[test]
    fn test_memtable_put_overwrite() {
        // prep a table
        let mut table = prep_test_table();

        // overwrite
        table.set(b"Key", b"Value2", 10);

        // assert overwritten key
        assert_eq!(table.entries[0].key, b"Key");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Value2"); // +1 to size
        assert_eq!(table.entries[0].timestamp, 10);
        assert!(!table.entries[0].deleted); // not deleted

        // assert other contents
        assert_eq!(table.entries[1].key, b"Test");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Testerton");
        assert_eq!(table.entries[1].timestamp, 0);
        assert!(!table.entries[1].deleted); // not deleted

        assert_eq!(table.size, 56);
    }

    #[test]
    fn test_memtable_get_exists() {
        // prep a table
        let table = prep_test_table();

        let actual_entry = table.get(b"Key").unwrap();

        assert_eq!(actual_entry.key, b"Key");
        assert_eq!(actual_entry.value.as_ref().unwrap(), b"Value");
        assert_eq!(actual_entry.timestamp, 10);
        assert!(!actual_entry.deleted);
    }

    #[test]
    fn test_memtable_get_not_exists() {
        // prep a table
        let table = prep_test_table();

        let none_entry = table.get(b"random");
        assert!(none_entry.is_none());
    }

    #[test]
    fn test_memtable_delete_exists() {
        // prep a table
        let mut table = prep_test_table();

        table.delete(b"Key", 20);

        // assert that result contains expected Tombstone entry
        let tombstone_entry = table.get(b"Key").unwrap();
        assert_eq!(tombstone_entry.key, b"Key");
        assert_eq!(tombstone_entry.value, None); // size -5
        assert_eq!(tombstone_entry.timestamp, 20);
        assert!(tombstone_entry.deleted);
        // also assert the actual value in the entries is correct
        assert_eq!(table.entries[0].key, b"Key");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp, 20);
        assert!(table.entries[0].deleted);

        //assert other contents
        assert_eq!(table.entries[1].key, b"Test");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Testerton");
        assert_eq!(table.entries[1].timestamp, 0);
        assert!(!table.entries[1].deleted);

        assert_eq!(table.size, 50);
    }

    #[test]
    fn test_memtable_delete_empty() {
        // prep a table
        let mut table = prep_test_table();

        table.delete(b"ANonKey", 0);

        // assert that result contains expected Tombstone entry
        let tombstone_entry = table.get(b"ANonKey").unwrap();
        assert_eq!(tombstone_entry.key, b"ANonKey");
        assert_eq!(tombstone_entry.value, None); // size += 7 + 0 + 16 + 1
        assert_eq!(tombstone_entry.timestamp, 0);
        assert!(tombstone_entry.deleted); // is deleted
        // also assert the actual value in the entries is correct
        assert_eq!(table.entries[0].key, b"ANonKey");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp, 0);
        assert!(table.entries[0].deleted); // is deleted

        // assert other contents
        assert_eq!(table.entries[1].key, b"Key");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Value");
        assert_eq!(table.entries[1].timestamp, 10);
        assert!(!table.entries[1].deleted); // not deleted
        assert_eq!(table.entries[2].key, b"Test");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Testerton");
        assert_eq!(table.entries[2].timestamp, 0);
        assert!(!table.entries[2].deleted); // not deleted

        assert_eq!(table.size, 79)
    }
}