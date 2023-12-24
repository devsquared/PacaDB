use std::path::PathBuf;

#[derive(Debug)]
pub struct DatabaseEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u128,
}

/// The basic entry in PacaDB defined by a key-value pair both consisting of simple bytes.
impl DatabaseEntry {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn timestamp(&self) -> u128 {
        self.timestamp
    }
}

//TODO: not sure what we will do here but stubbing for now
#[derive(Debug)]
pub struct RecordIterator {}

/// Database is the top level struct that holds a connection and all data within a PacaDB instance.
#[derive(Debug)]
pub struct Database {
    dir: PathBuf
}

impl Database {
    /// Returns either a record retrieved from the database or None.
    ///
    /// First, we check to see if the record is in the MemTable. If it is not, we then check
    /// the SSTables from smallest to largest - returning the record found at the lowest level.
    pub fn get(&self, key: &[u8]) -> Option<DatabaseEntry> {
        todo!()
    }

    // TODO: what do we want from the returns here? Result<usize, usize> give us flexibility to return number codes back
    //  But do we want to return more or less?

    /// Creates a key-value pair in the database returning a success or failure.
    ///
    /// This creates the record in the MemTable and WAL.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> bool {
        todo!()
    }

    /// Deletes a record from the database returning a success or failure.
    ///
    /// This operates by storing a Tombstone record in the MemTable and WAL. This method is very similar
    /// to the set method. This is done - in short - because cleaning out a record would be much, much slower.
    pub fn delete(&mut self, key: &[u8]) -> bool {
        todo!()
    }


    /// Retrieves a `RecordIterator` to iterate over a set of records returned between a high- and low-key.
    ///
    /// Scan is quite an expensive operation and is only recommended for small ranges and low quantity sets of data.
    pub fn scan(&self, high_key: &[u8], low_key: &[u8]) -> RecordIterator {
        todo!()
    }
}