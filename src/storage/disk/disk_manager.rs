use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::storage::page::BLCKSZ;

/// Unique identifier for a page within the database file
pub type PageId = u32;

/// Manages reading and writing pages to a database file with thread-safety
pub struct DiskManager {
    /// The database file protected by a mutex for thread-safe access
    db_file: Mutex<File>,
    /// Next page ID to allocate using atomic operations
    next_page_id: AtomicU32,
}

impl DiskManager {
    /// Opens or creates a database file with thread-safety
    pub fn new<P: AsRef<Path>>(db_path: P) -> io::Result<Self> {
        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path)?;
        
        // Calculate next_page_id based on file size
        let file_size = db_file.metadata()?.len();
        let next_page_id = (file_size / BLCKSZ as u64) as PageId;
        
        Ok(Self { 
            db_file: Mutex::new(db_file),
            next_page_id: AtomicU32::new(next_page_id)
        })
    }
    
    /// Calculates the byte offset for a given page ID
    fn page_offset(page_id: PageId) -> u64 {
        (page_id as u64) * (BLCKSZ as u64)
    }
    
    /// Reads a page from disk into the provided buffer (thread-safe)
    pub fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> io::Result<()> {
        // Validate buffer size
        if buf.len() != BLCKSZ {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Buffer size must match page size"
            ));
        }
        
        // Get current page count using atomic load
        let next_page_id = self.next_page_id.load(Ordering::Acquire);
        if page_id >= next_page_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Page ID out of bounds"
            ));
        }
        
        // Safely access the file for reading
        let offset = Self::page_offset(page_id);
        let mut file = self.db_file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buf)?;
        
        Ok(())
    }
    
    /// Writes a page to disk from the provided buffer (thread-safe)
    pub fn write_page(&self, page_id: PageId, buf: &[u8]) -> io::Result<()> {
        // Validate buffer size
        if buf.len() != BLCKSZ {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Buffer size must match page size"
            ));
        }
        
        // Get current page count using atomic load
        let next_page_id = self.next_page_id.load(Ordering::Acquire);
        if page_id >= next_page_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Page ID out of bounds"
            ));
        }
        
        // Safely access the file for writing
        let offset = Self::page_offset(page_id);
        let mut file = self.db_file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(buf)?;
        
        Ok(())
    }
    
    /// Allocates a new page ID using atomic fetch_add
    pub fn allocate_page(&self) -> PageId {
        // Atomically get the current value and increment
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }
    
    /// Returns the total number of pages (thread-safe)
    pub fn num_pages(&self) -> PageId {
        self.next_page_id.load(Ordering::Acquire)
    }
    
    /// Flushes all changes to disk (thread-safe)
    pub fn sync(&self) -> io::Result<()> {
        self.db_file.lock().unwrap().sync_all()
    }
}