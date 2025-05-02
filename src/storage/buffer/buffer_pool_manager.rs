use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, MutexGuard};
use std::sync::atomic::{AtomicU32, Ordering};
use std::cell::Cell;
use std::ops::{Deref, DerefMut};
use crate::storage::disk::{DiskScheduler, PageId};
use crate::storage::page::Page;
use super::error::BufferPoolError;

/// Represents a frame in the buffer pool
pub(crate) struct Frame {
    /// The page stored in this frame
    page: Page,
    /// The ID of the page stored in this frame (None if frame is empty)
    page_id: Option<PageId>,
    /// Whether the page has been modified since it was read from disk
    is_dirty: bool,
    /// Number of pins on this frame (cannot be evicted if > 0)
    pin_count: AtomicU32,
    /// Usage counter for clock replacement policy
    usage_count: u8,
}

impl Frame {
    fn new() -> Self {
        Self {
            page: Page::new(),
            page_id: None,
            is_dirty: false,
            pin_count: AtomicU32::new(0),
            usage_count: 0, // Start with 0 usage
        }
    }

    // Helper to check if frame is pinned
    fn is_pinned(&self) -> bool {
        self.pin_count.load(Ordering::Acquire) > 0
    }
}

/// A guard object that ensures a page is unpinned when it goes out of scope.
/// It also provides safe access to the page's content.
pub struct PageGuard<'bpm> {
    bpm: &'bpm BufferPoolManager,
    page_id: PageId,
    frame_id: usize,
    // Keep track if the guard requested the page be marked dirty.
    is_dirty_on_drop: Cell<bool>,
}

impl<'bpm> PageGuard<'bpm> {
    /// Creates a new PageGuard. Called internally by BufferPoolManager.
    fn new(bpm: &'bpm BufferPoolManager, page_id: PageId, frame_id: usize) -> Self {
        Self {
            bpm,
            page_id,
            frame_id,
            is_dirty_on_drop: Cell::new(false),
        }
    }

    /// Get the PageId of the guarded page.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Mark the page as dirty. It will be marked dirty in the frame
    /// metadata when this guard is dropped.
    pub fn mark_dirty(&self) {
        self.is_dirty_on_drop.set(true);
    }

    /// Acquire a read lock on the frame's content (page data).
    /// Note: This locks the Frame's RwLock internally.
    pub fn read(&self) -> Result<PageReadGuard<'_>, BufferPoolError> { // Return PageReadGuard
        let frame_lock = self.bpm.frames[self.frame_id]
            .read()
            .map_err(|e| BufferPoolError::LockError(format!("Failed to read-lock frame {}: {}", self.frame_id, e)))?;

        // Wrap the frame guard in our custom guard type
        Ok(PageReadGuard { frame_guard: frame_lock })
    }

    /// Acquire a write lock on the frame's content (page data).
    /// Automatically marks the page as dirty when this guard is dropped.
    /// Note: This locks the Frame's RwLock internally.
    pub fn write(&self) -> Result<PageWriteGuard<'_>, BufferPoolError> { // Return PageWriteGuard
        self.mark_dirty(); // Writing implies dirtying

        let frame_lock = self.bpm.frames[self.frame_id]
            .write()
            .map_err(|e| BufferPoolError::LockError(format!("Failed to write-lock frame {}: {}", self.frame_id, e)))?;

        // Wrap the frame guard in our custom guard type
        Ok(PageWriteGuard { frame_guard: frame_lock })
    }

    // Internal access for testing
    // pub(crate) fn frame_id(&self) -> usize {
    //     self.frame_id
    // }
}

impl<'bpm> Drop for PageGuard<'bpm> {
    fn drop(&mut self) {
        // Call the internal unpin logic.
        // We ignore potential errors here as drop cannot return Result.
        // In a production system, logging the error would be appropriate.
        if let Err(e) = self.bpm.unpin_internal(self.frame_id, self.is_dirty_on_drop.get()) {
             eprintln!(
                 "ERROR: Failed to unpin page {:?} in frame {}: {:?}",
                 self.page_id, self.frame_id, e
             );
        }
    }
}

/// A read guard for a page's content within a locked frame.
pub struct PageReadGuard<'a> {
    // This guard holds the lock on the Frame
    frame_guard: RwLockReadGuard<'a, Frame>,
}

impl<'a> Deref for PageReadGuard<'a> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        // Dereference to the Page inside the locked Frame
        &self.frame_guard.page
    }
}

/// A write guard for a page's content within a locked frame.
pub struct PageWriteGuard<'a> {
    // This guard holds the lock on the Frame
    frame_guard: RwLockWriteGuard<'a, Frame>,
}

impl<'a> Deref for PageWriteGuard<'a> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.frame_guard.page
    }
}

impl<'a> DerefMut for PageWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Dereference mutably to the Page inside the locked Frame
        &mut self.frame_guard.page
    }
}

/// Buffer pool manager that caches pages in memory
pub struct BufferPoolManager {
    /// Fixed array of frames, each protected by its own RwLock
    frames: Vec<RwLock<Frame>>,
    /// Maps page IDs to frame indices, protected by a Mutex
    page_table: Mutex<HashMap<PageId, usize>>,
    /// Disk scheduler for reading/writing pages
    disk_scheduler: Arc<DiskScheduler>,
    /// Clock hand for replacement policy, protected by a Mutex
    clock_hand: Mutex<usize>,
    /// Size of the buffer pool
    pool_size: usize,
}

impl BufferPoolManager {
    /// Create a new buffer pool manager with the specified number of frames
    pub fn new(disk_scheduler: Arc<DiskScheduler>, pool_size: usize) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            frames.push(RwLock::new(Frame::new()));
        }

        Self {
            frames,
            page_table: Mutex::new(HashMap::new()),
            disk_scheduler,
            clock_hand: Mutex::new(0),
            pool_size,
        }
    }

    /// Fetch a page, potentially reading it from disk if not in the buffer pool.
    /// Handles internal retries for transient contention.
    /// Returns a PageGuard which handles pinning and unpinning.
    pub fn fetch_page(&self, page_id: PageId) -> Result<PageGuard<'_>, BufferPoolError> {
        
        // --- Phase 1: Check cache and handle fast path (outside the main loop) ---
        {
            let page_table = self.page_table.lock()?;
            if let Some(&frame_id) = page_table.get(&page_id) {
                // Page found in pool - Fast path
                let mut frame_lock = self.frames[frame_id].write()?; // Lock the frame
                frame_lock.pin_count.fetch_add(1, Ordering::SeqCst);
                frame_lock.usage_count = frame_lock.usage_count.saturating_add(1).max(1); // Ensure usage is at least 1
                // Drop locks implicitly
                return Ok(PageGuard::new(self, page_id, frame_id));
            }
            // Page not in pool, release page_table lock
        }

        // --- Phase 2: Page not in cache - Slow path with internal retries ---
        loop { // Internal retry loop
            // Acquire page table lock
            let mut page_table = self.page_table.lock()?;

            // Re-check cache *after* acquiring lock (handles race condition)
            // In case page was fetched by another thread while we waited for the lock.
            if let Some(&frame_id) = page_table.get(&page_id) {
                // Handle like the fast path.
                let mut frame_lock = self.frames[frame_id].write()?;
                frame_lock.pin_count.fetch_add(1, Ordering::SeqCst);
                frame_lock.usage_count = frame_lock.usage_count.saturating_add(1).max(1);
                // Drop locks implicitly
                return Ok(PageGuard::new(self, page_id, frame_id));
            }

            // Find a victim frame ID (still holding page_table lock)
            let victim_frame_id = match self.find_victim_frame_id(&page_table) {
                Ok(id) => id,
                Err(BufferPoolError::NoAvailableFrames) => {
                    // Non-recoverable error
                    return Err(BufferPoolError::NoAvailableFrames);
                }
                Err(e) => {
                    // Propagate unexpected errors from find_victim
                    return Err(e);
                }
            };

            // Attempt to lock the chosen victim frame (still holding page_table lock)
            let mut victim_frame_lock = match self.frames[victim_frame_id].try_write() {
                Ok(lock) => lock,
                Err(_) => {
                    // Failed to lock the victim frame (likely held by another thread, e.g., flushing).
                    // Release page_table lock and retry the entire operation.
                    drop(page_table);
                    std::thread::yield_now(); // Yield to prevent busy-waiting
                    continue; // Restart the loop
                }
            };

            // Final check: Ensure the victim frame is still unpinned after locking it.
            if victim_frame_lock.is_pinned() {
                // The frame became pinned between find_victim_frame_id check and acquiring the lock.
                // Release locks and retry.
                drop(victim_frame_lock);
                drop(page_table);
                std::thread::yield_now(); // Yield
                continue; // Restart the loop
            }

            //  --- State is consistent: 
            // (1) Page not in table, 
            // (2) victim frame found and locked, 
            // (3) victim is unpinned ---
            // We hold both page_table lock and victim frame lock ---

            // Eviction logic (if necessary)
            let old_page_id = victim_frame_lock.page_id;
            // Store potential eviction info (PageId and data)
            let mut evicted_page_info: Option<(PageId, Vec<u8>)> = None;

            if let Some(old_pid) = old_page_id {
                if victim_frame_lock.is_dirty {
                    // Prepare data for writing *after* releasing page_table lock
                    evicted_page_info = Some((old_pid, victim_frame_lock.page.raw.to_vec()));
                }
                // Remove old page from page table
                let removed = page_table.remove(&old_pid);
                assert!(removed.is_some(), "Page table should contain the old page ID");
            }

            // Update metadata for the new page
            victim_frame_lock.page_id = Some(page_id);
            victim_frame_lock.is_dirty = false; // Page is clean initially from disk
            victim_frame_lock.pin_count.store(1, Ordering::SeqCst); // Pin the frame
            victim_frame_lock.usage_count = 1; // Initial usage count for clock

            // Insert new page into page table
            page_table.insert(page_id, victim_frame_id);

            // --- CRITICAL: Release page_table lock *before* disk I/O ---
            drop(page_table);

            // --- Perform I/O (holding only victim frame lock) ---

            // Write evicted page to disk if necessary
            // Capture the result, including the PageId if an error occurs
            let write_evicted_result = if let Some((evicted_pid, ref data)) = evicted_page_info {
                 self.disk_scheduler.write_page(evicted_pid, data.clone()) // Clone data for write
                     .map_err(|io_err| (evicted_pid, io_err)) // Pair PageId with error
            } else {
                 Ok(()) // No eviction needed or page wasn't dirty
            };

            // Handle potential write error
            if let Err((evicted_pid_for_err, io_err)) = write_evicted_result {
                 // release frame lock and return the error
                 // TODO: More robust recovery might be needed
                 victim_frame_lock.pin_count.store(0, Ordering::SeqCst); // Attempt to unpin
                 victim_frame_lock.page_id = old_page_id; // Try to restore old ID? Risky.
                 // Use the captured PageId and the correct error variable io_err
                 eprintln!("ERROR: Failed to write evicted page {}: {}", evicted_pid_for_err, io_err);
                 // Best to just signal failure. Wrap the io::Error.
                 // Ensure your BufferPoolError::IoError variant takes io::Error
                 return Err(BufferPoolError::IoError(io_err));
            }

            // Reset dirty flag *after* successful write of evicted page
            // Check if eviction info existed and the write was successful
            if evicted_page_info.is_some() && write_evicted_result.is_ok() {
                victim_frame_lock.is_dirty = false; // Reset dirty flag related to the *old* page
            }


            // Read the requested page from disk into the frame's buffer
            match self.disk_scheduler.read_page(page_id) {
                Ok(buf) => {
                    victim_frame_lock.page.raw.copy_from_slice(&buf);
                    victim_frame_lock.is_dirty = false; // Ensure clean after read

                    // Success! Drop frame lock implicitly and return the guard.
                    // The loop is exited via this return.
                    return Ok(PageGuard::new(self, page_id, victim_frame_id));
                }
                Err(e) => {
                    // Read failed. Revert state changes made under the page_table lock.
                    eprintln!("ERROR: Failed to read page {} into frame {}: {}. Reverting state.", page_id, victim_frame_id, e);

                    // Reset frame state
                    victim_frame_lock.page_id = old_page_id; // Restore old page id
                    victim_frame_lock.is_dirty = false; // Mark as clean, content is invalid
                    victim_frame_lock.pin_count.store(0, Ordering::SeqCst); // Unpin
                    victim_frame_lock.usage_count = 0;

                    // Re-acquire page table lock to remove the entry we added
                    // This adds complexity but is necessary for consistency.
                    {
                        let mut pt_lock = self.page_table.lock()?;
                        pt_lock.remove(&page_id);
                    } // pt_lock released

                    // Drop frame lock implicitly.
                    // Return the I/O error. The loop is exited via this return.
                    return Err(BufferPoolError::IoError(e));
                }
            }
            // Should be unreachable due to returns in match arms
        } // End internal loop
    }


    pub fn new_page(&self) -> Result<PageGuard<'_>, BufferPoolError> {
        // --- Phase 1: Allocate Page ID ---
        // Do this first, as it involves I/O and might fail.
        let page_id = self.disk_scheduler.allocate_page()
            .map_err(BufferPoolError::IoError)?;

        // --- Phase 2: Find Victim Frame and setup (with internal retries) ---
        loop { // Internal retry loop
            // Acquire page table lock
            let mut page_table = self.page_table.lock()?;

            // Find a victim frame ID (still holding page_table lock)
            let victim_frame_id = match self.find_victim_frame_id(&page_table) {
                 Ok(id) => id,
                 Err(BufferPoolError::NoAvailableFrames) => {
                     // Non-recoverable error. Deallocate the page ID we allocated?
                     // TODO: Add disk_scheduler.deallocate_page(page_id) if possible.
                     eprintln!("WARN: Failed to find frame for new page {}, page ID might be leaked on disk.", page_id);
                     return Err(BufferPoolError::NoAvailableFrames);
                 }
                 Err(e) => return Err(e), // Propagate unexpected errors
            };

            // Attempt to lock the chosen victim frame (still holding page_table lock)
            let mut victim_frame_lock = match self.frames[victim_frame_id].try_write() {
                 Ok(lock) => lock,
                 Err(_) => {
                     // Failed to lock victim. Release page_table lock and retry.
                     drop(page_table);
                     std::thread::yield_now();
                     continue; // Restart the loop
                 }
            };

            // Final check: Ensure the victim frame is *still* unpinned *after* locking it.
            if victim_frame_lock.is_pinned() {
                 // Victim became pinned. Release locks and retry.
                 drop(victim_frame_lock);
                 drop(page_table);
                 std::thread::yield_now();
                 continue; // Restart the loop
            }

            // --- State is consistent: Victim frame found and locked, victim is unpinned ---
            // --- We hold both page_table lock and victim frame lock ---

            // Eviction logic (if necessary)
            let old_page_id = victim_frame_lock.page_id;
            let mut evicted_page_data: Option<(PageId, Vec<u8>)> = None;

            if let Some(old_pid) = old_page_id {
                 if victim_frame_lock.is_dirty {
                     evicted_page_data = Some((old_pid, victim_frame_lock.page.raw.to_vec()));
                 }
                 let removed = page_table.remove(&old_pid);
                 assert!(removed.is_some(), "Page table should contain the old page ID for eviction");
            }

            // Update metadata for the new page
            victim_frame_lock.page.init(0); // Zero-fill the page content
            victim_frame_lock.page_id = Some(page_id);
            victim_frame_lock.is_dirty = true; // New pages start dirty (need flushing eventually)
            victim_frame_lock.pin_count.store(1, Ordering::SeqCst); // Pin the frame
            victim_frame_lock.usage_count = 1; // Initial usage count

            // Insert new page into page table
            page_table.insert(page_id, victim_frame_id);

            // --- CRITICAL: Release page_table lock *before* potential disk write ---
            drop(page_table);

            // --- Perform I/O (holding only victim frame lock) ---

                        let write_evicted_result = if let Some((evicted_pid, data)) = evicted_page_data {
                 self.disk_scheduler.write_page(evicted_pid, data)
                     .map_err(|e| (evicted_pid, e)) // Keep PageId with error for logging
            } else {
                 Ok(())
            };

            // Log error if writing the evicted page failed, but proceed.
            // The new page is already set up in the frame.
            if let Err((evicted_pid_for_err, io_err)) = write_evicted_result {
                eprintln!(
                    "ERROR: Failed to write evicted page {} during new_page operation for page {}: {}",
                    evicted_pid_for_err, page_id, io_err
                );
                // We don't return an error here because the primary operation (creating the *new* page
                // in the buffer pool) has succeeded conceptually. The frame holds the new page_id
                // and is marked dirty. The failure is logged.
            }


            // --- Success: New page is set up in the frame ---
            // Drop frame lock implicitly.
            // Return the guard for the newly created page. Loop is exited via this return.
            return Ok(PageGuard::new(self, page_id, victim_frame_id));

            // Should be unreachable
        } // End internal loop
    }
    
    
    /// Unpin a page identified by its frame ID, optionally marking it as dirty.
    /// This is called internally by PageGuard's Drop implementation.
    /// Lock order: Frame lock only.
    fn unpin_internal(&self, frame_id: usize, is_dirty: bool) -> Result<(), BufferPoolError> {
        let mut frame_guard = self.frames[frame_id].write()?; // Acquire write lock on the frame

        if is_dirty {
            frame_guard.is_dirty = true;
        }

        let current_pins = frame_guard.pin_count.load(Ordering::Acquire);
        if current_pins == 0 {
            // This should ideally not happen if pin counts are managed correctly by guards
             eprintln!("WARN: Attempted to unpin frame {} which already has pin count zero.", frame_id);
             // Return Ok anyway, as the goal (pin count >= 0) is met.
             return Ok(());
             // Or return Err(BufferPoolError::CannotUnpin)? For now, allow.
        }

        // Decrement pin count
        frame_guard.pin_count.fetch_sub(1, Ordering::Release);

        Ok(())
    }

    /// Find an unpinned frame using the Clock replacement algorithm.
    /// This function handles locking the clock_hand mutex.
    /// It attempts to briefly lock frames to check their pin status and usage count.
    /// It does NOT return a lock on the frame, only its ID.
    /// The caller is responsible for locking the returned frame_id and potentially
    /// re-validating its state.
    /// Requires the page_table lock to be held by the caller if needed for context,
    /// but doesn't use it directly here.
    /// for the repo simpilicity reasons we just write it in file
    fn find_victim_frame_id(
        &self,
        _page_table_guard: &MutexGuard<HashMap<PageId, usize>>, // Pass guard to prove lock is held
    ) -> Result<usize, BufferPoolError> {
        let mut clock_hand_guard = self.clock_hand.lock()?;
        let start_pos = *clock_hand_guard;

        // Iterate twice around the clock maximum
        for i in 0..(self.pool_size * 2) {
            let current_frame_id = (start_pos + i) % self.pool_size;
            let frame = &self.frames[current_frame_id];

            // Attempt to acquire write lock briefly to check/modify state
            // Use try_write to avoid blocking if another thread holds the lock long.
            if let Ok(mut frame_guard) = frame.try_write() {
                if !frame_guard.is_pinned() {
                    // Found an unpinned frame
                    if frame_guard.usage_count > 0 {
                        // Decrement usage count (give it a second chance)
                        frame_guard.usage_count -= 1;
                        // Drop the temporary lock and continue searching
                        drop(frame_guard);
                        continue; // Check next frame
                    } else {
                        // Usage count is 0, this is our victim.
                        // Update the clock hand position for the next search.
                        *clock_hand_guard = (current_frame_id + 1) % self.pool_size;
                        // Return the frame_id. Lock is released by drop(frame_guard).
                        return Ok(current_frame_id);
                    }
                }
                // Frame is pinned, release lock and continue search
                // Lock released by drop(frame_guard)
            } else {
                // Failed to acquire try_write lock, frame is likely held by another
                // thread (e.g., doing I/O). Skip it for this round.
                continue;
            }
        }

        // No suitable frame found after two full passes
        // This likely means all frames are pinned or constantly locked.
        Err(BufferPoolError::NoAvailableFrames)
    }

    // --- Helper methods (optional) ---

    /// Get the number of frames in the buffer pool.
    pub fn size(&self) -> usize {
        self.pool_size
    }

    // /// Get a snapshot of the page table (for debugging/testing)
    // pub fn page_table_snapshot(&self) -> Result<HashMap<PageId, usize>, BufferPoolError> {
    //     let lock = self.page_table.lock()?;
    //     Ok(lock.clone())
    // }
}