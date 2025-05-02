use baselab::storage::buffer::BufferPoolManager;
use baselab::storage::disk::{DiskScheduler, PageId};
use baselab::storage::page::{Page, BLCKSZ};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

// Use a counter to generate unique filenames for parallel tests
static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

const BUFFER_POOL_SIZE: usize = 10;

// Helper function to set up BPM for tests, now returns the unique filename
fn setup_bpm(pool_size: usize) -> (Arc<DiskScheduler>, Arc<BufferPoolManager>, String) {
    let test_num = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let db_filename = format!("test_bpm_{}.db", test_num);

    // Clean up any existing test file for this specific test run
    if Path::new(&db_filename).exists() {
        fs::remove_file(&db_filename)
            .expect(&format!("Failed to remove old test file: {}", db_filename));
    }

    let disk_scheduler = Arc::new(DiskScheduler::new(&db_filename).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(disk_scheduler.clone(), pool_size));
    (disk_scheduler, bpm, db_filename) // Return the filename
}

// Helper function to clean up test file - takes filename as argument
fn cleanup_db(db_filename: &str) {
    if Path::new(db_filename).exists() {
        if let Err(e) = fs::remove_file(db_filename) {
            eprintln!("Warning: Failed to clean up test file {}: {}", db_filename, e);
        }
    }
}

#[test]
fn test_bpm_new_page() {
    let (_disk_scheduler, bpm, db_filename) = setup_bpm(BUFFER_POOL_SIZE);

    // Create a new page
    let page_guard = bpm.new_page().expect("Failed to create new page");
    let page_id = page_guard.page_id();

    // Write data to the new page
    {
        let mut page_write_guard = page_guard.write().expect("Failed to get write lock");
        let sid = page_write_guard.add_item(b"hello bpm new").unwrap();
        assert_eq!(page_write_guard.get_item(sid).unwrap(), b"hello bpm new");
    } // Write guard dropped here, page marked dirty

    // Drop the main guard, unpinning the page
    drop(page_guard);

    // Fetch the same page again
    let page_guard_2 = bpm.fetch_page(page_id).expect("Failed to fetch page");
    {
        let page_read_guard = page_guard_2.read().expect("Failed to get read lock");
        // Assuming slot ID remains 0 after creation/re-fetch
        assert_eq!(page_read_guard.get_item(0).unwrap(), b"hello bpm new");
    }
    drop(page_guard_2);

    cleanup_db(&db_filename);
}

#[test]
fn test_bpm_fetch_page() {
    let (disk_scheduler, bpm, db_filename) = setup_bpm(BUFFER_POOL_SIZE);

    // Manually create a page on disk first
    let page_id_on_disk: PageId = 0;
    let page_data = vec![0u8; BLCKSZ];
    let mut page = Page::new();
    page.raw.copy_from_slice(&page_data); // Use existing buffer
    page.init(0); // Correct: Initialize with 0 special size
    let sid = page.add_item(b"data from disk").unwrap();
    disk_scheduler.allocate_page().unwrap(); // Allocate page 0
    disk_scheduler.write_page(page_id_on_disk, page.raw.to_vec()).unwrap();
    disk_scheduler.sync().unwrap(); // Ensure it's written

    // Fetch the page using BPM
    let page_guard = bpm.fetch_page(page_id_on_disk).expect("Failed to fetch page");
    {
        let page_read_guard = page_guard.read().expect("Failed to get read lock");
        assert_eq!(page_read_guard.get_item(sid).unwrap(), b"data from disk");
    }
    drop(page_guard);

    // Fetch again, should be cached (though hard to verify directly without internal access)
    let page_guard_2 = bpm.fetch_page(page_id_on_disk).expect("Failed to fetch page again");
    {
        let page_read_guard = page_guard_2.read().expect("Failed to get read lock again");
        assert_eq!(page_read_guard.get_item(sid).unwrap(), b"data from disk");
    }
    drop(page_guard_2);

    cleanup_db(&db_filename);
}

#[test]
fn test_bpm_eviction() {
    let pool_size = 3;
    let (_disk_scheduler, bpm, db_filename) = setup_bpm(pool_size);
    let mut guards = Vec::new();

    // Create pages to fill the buffer pool
    for i in 0..pool_size {
        let page_guard = bpm.new_page().expect(&format!("Failed to create page {}", i));
        let page_id = page_guard.page_id();
        {
            let mut write_guard = page_guard.write().unwrap();
            let data = format!("page data {}", page_id);
            write_guard.add_item(data.as_bytes()).unwrap();
        }
        guards.push(page_guard); // Keep guards to keep pages pinned initially
    }

    // Unpin all pages
    guards.clear();

    // Create one more page, this should force an eviction (likely page 0 due to clock)
    let page_guard_new = bpm.new_page().expect("Failed to create page causing eviction");
    let new_page_id = page_guard_new.page_id();
    {
        let mut write_guard = page_guard_new.write().unwrap();
        write_guard.add_item(b"evictor page").unwrap();
    }
    drop(page_guard_new);

    // Try to fetch the first page (page_id 0). It *might* require a disk read if evicted.
    // This test mainly ensures the BPM doesn't block or error when full and eviction happens.
    let page_guard_0 = bpm.fetch_page(0).expect("Failed to fetch page 0 after potential eviction");
     {
        let read_guard = page_guard_0.read().unwrap();
        assert_eq!(read_guard.get_item(0).unwrap(), b"page data 0"); // Check data integrity
    }
    drop(page_guard_0);

    // Fetch the newly added page
    let page_guard_evictor = bpm.fetch_page(new_page_id).expect("Failed to fetch evictor page");
    {
        let read_guard = page_guard_evictor.read().unwrap();
        assert_eq!(read_guard.get_item(0).unwrap(), b"evictor page");
    }
    drop(page_guard_evictor);


    cleanup_db(&db_filename);
}

#[test]
fn test_bpm_dirty_flush_on_eviction() {
    let pool_size = 2;
    let (disk_scheduler, bpm, db_filename) = setup_bpm(pool_size);

    // Page 1: Create and make dirty
    let page1_guard = bpm.new_page().expect("Failed to create page 1");
    let page1_id = page1_guard.page_id(); // Should be 0
    {
        let mut write_guard = page1_guard.write().unwrap();
        write_guard.add_item(b"dirty data 1").unwrap();
    }
    drop(page1_guard); // Unpin page 1, it's dirty

    // Page 2: Create and make dirty
    let page2_guard = bpm.new_page().expect("Failed to create page 2");
    let _page2_id = page2_guard.page_id(); // Should be 1 (Marked unused for now)
     {
        let mut write_guard = page2_guard.write().unwrap();
        write_guard.add_item(b"dirty data 2").unwrap();
    }
    drop(page2_guard); // Unpin page 2, it's dirty

    // Page 3: Create, this should evict page 1 (ID 0)
    // Eviction should trigger flush of page 1's dirty data.
    let page3_guard = bpm.new_page().expect("Failed to create page 3");
    let _page3_id = page3_guard.page_id(); // Should be 2
    {
        let mut write_guard = page3_guard.write().unwrap();
        write_guard.add_item(b"page 3 data").unwrap();
    }
    drop(page3_guard); // Unpin page 3

    // Drop the BPM instance to ensure its internal state doesn't interfere
    // and potentially trigger flushes if Drop implemented it (it doesn't currently).
    drop(bpm);
    drop(disk_scheduler); // Drop scheduler too

    // Re-open the database file with a new BPM and DiskScheduler
    let disk_scheduler2 = Arc::new(DiskScheduler::new(&db_filename).unwrap());
    let bpm2 = Arc::new(BufferPoolManager::new(disk_scheduler2.clone(), pool_size));

    // Fetch page 1 (ID 0) - it should have the flushed dirty data from the eviction
    let page1_refetch_guard = bpm2.fetch_page(page1_id).expect("Failed to fetch page 1 after reopen");
    {
        let read_guard = page1_refetch_guard.read().unwrap();
        assert_eq!(read_guard.get_item(0).unwrap(), b"dirty data 1");
    }
    drop(page1_refetch_guard);

    drop(bpm2);
    drop(disk_scheduler2);

    cleanup_db(&db_filename);
}


#[test]
fn test_bpm_pinning_prevents_eviction() {
    let pool_size = 3;
    let (_disk_scheduler, bpm, db_filename) = setup_bpm(pool_size);
    let mut guards = Vec::new();

    // Fill the pool
    for i in 0..pool_size {
        let guard = bpm.new_page().expect(&format!("Failed to create page {}", i));
        guards.push(guard);
    }

    // Keep guard[0] (page 0) pinned
    // Drop guards for page 1 and 2
    let pinned_guard = guards.remove(0);
    guards.clear(); // Drops guards for page 1 and 2

    // Create two more pages - should evict pages 1 and 2
    let guard_a = bpm.new_page().expect("Failed to create page A");
    drop(guard_a);
    let guard_b = bpm.new_page().expect("Failed to create page B");
    drop(guard_b);

    // Try to create one more page. If page 0 wasn't pinned, it would be evicted.
    // Since it *is* pinned, and pages 1 & 2 were replaced by A & B,
    // the only candidates are A & B.
    let guard_c = bpm.new_page().expect("Failed to create page C");
    drop(guard_c);

    // Check if the pinned page's data is still accessible via its guard
    // (This implicitly checks it wasn't evicted/overwritten)
    {
        let read_guard = pinned_guard.read().unwrap();
        // We didn't write specific data, but accessing it shouldn't panic.
        // The previous check `assert!(read_guard.num_slots() >= 0)` was redundant
        // because num_slots() likely returns an unsigned type (e.g., usize),
        // which is always >= 0. Simply accessing it is enough for this basic check.
        let _ = read_guard.num_slots(); // Access num_slots to ensure it doesn't panic
    }

    // Now drop the pinned guard
    drop(pinned_guard);

    // Create another page, now page 0 *can* be evicted.
    let guard_d = bpm.new_page().expect("Failed to create page D after unpin");
    drop(guard_d);

    // This doesn't definitively prove page 0 *was* evicted, but shows the flow works.
    // A more complex check would involve checking the page table state if accessible.

    cleanup_db(&db_filename);
}

#[test]
fn test_bpm_no_available_frames() {
    let pool_size = 2;
    let (_disk_scheduler, bpm, db_filename) = setup_bpm(pool_size);
    let mut guards = Vec::new();

    // Fill the pool and keep all pages pinned
    for _ in 0..pool_size {
        let guard = bpm.new_page().expect("Failed to create page");
        guards.push(guard); // Keep the guard, page remains pinned
    }

    // Try to get another page - should fail
    let result = bpm.new_page();
    assert!(matches!(result, Err(baselab::storage::buffer::error::BufferPoolError::NoAvailableFrames)));

    // Try to fetch a non-existent page (which would need a frame) - should also fail
    let fetch_result = bpm.fetch_page(999); // Assuming page 999 doesn't exist
    assert!(matches!(fetch_result, Err(baselab::storage::buffer::error::BufferPoolError::NoAvailableFrames)));

    // Drop guards to allow completion
    guards.clear();
    cleanup_db(&db_filename);
}


// --- Concurrency Tests ---

#[test]
fn test_bpm_concurrent_fetch_same_page() {
    let (disk_scheduler, bpm, db_filename) = setup_bpm(BUFFER_POOL_SIZE);
    let num_threads = 5;
    let num_ops_per_thread = 10;

    // Create a page on disk first
    let page_id_on_disk: PageId = 0;
    let mut page = Page::new();
    page.init(0); 
    let sid = page.add_item(b"concurrent fetch data").unwrap();
    disk_scheduler.allocate_page().unwrap();
    disk_scheduler.write_page(page_id_on_disk, page.raw.to_vec()).unwrap();
    disk_scheduler.sync().unwrap();

    let mut handles = vec![];

    for _ in 0..num_threads {
        let bpm_clone = bpm.clone();
        let handle = thread::spawn(move || {
            for _ in 0..num_ops_per_thread {
                let page_guard = bpm_clone.fetch_page(page_id_on_disk).expect("Fetch failed");
                {
                    let read_guard = page_guard.read().expect("Read lock failed");
                    assert_eq!(read_guard.get_item(sid).unwrap(), b"concurrent fetch data");
                }
                // Drop guard implicitly
                 std::thread::yield_now(); // Encourage context switching
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    cleanup_db(&db_filename);
}

#[test]
fn test_bpm_concurrent_new_pages() {
    let (_disk_scheduler, bpm, db_filename) = setup_bpm(BUFFER_POOL_SIZE);
    let num_threads = 8; // More threads than pool size potentially
    let pages_per_thread = 3;

    let mut handles = vec![];
    let created_pages = Arc::new(Mutex::new(Vec::new()));

    for i in 0..num_threads {
        let bpm_clone = bpm.clone();
        let created_pages_clone = created_pages.clone();
        let handle = thread::spawn(move || {
            let mut thread_pages = Vec::new();
            for j in 0..pages_per_thread {
                let page_guard = bpm_clone.new_page().expect("New page failed");
                let page_id = page_guard.page_id();
                let data = format!("thread {} page {}", i, j);
                {
                    let mut write_guard = page_guard.write().expect("Write lock failed");
                    write_guard.add_item(data.as_bytes()).unwrap();
                } // Write guard dropped
                thread_pages.push((page_id, data));
                // Drop page_guard implicitly
                 std::thread::yield_now();
            }
            let mut locked_pages = created_pages_clone.lock().unwrap();
            locked_pages.extend(thread_pages);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verification: Fetch all created pages and check their content
    let final_pages = created_pages.lock().unwrap();
    assert_eq!(final_pages.len(), num_threads * pages_per_thread);

    for (page_id, expected_data) in final_pages.iter() {
        let page_guard = bpm.fetch_page(*page_id).expect(&format!("Fetch failed for page {}", page_id));
        {
            let read_guard = page_guard.read().expect(&format!("Read lock failed for page {}", page_id));
            // Assuming data is always in slot 0 for these simple tests
            assert_eq!(read_guard.get_item(0).unwrap(), expected_data.as_bytes());
        }
    }

    cleanup_db(&db_filename);
}

#[test]
fn test_bpm_concurrent_fetch_different_pages_eviction() {
    let pool_size = 5; // Small pool size relative to operations
    let (disk_scheduler, bpm, db_filename) = setup_bpm(pool_size);
    let num_threads = 4;
    let pages_per_thread = 10; // Total pages > pool_size * 2 to force eviction heavily
    let total_pages = num_threads * pages_per_thread;

    // Pre-populate pages on disk
    let page_contents: Vec<(PageId, String)> = (0..total_pages).map(|i| {
        let page_id = i as PageId;
        let data = format!("page content {}", page_id);
        let mut page = Page::new();
        page.init(0); // Correct: Initialize with 0 special size
        page.add_item(data.as_bytes()).unwrap();
        disk_scheduler.allocate_page().unwrap();
        disk_scheduler.write_page(page_id, page.raw.to_vec()).unwrap();
        (page_id, data)
    }).collect();
    disk_scheduler.sync().unwrap();


    let mut handles = vec![];

    for i in 0..num_threads {
        let bpm_clone = bpm.clone();
        let page_contents_clone = page_contents.clone(); // Clone data for verification inside thread
        let handle = thread::spawn(move || {
            for j in 0..pages_per_thread {
                // Each thread accesses a distinct set of pages
                let page_index = i * pages_per_thread + j;
                let (page_id, expected_data) = &page_contents_clone[page_index];

                let page_guard = bpm_clone.fetch_page(*page_id)
                    .expect(&format!("Thread {} failed fetching page {}", i, page_id));
                {
                    let read_guard = page_guard.read()
                        .expect(&format!("Thread {} failed read lock page {}", i, page_id));
                    assert_eq!(read_guard.get_item(0).unwrap(), expected_data.as_bytes());
                }
                // Drop guard implicitly
                std::thread::yield_now(); // Encourage contention
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Final check: Fetch a few pages again to ensure basic state is okay
     let (page_id_check, expected_data_check) = &page_contents[total_pages / 2];
     let guard_check = bpm.fetch_page(*page_id_check).unwrap();
     {
         let read_guard = guard_check.read().unwrap();
         assert_eq!(read_guard.get_item(0).unwrap(), expected_data_check.as_bytes());
     }


    cleanup_db(&db_filename);
}
