use baselab::storage::disk::DiskScheduler;
use baselab::storage::page::{Page, BLCKSZ};
use std::fs;
use std::path::Path;

#[test]
fn test_disk_scheduler_basics() {
    let test_file = "test_scheduler.db";
    
    // Clean up any existing test file
    if Path::new(test_file).exists() {
        fs::remove_file(test_file).unwrap();
    }
    
    // Create a new disk scheduler
    let scheduler = DiskScheduler::new(test_file).unwrap();
    
    // Allocate a page first
    let page_id = scheduler.allocate_page().unwrap();
    assert_eq!(page_id, 0);
    
    // Create a page with some data
    let mut page = Page::new();
    page.init(0);
    let sid = page.add_item(b"hello disk scheduler").unwrap();
    
    // Write the page to disk
    scheduler.write_page(page_id, page.raw.to_vec()).unwrap();
    
    // Read it back
    let raw_page = scheduler.read_page(page_id).unwrap();
    
    // Convert raw data back to a Page
    let mut page2 = Page::new();
    page2.raw.copy_from_slice(&raw_page);
    
    // Verify the data
    assert_eq!(page2.get_item(sid).unwrap(), b"hello disk scheduler");
    
    // Test sync operation
    scheduler.sync().unwrap();
    
    // Clean up
    fs::remove_file(test_file).unwrap();
}

#[test]
fn test_disk_scheduler_concurrent_operations() {
    let test_file = "test_scheduler_concurrent.db";
    
    // Clean up any existing test file
    if Path::new(test_file).exists() {
        fs::remove_file(test_file).unwrap();
    }
    
    // Test configuration
    let num_threads = 10;
    let operations_per_thread = 5;
    
    // Create a shared disk scheduler
    let scheduler = DiskScheduler::new(test_file).unwrap();
    
    // Pre-allocate pages for all threads and operations
    let total_pages_needed = num_threads * operations_per_thread;
    for _ in 0..total_pages_needed {
        scheduler.allocate_page().unwrap();
    }
    
    // Spawn multiple threads to perform operations
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let scheduler_clone = scheduler.clone();
        let handle = std::thread::spawn(move || {
            for op_id in 0..operations_per_thread {
                // Calculate a unique page ID for this thread and operation
                let page_id = thread_id * operations_per_thread + op_id;
                
                // Create a unique page with identifiable data
                let mut page = Page::new();
                page.init(0);
                let message = format!("thread {} operation {} data", thread_id, op_id);
                let sid = page.add_item(message.as_bytes()).unwrap();
                
                // Write the page
                scheduler_clone.write_page(page_id, page.raw.to_vec()).unwrap();
                
                // Read it back
                let raw_page = scheduler_clone.read_page(page_id).unwrap();
                
                // Convert raw data back to a Page
                let mut page2 = Page::new();
                page2.raw.copy_from_slice(&raw_page);
                
                // Verify the data
                assert_eq!(
                    page2.get_item(sid).unwrap(), 
                    message.as_bytes()
                );
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Final sync
    scheduler.sync().unwrap();
    
    // Clean up
    fs::remove_file(test_file).unwrap();
}

#[test]
fn test_disk_scheduler_error_handling() {
    let test_file = "test_scheduler_errors.db";
    
    // Clean up any existing test file
    if Path::new(test_file).exists() {
        fs::remove_file(test_file).unwrap();
    }
    
    // Create a new disk scheduler
    let scheduler = DiskScheduler::new(test_file).unwrap();
    
    // Try to read a non-existent page (should fail)
    let result = scheduler.read_page(999);
    assert!(result.is_err());
    
    // Write a page with incorrect size (should fail)
    let bad_data = vec![0u8; BLCKSZ / 2]; // Half the required size
    let result = scheduler.write_page(0, bad_data);
    assert!(result.is_err());
    
    // Clean up
    fs::remove_file(test_file).unwrap();
}
