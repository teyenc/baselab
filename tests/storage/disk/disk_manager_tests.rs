use baselab::storage::disk::DiskManager;
use baselab::storage::page::Page;
use std::fs;
use std::path::Path;

#[test]
fn test_disk_manager_basics() {
    let test_file = "test_db.db";
    
    // Clean up any existing test file
    if Path::new(test_file).exists() {
        fs::remove_file(test_file).unwrap();
    }
    
    // Create a new disk manager
    let disk_manager = DiskManager::new(test_file).unwrap();
    
    // Allocate a page and verify the count
    let page_id = disk_manager.allocate_page();
    assert_eq!(page_id, 0);
    assert_eq!(disk_manager.num_pages(), 1);
    
    // Create a page with some data
    let mut page = Page::new();
    page.init(0);
    let sid = page.add_item(b"hello disk manager").unwrap();
    
    // Write the page to disk
    disk_manager.write_page(page_id, &page.raw).unwrap();
    
    // Read it back into a new page
    let mut page2 = Page::new();
    disk_manager.read_page(page_id, &mut page2.raw).unwrap();
    
    // Verify the data
    assert_eq!(page2.get_item(sid).unwrap(), b"hello disk manager");
    
    // Allocate another page
    let page_id2 = disk_manager.allocate_page();
    assert_eq!(page_id2, 1);
    assert_eq!(disk_manager.num_pages(), 2);
    
    // Clean up
    fs::remove_file(test_file).unwrap();
}

#[test]
fn test_disk_manager_persistence() {
    let test_file = "test_persistence.db";
    
    // Clean up any existing test file
    if Path::new(test_file).exists() {
        fs::remove_file(test_file).unwrap();
    }
    
    // Write data
    {
        let disk_manager = DiskManager::new(test_file).unwrap();
        let page_id = disk_manager.allocate_page();
        
        let mut page = Page::new();
        page.init(0);
        page.add_item(b"persistent data").unwrap();
        
        disk_manager.write_page(page_id, &page.raw).unwrap();
        disk_manager.sync().unwrap();
    }
    
    // Reopen and read data
    {
        let disk_manager = DiskManager::new(test_file).unwrap();
        assert_eq!(disk_manager.num_pages(), 1);
        
        let mut page = Page::new();
        disk_manager.read_page(0, &mut page.raw).unwrap();
        
        // Find the item (we know it's at slot 0)
        let data = page.get_item(0).unwrap();
        assert_eq!(data, b"persistent data");
    }
    
    // Clean up
    fs::remove_file(test_file).unwrap();
}