use baselab::storage::page::Page;
use std::collections::HashMap;

#[test]
fn smoke_test_add_and_get() {
    let mut page = Page::new();
    page.init(0);

    let sid = page.add_item(b"xyz").unwrap();
    assert_eq!(page.get_item(sid).unwrap(), b"xyz");
}

#[test]
fn full_cycle() {
    let mut page = Page::new();
    page.init(0);

    // fill page until no more space
    let mut slots = Vec::new();
    while let Some(sid) = page.add_item(&[0u8; 128]) {
        slots.push(sid);
    }
    assert!(page.free_space() < 128);

    // Delete some items
    if !slots.is_empty() {
        page.delete_item(slots[0]);
    }

    // Before compaction, we might not have enough space
    assert!(page.free_space() < 128);

    // After compaction, we should have freed space
    assert_eq!(page.compact(), true);
    assert!(page.free_space() >= 128);
}

#[test]
fn test_delete_item() {
    let mut page = Page::new();
    page.init(0);
    
    let sid = page.add_item(b"test_data").unwrap();
    assert!(page.get_item(sid).is_some());
    
    // Delete the item
    assert!(page.delete_item(sid));
    
    // Item should no longer be retrievable
    assert!(page.get_item(sid).is_none());
    
    // Attempting to delete an already deleted item should fail
    assert!(!page.delete_item(sid));
}

#[test]
fn test_reuse_deleted_slots() {
    let mut page = Page::new();
    page.init(0);
    
    // Add some items
    let sid1 = page.add_item(b"item1").unwrap();
    let sid2 = page.add_item(b"item2").unwrap();
    
    // Check initial state
    assert_eq!(page.num_slots(), 2);
    assert_eq!(page.num_active_slots(), 2);
    
    // Delete one item
    page.delete_item(sid1);
    assert_eq!(page.num_slots(), 2);
    assert_eq!(page.num_active_slots(), 1);
    
    // Add a new item - should reuse the deleted slot
    let sid3 = page.add_item(b"item3").unwrap();
    assert_eq!(sid3, sid1); // Should reuse the same slot ID
    assert_eq!(page.num_slots(), 2); // Still only 2 slots total
    assert_eq!(page.num_active_slots(), 2);
    
    // Verify both items are accessible
    assert_eq!(page.get_item(sid2).unwrap(), b"item2");
    assert_eq!(page.get_item(sid3).unwrap(), b"item3");
}


#[test]
fn test_edge_cases() {
    let mut page = Page::new();
    page.init(0);
    
    // Test empty page
    assert_eq!(page.num_slots(), 0);
    assert_eq!(page.num_active_slots(), 0);
    
    // Test invalid slot access
    assert!(page.get_item(999).is_none());
    assert!(!page.delete_item(999));
    
    // Test adding oversize item
    let big_data = vec![0u8; 9000]; // Larger than page size
    assert!(page.add_item(&big_data).is_none());
    
    // Test compacting an empty page
    assert!(page.compact());
}


#[test]
fn test_fragmentation_and_compaction() {
    let mut page = Page::new();
    page.init(0);
    
    // Add items of varying sizes
    let sid1 = page.add_item(&[0u8; 1000]).unwrap();
    let sid2 = page.add_item(&[0u8; 500]).unwrap();
    let sid3 = page.add_item(&[0u8; 2000]).unwrap();
    
    // Calculate initial free space
    let initial_free = page.free_space();
    
    // Verify all items are accessible
    assert_eq!(page.get_item(sid1).unwrap().len(), 1000);
    assert_eq!(page.get_item(sid2).unwrap().len(), 500);
    assert_eq!(page.get_item(sid3).unwrap().len(), 2000);
    
    // Delete middle item
    page.delete_item(sid2);
    assert!(page.get_item(sid2).is_none());
    
    // Free space after deletion but before compaction
    let post_delete_free = page.free_space();
    
    // Free space should not have significantly increased
    assert!(post_delete_free - initial_free < 20); // Allow for small variations
    
    // Compact the page
    page.compact();
    
    // After compaction, free space should increase by about 500 bytes
    let post_compact_free = page.free_space();
    assert!(post_compact_free > post_delete_free + 480); // ~500 bytes freed
    
    // Verify remaining items are still accessible (possibly with new IDs due to compaction)
    // We need to check the contents rather than the IDs
    let mut found_item1 = false;
    let mut found_item3 = false;
    
    for i in 0..page.num_slots() {
        if let Some(item) = page.get_item(i) {
            if item.len() == 1000 {
                found_item1 = true;
            } else if item.len() == 2000 {
                found_item3 = true;
            }
        }
    }
    
    assert!(found_item1, "Item 1 should still be accessible after compaction");
    assert!(found_item3, "Item 3 should still be accessible after compaction");
}

#[test]
fn test_compaction_data_integrity() {
    let mut page = Page::new();
    page.init(0);
    let mut expected_items = HashMap::new();

    // Add items
    let data1 = b"item the first".to_vec();
    let sid1 = page.add_item(&data1).unwrap();
    expected_items.insert(sid1, data1);

    let data2 = b"second item data".to_vec();
    let sid2 = page.add_item(&data2).unwrap();
    // Don't add sid2 to expected_items, we will delete it

    let data3 = b"numero tres".to_vec();
    let sid3 = page.add_item(&data3).unwrap();
    expected_items.insert(sid3, data3);

    // Delete the middle item
    assert!(page.delete_item(sid2));

    // Compact
    assert!(page.compact());

    // Verify remaining items after compaction
    let mut found_count = 0;
    let mut actual_items_after_compact = HashMap::new();
    for i in 0..page.num_slots() {
        if let Some(item_data) = page.get_item(i) {
            // Store the data found at the new slot id 'i'
            actual_items_after_compact.insert(i, item_data.to_vec());
            found_count += 1;
        }
    }

    assert_eq!(found_count, expected_items.len(), "Should have the same number of active items after compaction");

    // Check that all expected items are present in the actual items, regardless of slot id
    for expected_data in expected_items.values() {
        let found = actual_items_after_compact.values().any(|actual_data| actual_data == expected_data);
        assert!(found, "Expected item data {:?} not found after compaction", expected_data);
    }
}



