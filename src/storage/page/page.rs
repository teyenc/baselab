use std::mem::size_of;

pub const BLCKSZ: usize = 8 * 1024;
pub type SlotId = usize;

#[repr(C)]
#[derive(Clone, Copy)]
struct PageHeader {
    pd_lower:   u16,  // end of slot array
    pd_upper:   u16,  // start of tuple area
    pd_special: u16,  // start of "special" region (unused here)
    flags:      u16,  // reserved for AM-specific flags
}

const PAGE_HEADER_SIZE: usize = size_of::<PageHeader>();

#[repr(C)]
#[derive(Clone, Copy)]
struct Slot {
    offset: u16,
    length: u16,
    flags: u16,  // Added flags to track deletion status and other metadata
}

// Flag definitions for slot entries
const SLOT_FLAG_DELETED: u16 = 0x0001;  // Marks a slot as deleted

const SLOT_SIZE: usize = size_of::<Slot>();

/// A single 8 KiB page with a simple slot‐array / tuple‐heap layout.
pub struct Page {
    raw: [u8; BLCKSZ],
}

impl Page {
    /// All‐zero page.
    pub fn new() -> Self {
        Self { raw: [0; BLCKSZ] }
    }

    /// Initialize a fresh page, reserving `special_size` bytes at the end.
    pub fn init(&mut self, special_size: usize) {
        let up = (BLCKSZ - special_size) as u16;
        // Zero everything first
        self.raw.iter_mut().for_each(|b| *b = 0);
        // Now get header and update it
        let hdr = self.header_mut();
        hdr.pd_lower = PAGE_HEADER_SIZE as u16;
        hdr.pd_upper = up;
        hdr.pd_special = up;
        hdr.flags = 0;
    }

    /// How many bytes remain (after reserving one more slot)
    pub fn free_space(&self) -> usize {
        let hdr = self.header();
        let used = hdr.pd_lower as usize;
        let avail = hdr.pd_upper as usize - used;
        return avail.saturating_sub(SLOT_SIZE);
    }

    /// Current slot count (including deleted slots)
    pub fn num_slots(&self) -> usize {
        (self.header().pd_lower as usize - PAGE_HEADER_SIZE) / SLOT_SIZE
    }

    /// Count of active (non-deleted) slots
    pub fn num_active_slots(&self) -> usize {
        let mut count = 0;
        for sid in 0..self.num_slots() {
            let slot = self.slot(sid);
            if slot.flags & SLOT_FLAG_DELETED == 0 {
                count += 1;
            }
        }
        count
    }

    /// Append a new item. Returns slot id or `None` if out of space.
    pub fn add_item(&mut self, data: &[u8]) -> Option<SlotId> {
        // First, try to reuse a deleted slot
        if let Some(sid) = self.find_deleted_slot() {
            // We need to check if there's space for the new data
            if data.len() <= self.free_space() {
                // Get necessary values from header
                let pd_upper = self.header().pd_upper as usize - data.len();
                
                // Copy data
                self.raw[pd_upper .. pd_upper + data.len()].copy_from_slice(data);
                
                // Update slot
                let slot = self.slot_mut(sid);
                slot.offset = pd_upper as u16;
                slot.length = data.len() as u16;
                slot.flags &= !SLOT_FLAG_DELETED; // Clear deleted flag
                
                // Update header upper pointer
                let hdr = self.header_mut();
                hdr.pd_upper = pd_upper as u16;
                
                return Some(sid);
            }
            return None; // Not enough space
        }
        
        // No deleted slots to reuse, add a new one
        if self.free_space() < data.len() {
            return None;
        }
        
        // Get necessary values from header first
        let pd_lower = self.header().pd_lower;
        let pd_upper = self.header().pd_upper as usize - data.len();
        
        // Copy data first
        self.raw[pd_upper .. pd_upper + data.len()].copy_from_slice(data);
        
        // Calculate slot index
        let sid = (pd_lower as usize - PAGE_HEADER_SIZE) / SLOT_SIZE;
        
        // Write slot
        let base = PAGE_HEADER_SIZE + sid * SLOT_SIZE;
        let slot = unsafe { &mut *(self.raw.as_mut_ptr().add(base) as *mut Slot) };
        slot.offset = pd_upper as u16;
        slot.length = data.len() as u16;
        slot.flags = 0; // Not deleted
        
        // Update header values
        let hdr = self.header_mut();
        hdr.pd_upper = pd_upper as u16;
        hdr.pd_lower = pd_lower + SLOT_SIZE as u16;
        
        Some(sid)
    }
    
    /// Retrieve a slice for a given slot, if it exists and isn't deleted
    pub fn get_item(&self, sid: SlotId) -> Option<&[u8]> {
        if sid >= self.num_slots() {
            return None;
        }
        
        let slot = self.slot(sid);
        
        // Check if the slot is marked as deleted
        if slot.flags & SLOT_FLAG_DELETED != 0 {
            return None;
        }
        
        let o = slot.offset as usize;
        let l = slot.length as usize;
        Some(&self.raw[o .. o + l])
    }

    /// Mark a slot as deleted
    pub fn delete_item(&mut self, sid: SlotId) -> bool {
        if sid >= self.num_slots() {
            return false;
        }
        
        let slot = self.slot_mut(sid);
        
        // Check if already deleted
        if slot.flags & SLOT_FLAG_DELETED != 0 {
            return false;
        }
        
        // Mark as deleted
        slot.flags |= SLOT_FLAG_DELETED;
        true
    }

    /// Defragment the page by reclaiming space from deleted tuples
    /// This is an expensive operation as it may require moving tuples
    pub fn compact(&mut self) -> bool {
        // If no active slots, reset the whole page
        if self.num_active_slots() == 0 {
            let special_size = BLCKSZ - self.header().pd_special as usize;
            self.init(special_size);
            return true;
        }
        
        let mut new_page = Page::new();
        let special_size = BLCKSZ - self.header().pd_special as usize;
        new_page.init(special_size);
        
        // Copy all non-deleted tuples to the new page
        let mut slot_map: Vec<Option<SlotId>> = vec![None; self.num_slots()];
        
        for old_sid in 0..self.num_slots() {
            if let Some(data) = self.get_item(old_sid) {
                // This is a valid, non-deleted tuple
                if let Some(new_sid) = new_page.add_item(data) {
                    slot_map[old_sid] = Some(new_sid);
                } else {
                    // Should not happen if we're just copying existing data that fit before
                    return false;
                }
            }
        }
        
        // Replace our content with the compacted version
        self.raw = new_page.raw;
        
        true
    }

    /// Finds a deleted slot that can be reused
    fn find_deleted_slot(&self) -> Option<SlotId> {
        for sid in 0..self.num_slots() {
            let slot = self.slot(sid);
            if slot.flags & SLOT_FLAG_DELETED != 0 {
                return Some(sid);
            }
        }
        None
    }

    // ——— internal helpers ———

    fn header(&self) -> &PageHeader {
        unsafe { &*(self.raw.as_ptr() as *const PageHeader) }
    }
    fn header_mut(&mut self) -> &mut PageHeader {
        unsafe { &mut *(self.raw.as_mut_ptr() as *mut PageHeader) }
    }

    fn slot(&self, idx: usize) -> &Slot {
        let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
        unsafe { &*(self.raw.as_ptr().add(base) as *const Slot) }
    }
    fn slot_mut(&mut self, idx: usize) -> &mut Slot {
        let base = PAGE_HEADER_SIZE + idx * SLOT_SIZE;
        unsafe { &mut *(self.raw.as_mut_ptr().add(base) as *mut Slot) }
    }

}