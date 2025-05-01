use std::{
    io,
    sync::mpsc::{self, Receiver, Sender},
    thread,
};
use crate::storage::disk::{DiskManager, PageId};
use crate::storage::page::BLCKSZ;

/// Represents the operation to perform.
enum DiskOp {
    Read(PageId),
    Write(PageId, Vec<u8>),
    Sync,
    AllocatePage,
}

/// Unified result enum for all operations.
enum DiskResult {
    Read(io::Result<Vec<u8>>),
    Write(io::Result<()>),
    Sync(io::Result<()>),
    AllocatePage(io::Result<PageId>),
}

/// A request sent to the disk‐worker.
struct Request {
    op: DiskOp,
    resp: Sender<DiskResult>,
}

/// A scheduler that serializes all disk I/O through one background thread.
pub struct DiskScheduler {
    tx: Sender<Request>,
}

impl DiskScheduler {
    /// Spawns a background thread and returns a handle.
    pub fn new<P: AsRef<std::path::Path>>(db_path: P) -> io::Result<Self> {
        let manager = DiskManager::new(db_path)?;
        let (tx, rx): (Sender<Request>, Receiver<Request>) = mpsc::channel();

        // Worker thread
        thread::spawn(move || {
            let mgr = manager;
            for req in rx {
                match req.op {
                    DiskOp::Read(pid) => {
                        let mut buf = vec![0u8; BLCKSZ];
                        let result = mgr.read_page(pid, &mut buf).map(|_| buf);
                        let _ = req.resp.send(DiskResult::Read(result));
                    }
                    DiskOp::Write(pid, data) => {
                        let result = mgr.write_page(pid, &data);
                        let _ = req.resp.send(DiskResult::Write(result));
                    }
                    DiskOp::Sync => {
                        let result = mgr.sync();
                        let _ = req.resp.send(DiskResult::Sync(result));
                    }
                    DiskOp::AllocatePage => {
                        let page_id = mgr.allocate_page();
                        let _ = req.resp.send(DiskResult::AllocatePage(Ok(page_id)));
                    }
                }
            }
            // channel closed → exit thread
        });

        Ok(Self { tx })
    }

    /// Read a page; returns a fresh buffer of size BLCKSZ on success.
    pub fn read_page(&self, page_id: PageId) -> io::Result<Vec<u8>> {
        let (resp_tx, resp_rx) = mpsc::channel();
        let req = Request {
            op: DiskOp::Read(page_id),
            resp: resp_tx,
        };
        self.tx.send(req).unwrap();
        match resp_rx.recv().unwrap() {
            DiskResult::Read(r) => r,
            _ => unreachable!("mismatched response variant"),
        }
    }

    /// Write a full‐size buffer to disk.
    pub fn write_page(&self, page_id: PageId, buf: Vec<u8>) -> io::Result<()> {
        let (resp_tx, resp_rx) = mpsc::channel();
        let req = Request {
            op: DiskOp::Write(page_id, buf),
            resp: resp_tx,
        };
        self.tx.send(req).unwrap();
        match resp_rx.recv().unwrap() {
            DiskResult::Write(r) => r,
            _ => unreachable!("mismatched response variant"),
        }
    }

    /// Flush all pending writes to disk.
    pub fn sync(&self) -> io::Result<()> {
        let (resp_tx, resp_rx) = mpsc::channel();
        let req = Request {
            op: DiskOp::Sync,
            resp: resp_tx,
        };
        self.tx.send(req).unwrap();
        match resp_rx.recv().unwrap() {
            DiskResult::Sync(r) => r,
            _ => unreachable!("mismatched response variant"),
        }
    }

    pub fn allocate_page(&self) -> io::Result<PageId> {
    let (resp_tx, resp_rx) = mpsc::channel();
    let req = Request {
        op: DiskOp::AllocatePage,
        resp: resp_tx,
    };
    self.tx.send(req).unwrap();
    match resp_rx.recv().unwrap() {
        DiskResult::AllocatePage(r) => r,
        _ => unreachable!("mismatched response variant"),
    }
}

}



// for testing pupose
impl Clone for DiskScheduler {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}