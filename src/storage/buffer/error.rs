use std::io;
use std::sync::PoisonError;
use thiserror::Error;
use crate::storage::disk::PageId;

#[derive(Error, Debug)]
pub enum BufferPoolError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    
    #[error("Lock poisoned: {0}")]
    LockError(String),
    
    #[error("No available frames in buffer pool")]
    NoAvailableFrames,
    
    #[error("Page {0} not found in buffer pool")]
    PageNotFound(PageId),
    
    #[error("Buffer pool operation failed: {0}")]
    OperationFailed(String),
}

// Implement conversion from PoisonError for different lock types
impl<T> From<PoisonError<T>> for BufferPoolError {
    fn from(err: PoisonError<T>) -> Self {
        BufferPoolError::LockError(err.to_string())
    }
} 