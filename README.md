# Baselab

A work-in-progress relational database implementation built from scratch in Rust.

## Progress

### Storage Engine
- ✅ **Page Management**: Basic page structure with item storage
- ✅ **Disk Manager**: File I/O operations and page persistence
- ✅ **Disk Scheduler**: Async disk operation scheduling
- ✅ **Buffer Pool Manager**: In-memory page caching and management
- ⏳ **Index Structures**: B+ trees and other indexing methods (I built a B+ tree in [this project](https://15445.courses.cs.cmu.edu/spring2025/project2/) but didn't have time to rewrite in Rust yet)

### Query Engine
- ⏳ **Query Planner**: Optimize and plan query execution
- ⏳ **Query Executor**: Execute planned queries
  
(I built Query Planner and Executor in [this project](https://15445.courses.cs.cmu.edu/spring2025/project3/) but didn't have time to rewrite in Rust yet)

### Concurrency Control ⏳
I wrote a multi-version concurrency control (MVCC) in [this project](https://15445.courses.cs.cmu.edu/spring2025/project4/) but didn't have time to rewrite in Rust yet


## Components

- `src/main.rs`: Demo application showing page functionality
- `src/lib.rs`: The library crate of the database
- `src/storage`: The storage engine of the database
    - `src/storage/disk`: Disk management and scheduling
    - `src/storage/buffer`: Buffer pool management
    - `src/storage/page`: Page structures and operations 
