**BabuDB** is an embedded non-relational database system. Its lean and simple design allows it to persistently store large amounts of key-value pairs without the overhead and complexity of similar approaches such as BerkeleyDB.

Key features:

 * Support for large-scale databases that exceed the system's main memory
 * Efficient crash recovery
 * Snapshots and asynchronous dumps
 * Prefix and range lookups
 * Transparent replication with tuneable consistency/performance trade-offs
 * BabuDB has been independently implemented for Java and C++ (Win32/Linux). APIs and database formats of both implementations are not compatible.

Much of the simplicity and efficiency of the BabuDB design comes from the use of small mutable overlay-trees (also known as LSM-trees) layered on a larger immutable memory-mapped on-disk index, an architecture that was made popular by Google's BigTable?.
BabuDB is used as the database engine in the replicated metadata server of the XtreemFS file system.

You can contact the BabuDB developers at: babudb@googlegroups.com
