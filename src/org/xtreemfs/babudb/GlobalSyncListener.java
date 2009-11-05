package org.xtreemfs.babudb;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.replication.ReplicationManager;

public class GlobalSyncListener implements SyncListener {

    private final ReplicationManager replMan;
    private final BabuDB dbs;
    
    public GlobalSyncListener(ReplicationManager replMan, BabuDB dbs) {
        this.replMan = replMan;
        this.dbs = dbs;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.log.SyncListener#failed(org.xtreemfs.babudb.log.LogEntry, java.lang.Exception)
     */
    @Override
    public void failed(LogEntry entry, Exception ex) {
        entry.free();
        
        if (replMan != null) {
            // get some rest until the fail-over-mechanism starts
            if (replMan.isMaster()) {
                replMan.halt();                    
            }
        } else {
            // not a consistent state! PANIC!!!
            try {
                dbs.shutdown();
            } catch (BabuDBException e) { /* I don't care */ }
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.log.SyncListener#synced(org.xtreemfs.babudb.log.LogEntry)
     */
    @Override
    public void synced(LogEntry entry) {
        entry.free();
    }

}
