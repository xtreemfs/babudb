/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.log;

/**
 * Simple listener called after a LogEntry was synced or sent.
 * @author bjko
 */
public interface SyncListener {
    
    /** Called after the LogEntry was synced to logfile
     */
    public void synced(LogEntry entry);
    
    /** Called if the LogEntry could not be written/sent.
     */
    public void failed(LogEntry entry, Exception ex);
    
}
