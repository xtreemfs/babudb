/*
 * Copyright (c) 2008 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.log;

import org.xtreemfs.babudb.lsmdb.LSN;

/**
 * Simple listener called after a LogEntry was synchronized or sent.
 * 
 * @author bjko
 * @author flangner
 */
public interface SyncListener {
    
    /** Called after the LogEntry was synchronized to log-file.
     * @param lsn
     */
    public void synced(LSN lsn);
    
    /** Called if the LogEntry could not be written/sent.
     */
    public void failed(Exception ex);
    
}
