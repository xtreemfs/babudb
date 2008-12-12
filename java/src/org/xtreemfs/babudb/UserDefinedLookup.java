/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb;

import org.xtreemfs.babudb.lsmdb.LSMLookupInterface;

/**
 * This interface can be used to execute complex lookup
 * routines in the thread of the database worker.
 * @author bjko
 */
public interface UserDefinedLookup {

    /**
     * The method which is executed by the worker thread.
     * @param database direct access to synchronous database lookups
     * @return the result which is passed on to the listener
     * @throws org.xtreemfs.babudb.BabuDBException in case of an error, is passed to the listener
     */
    public Object execute(final LSMLookupInterface database) throws BabuDBException;
    
}
