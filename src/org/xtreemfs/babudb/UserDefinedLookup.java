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

    public Object execute(final LSMLookupInterface database) throws BabuDBException;
    
}
