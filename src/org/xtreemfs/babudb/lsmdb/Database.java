/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.babudb.log.SyncListener;

public interface Database extends DatabaseRO {
    
    /**
     * Returns the name associated with the database.
     * 
     * @return the database name
     */
    public String getName();
    
    /**
     * Creates a new group of inserts.
     * 
     * @return an insert record group
     * @throws BabuDBException
     */
    public BabuDBInsertGroup createInsertGroup() throws BabuDBException;
    
    /**
     * Returns an array of byte range comparators for all indices in the
     * database. The first entry contains the comparator for the first index,
     * the second entry for the second, and so on.
     * 
     * @return an array of byte range comparators
     */
    public ByteRangeComparator[] getComparators();
    
    /**
     * Inserts a single key value pair (synchronously)
     * 
     * @param indexId
     *            index id (0..NumIndices-1)
     * @param key
     *            the key
     * @param value
     *            the value
     * @throws BabuDBException
     *             if the operation failed
     */
    public void syncSingleInsert(int indexId, byte[] key, byte[] value) throws BabuDBException;
    
    /**
     * Inserts a group of key value pair (synchronously)
     * 
     * @param irg
     *            the insert record group to execute
     * @throws BabuDBException
     *             if the operation failed
     */
    public void syncInsert(BabuDBInsertGroup irg) throws BabuDBException;
    
    /**
     * Insert an group of inserts asynchronously.
     * 
     * @param ig
     *            the group of inserts
     * @param listener
     *            a callback for the result
     * @param context
     *            optional context object which is passed to the listener
     * @throws BabuDBException
     */
    public void asyncInsert(BabuDBInsertGroup ig, BabuDBRequestListener listener, Object context)
        throws BabuDBException;
    
    /**
     * Insert an group of inserts in the context of the invoking thread.
     * 
     * @param ig
     *            the group of inserts
     * @throws BabuDBException
     */
    public void directInsert(BabuDBInsertGroup ig) throws BabuDBException;
    
    /**
     * Insert an group of inserts in the context of the invoking thread.
     * 
     * @param ig
     *            the group of inserts
     * @param listener
     *            a customized {@link SyncListener} 
     * @param optimistic
     *            set true, if the insert should be established in memory, which
     *            can verursachen inconsistencies, or false if not.
     * @throws BabuDBException
     */
    public void directInsert(BabuDBInsertGroup ig, SyncListener listener, 
            boolean optimistic) throws BabuDBException;
}
