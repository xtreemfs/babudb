/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequestResult;
import org.xtreemfs.babudb.index.ByteRangeComparator;

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
     * Inserts a single key value pair.
     * 
     * @param indexId
     *            index id (0..NumIndices-1)
     * @param key
     *            the key
     * @param value
     *            the value
     * @param context
     *            arbitrary context which is passed to the listener
     * @return a future as proxy for the request result.
     */
    public BabuDBRequestResult<Object> singleInsert(int indexId, byte[] key, 
            byte[] value, Object context);
    
    /**
     * Inserts a group of key value pairs.
     * 
     * @param irg
     *            the insert record group to execute
     * @param context
     *            arbitrary context which is passed to the listener
     * @return a future as proxy for the request result.
     */
    public BabuDBRequestResult<Object> insert(BabuDBInsertGroup irg, 
            Object context);
}
