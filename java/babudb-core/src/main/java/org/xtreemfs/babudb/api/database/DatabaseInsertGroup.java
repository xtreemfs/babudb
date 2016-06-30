/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.database;

/**
 * Interface of database operations that may be grouped and executed together.
 * 
 * @author flangner
 * @since 11/03/2010
 */
public interface DatabaseInsertGroup {

    /**
     * Add a new insert operation to this group. Be aware of unpredictable behavior if a 
     * key-value pair is manipulated twice within the same insert group.
     * 
     * @param indexId - the index in which the key-value pair is inserted.
     * @param key - the key.
     * @param value - the value data.
     */
    public void addInsert(int indexId, byte[] key, byte[] value);

    /**
     * Add a new delete operation to this group. Be aware of unpredictable behavior if a 
     * key-value pair is manipulated twice within the same insert group.
     * 
     * @param indexId - in which the key-value pair is located.
     * @param key - of the key-value pair to delete. 
     */
    public void addDelete(int indexId, byte[] key);
}