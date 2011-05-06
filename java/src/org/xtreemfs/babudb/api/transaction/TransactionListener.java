/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.api.transaction;

/**
 * A listener that observes lightweight BabuDB transactions.
 * 
 * @author stenjan
 * 
 */
public interface TransactionListener {
    
    /**
     * The method is invoked with each transaction that was successfully executed.
     * 
     * @param txn the executed transaction
     */
    public void transactionPerformed(Transaction txn);
        
}
