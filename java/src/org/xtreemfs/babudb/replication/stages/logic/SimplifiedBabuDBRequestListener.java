/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequestListener;

/**
 * <p>Simplifies the {@link BabuDBRequestListener} interface, to it's
 * basic operation.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

abstract class SimplifiedBabuDBRequestListener implements
        BabuDBRequestListener {

    /**
     * The simplified listener function.
     * 
     * @param context
     * @param error
     */
    abstract void finished(Object context, BabuDBException error);
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#insertFinished(java.lang.Object)
     */
    @Override
    public void insertFinished(Object context) {
        this.finished(context, null);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#lookupFinished(java.lang.Object, byte[])
     */
    @Override
    public void lookupFinished(Object context, byte[] value) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#prefixLookupFinished(java.lang.Object, java.util.Iterator)
     */
    @Override
    public void prefixLookupFinished(Object context,
            Iterator<Entry<byte[], byte[]>> iterator) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#requestFailed(java.lang.Object, org.xtreemfs.babudb.BabuDBException)
     */
    @Override
    public void requestFailed(Object context, BabuDBException error) {
        this.finished(context, error);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBRequestListener#userDefinedLookupFinished(java.lang.Object, java.lang.Object)
     */
    @Override
    public void userDefinedLookupFinished(Object context, Object result) {
        throw new UnsupportedOperationException();
    }
}