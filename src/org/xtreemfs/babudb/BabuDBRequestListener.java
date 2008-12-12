/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 *
 * @author bjko
 */
public interface BabuDBRequestListener {

    /**
     * Method is called when a worker has finished processing the request.
     * @param request the request with the response data
     * @param error the exception, null if the operation was successful
     */
    public void insertFinished(Object context);
    
    public void lookupFinished(Object context, byte[] value);
    
    public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator);
    
    public void userDefinedLookupFinished(Object context, Object result);
    
    public void requestFailed(Object context, BabuDBException error);
}
