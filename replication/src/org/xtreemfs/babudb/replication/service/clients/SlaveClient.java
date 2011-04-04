/*
 * Copyright (c) 2010-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.clients;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Client to access services of a server that is currently slave for a master.
 * 
 * @author flangner
 * @since 04/12/2010
 */
public interface SlaveClient extends ClientInterface {

    /**
     * The slave is requested to replicate the given LogEntry identified by its
     * {@link org.xtreemfs.babudb.lsmdb.LSN}. The buffer will be freed afterwards.
     * 
     * @param lsn
     * @param data
     * @return the {@link ClientResponseFuture}.
     */
    public ClientResponseFuture<Object, ErrorCodeResponse> replicate(LSN lsn, ReusableBuffer data);
}