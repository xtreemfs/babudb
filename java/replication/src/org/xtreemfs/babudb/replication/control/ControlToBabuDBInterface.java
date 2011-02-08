/*
 * Copyright (c) 2010, Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this 
 * list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * Neither the name of the Konrad-Zuse-Zentrum fuer Informationstechnik Berlin 
 * nor the names of its contributors may be used to endorse or promote products 
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.control;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 *
 * <br>15.04.2010
 */
public interface ControlToBabuDBInterface extends RoleChangeListener{

    /**
     * Performs a valid replicate-operation on all available slaves.
     * 
     * @param le
     * @param buffer
     * 
     * @return the {@link ReplicateResponse} future.
     */
    public ReplicateResponse replicate(LogEntry le, ReusableBuffer buffer);

    /**
     * <p>
     * Registers the listener for a replicate call.
     * </p>
     * 
     * @param response
     */
    public void subscribeListener(ReplicateResponse rp);

    /**
     * @return true, if this server has the lease and is not suspended, 
     *         false otherwise.
     */
    public boolean hasLease();

    /**
     * @return the address of the current lease holder, or null if the lease is 
     *         not available at the moment.
     */
    public InetSocketAddress getLeaseHolder();
    
    /**
     * @param master - the address to compare with.
     * @return true, if the given master address is the address of this server. false otherwise.
     */
    public boolean amIMaster(InetSocketAddress master);
}