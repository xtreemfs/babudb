/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBImpl;

public class DummyReplication extends Replication {
    
    public DummyReplication(BabuDBImpl  babu, int Port) throws BabuDBException{       
        super(new InetSocketAddress(0), null, Port, babu,0,4000,null,Replication.DEFAULT_NO_TRIES);
    }
    
    @Override
    boolean isDesignatedMaster(InetSocketAddress potMaster) {
        return true;
    }
    
    @Override
    boolean isDesignatedSlave(InetSocketAddress potSlave) {
        return true;
    }
}
