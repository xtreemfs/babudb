/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.concurrent.locks.ReentrantLock;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBImpl;

public class DummyReplication extends Replication {
    
    public DummyReplication(int Port) throws BabuDBException{       
        super(new InetSocketAddress(0), null, Port, new BabuDBImpl(),0,4000,new ReentrantLock(),Replication.DEFAULT_NO_TRIES);
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
