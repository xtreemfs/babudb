package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory.BabuDBImpl;

public class DummyReplication extends Replication {
    
    public DummyReplication(BabuDBImpl  babu, int Port) throws BabuDBException{
        super(new InetSocketAddress(0), null, Port, babu,SYNC_MODUS.SYNC);
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
