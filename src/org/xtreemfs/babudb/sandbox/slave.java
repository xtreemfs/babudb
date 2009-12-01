package org.xtreemfs.babudb.sandbox;

import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.config.ReplicationConfig;

public class slave {
    public static void main(String[] args) throws Exception {
        ReplicationConfig sConf = new ReplicationConfig("config/replication.properties");
        BabuDBFactory.createReplicatedBabuDB(sConf);
        
        Thread.sleep(1000000);
    }
}
