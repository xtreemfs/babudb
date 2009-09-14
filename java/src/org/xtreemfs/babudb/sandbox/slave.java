package org.xtreemfs.babudb.sandbox;

import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.include.common.config.ReplicationConfig;

public class slave {
    public static void main(String[] args) throws Exception {
        ReplicationConfig sConf = new ReplicationConfig("config/replication.properties");
        sConf.read();
        BabuDBFactory.createReplicatedBabuDB(sConf);
        
        Thread.sleep(1000000);
    }
}
