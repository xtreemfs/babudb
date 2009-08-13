package org.xtreemfs.babudb.sandbox;

import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.include.common.config.SlaveConfig;

public class slave {
    public static void main(String[] args) throws Exception {
        SlaveConfig sConf = new SlaveConfig("config/slave.properties");
        sConf.read();
        BabuDBFactory.createSlaveBabuDB(sConf);
        
        Thread.sleep(1000000);
    }
}
