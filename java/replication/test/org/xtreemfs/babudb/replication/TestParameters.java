/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.config.ConfigBuilder;
import org.xtreemfs.babudb.config.ReplicationConfig;

/**
 * Default parameters for the replication TestSuite.
 * 
 * @author flangner
 * @since 02/25/2011
 */
public final class TestParameters {
    
    private TestParameters() { }
    
    public final static int MAX_PARTICIPANTS    = 20;
    public final static int MIN_PARTICIPANTS    = 1;
    public final static int MAX_Q               = 100;
    public final static int RQ_TIMEOUT          = ReplicationConfig.REQUEST_TIMEOUT;
    public final static int CON_TIMEOUT         = ReplicationConfig.CONNECTION_TIMEOUT;
    public final static int TIMESYNC_GLOBAL     = 3 * 1000;
    public final static int TIMESYNC_LOCAL      = 3 * 1000;
    
    public final static BabuDBConfig conf0 = new ConfigBuilder()
            .setDataPath("/tmp/babudb0/base", "/tmp/babudb0/log")
            .putPlugin("dist/Replication-1.0.0_0.5.0.jar", "config/replication_server0.test")
            .build();
    
    public final static BabuDBConfig conf1 = new ConfigBuilder()
            .setDataPath("/tmp/babudb1/base", "/tmp/babudb1/log")
            .putPlugin("dist/Replication-1.0.0_0.5.0.jar", "config/replication_server1.test")
            .build();

    public final static BabuDBConfig conf2 = new ConfigBuilder()
            .setDataPath("/tmp/babudb2/base", "/tmp/babudb2/log")
            .putPlugin("dist/Replication-1.0.0_0.5.0.jar", "config/replication_server2.test")
            .build();
}
