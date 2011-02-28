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

/**
 * @author flangner
 * @since 02/25/2011
 */
public final class TestParameters {
    
    private TestParameters() { }
    
    public final static BabuDBConfig mock0Conf = new ConfigBuilder().setDataPath(
            "/tmp/babudb0/base", "/tmp/babudb0/log").build();
    
    public final static BabuDBConfig mock1Conf = new ConfigBuilder().setDataPath(
            "/tmp/babudb1/base", "/tmp/babudb1/log").build();

    public final static BabuDBConfig mock2Conf = new ConfigBuilder().setDataPath(
            "/tmp/babudb2/base", "/tmp/babudb2/log").build();
}
