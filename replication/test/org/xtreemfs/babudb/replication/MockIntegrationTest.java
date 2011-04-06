/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.mock.BabuDBMock;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;
import org.xtreemfs.foundation.util.FSUtils;

import static junit.framework.Assert.*;
import static org.xtreemfs.babudb.replication.TestParameters.*;

/**
 * @author flangner
 * @since 02/20/2011
 */
public class MockIntegrationTest {

    private static Main main;
    
    private BabuDBInternal mock0;
    private BabuDBInternal repl0;
    
    private BabuDBInternal mock1;
    private BabuDBInternal repl1;
    
    private BabuDBInternal mock2;
    private BabuDBInternal repl2;
        
    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Logging.start(Logging.LEVEL_WARN, Category.all);
        
        main = new Main();
        assertEquals("0.5.0-0.5.0", main.compatibleBabuDBVersion());
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {   
        
    	FSUtils.delTree(new File(conf0.getBaseDir()));
    	FSUtils.delTree(new File(conf1.getBaseDir()));
    	FSUtils.delTree(new File(conf2.getBaseDir()));
        FSUtils.delTree(new File(conf0.getDbLogDir()));
        FSUtils.delTree(new File(conf1.getDbLogDir()));
        FSUtils.delTree(new File(conf2.getDbLogDir()));
    	
        // starting three local BabuDB services supporting replication; based on mock databases
        ReplicationConfig conf = new ReplicationConfig("config/replication_server0.test", conf0);
        FSUtils.delTree(new File(conf.getTempDir()));
        mock0 = new BabuDBMock(conf.getAddress().toString() + ":" + conf.getPort(), conf0);
        repl0 = main.start(mock0, "config/replication_server0.test");
        
        conf = new ReplicationConfig("config/replication_server1.test", conf1);
        FSUtils.delTree(new File(conf.getTempDir()));
        mock1 = new BabuDBMock(conf.getAddress().toString() + ":" + conf.getPort(), conf1);
        repl1 = main.start(mock1, "config/replication_server1.test");
        
        conf = new ReplicationConfig("config/replication_server2.test", conf2);
        FSUtils.delTree(new File(conf.getTempDir()));
        mock2 = new BabuDBMock(conf.getAddress().toString() + ":" + conf.getPort(), conf2);
        repl2 = main.start(mock2, "config/replication_server2.test");
        
        // initialization
        repl0.init(null);
        repl1.init(null);
        repl2.init(null);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        
        // shutting down the services
        repl0.shutdown();
        repl1.shutdown();
        repl2.shutdown();
    }

    /**
     * @throws Exception
     */
    @Test
    public void testBasicIO() throws Exception {
        
     // create some DBs
        Database test0 = repl0.getDatabaseManager().createDatabase("0", 3);    
        Database test1 = repl1.getDatabaseManager().createDatabase("1", 3);
        Database test2 = repl2.getDatabaseManager().createDatabase("2", 3);
                
        // retrieve the databases
        test0 = repl2.getDatabaseManager().getDatabase("0");
        test1 = repl0.getDatabaseManager().getDatabase("1");
        test2 = repl1.getDatabaseManager().getDatabase("2");
        
        // make some inserts
        DatabaseInsertGroup ig = test0.createInsertGroup();
        ig.addInsert(0, "bla00".getBytes(), "blub00".getBytes());
        ig.addInsert(1, "bla01".getBytes(), "blub01".getBytes());
        ig.addInsert(2, "bla02".getBytes(), "blub02".getBytes());
        test0.insert(ig, test0).get();
        
        ig = test1.createInsertGroup();
        ig.addInsert(0, "bla10".getBytes(), "blub10".getBytes());
        ig.addInsert(1, "bla11".getBytes(), "blub11".getBytes());
        ig.addInsert(2, "bla12".getBytes(), "blub12".getBytes());
        test1.insert(ig, test1).get();
        
        ig = test2.createInsertGroup();
        ig.addInsert(0, "bla20".getBytes(), "blub20".getBytes());
        ig.addInsert(1, "bla21".getBytes(), "blub21".getBytes());
        ig.addInsert(2, "bla22".getBytes(), "blub22".getBytes());
        test2.insert(ig, test2).get();
        
        // retrieve the databases
        test0 = repl1.getDatabaseManager().getDatabase("0");
        test1 = repl2.getDatabaseManager().getDatabase("1");
        test2 = repl0.getDatabaseManager().getDatabase("2");
        
        // make some lookups
        byte[] res = test0.lookup(0, "bla00".getBytes(), test0).get();
        assertNotNull(res);
        assertEquals("blub00", new String(res));
        
        assertNull(test0.lookup(0, "bla20".getBytes(), test0).get());
    }
}
