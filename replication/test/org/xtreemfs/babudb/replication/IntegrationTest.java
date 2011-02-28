/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;


import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.exception.BabuDBException;
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
public class IntegrationTest {

    private static Main main;
    
    private BabuDBInternal mock0;
    private BabuDBInternal repl0;
    
    private BabuDBInternal mock1;
    private BabuDBInternal repl1;
    
    private BabuDBInternal mock2;
    private BabuDBInternal repl2;
    
    private final AtomicBoolean finishingLock = new AtomicBoolean(false);
    
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
        
    	FSUtils.delTree(new File(mock0Conf.getBaseDir()));
    	FSUtils.delTree(new File(mock1Conf.getBaseDir()));
    	FSUtils.delTree(new File(mock2Conf.getBaseDir()));
        FSUtils.delTree(new File(mock0Conf.getDbLogDir()));
        FSUtils.delTree(new File(mock1Conf.getDbLogDir()));
        FSUtils.delTree(new File(mock2Conf.getDbLogDir()));
    	
        // starting three local BabuDB services supporting replication; based on mock databases
        ReplicationConfig conf = new ReplicationConfig("config/replication_server0.test", mock0Conf);
        FSUtils.delTree(new File(conf.getTempDir()));
        mock0 = new BabuDBMock(conf.getAddress().toString() + ":" + conf.getPort(), mock0Conf);
        repl0 = main.start(mock0, "config/replication_server0.test");
        
        conf = new ReplicationConfig("config/replication_server1.test", mock1Conf);
        FSUtils.delTree(new File(conf.getTempDir()));
        mock1 = new BabuDBMock(conf.getAddress().toString() + ":" + conf.getPort(), mock1Conf);
        repl1 = main.start(mock1, "config/replication_server1.test");
        
        conf = new ReplicationConfig("config/replication_server2.test", mock2Conf);
        FSUtils.delTree(new File(conf.getTempDir()));
        mock2 = new BabuDBMock(conf.getAddress().toString() + ":" + conf.getPort(), mock2Conf);
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
        Database test0 = repl0.getDatabaseManager().createDatabase("test0", 3);    
        Database test1 = repl1.getDatabaseManager().createDatabase("test1", 3);
        Database test2 = repl2.getDatabaseManager().createDatabase("test2", 3);
        
        // retrieve the databases
        test0 = repl2.getDatabaseManager().getDatabase("test0");
        test1 = repl0.getDatabaseManager().getDatabase("test1");
        test2 = repl1.getDatabaseManager().getDatabase("test2");
        
        // make some inserts
        DatabaseInsertGroup ig = test0.createInsertGroup();
        ig.addInsert(0, "bla00".getBytes(), "blub00".getBytes());
        ig.addInsert(1, "bla01".getBytes(), "blub01".getBytes());
        ig.addInsert(2, "bla02".getBytes(), "blub02".getBytes());
        test0.insert(ig, main).registerListener(new DatabaseRequestListener<Object>() {
            
            @Override
            public void finished(Object result, Object context) {
                assertEquals(main, context);
            
                finish();
            }
            
            @Override
            public void failed(BabuDBException error, Object context) {
                fail("Insert failed!");
                
                finish();
            }
        });
        
        waitForFinish();
        
        ig = test1.createInsertGroup();
        ig.addInsert(0, "bla10".getBytes(), "blub10".getBytes());
        ig.addInsert(1, "bla11".getBytes(), "blub11".getBytes());
        ig.addInsert(2, "bla12".getBytes(), "blub12".getBytes());
        test1.insert(ig, main).registerListener(new DatabaseRequestListener<Object>() {
            
            @Override
            public void finished(Object result, Object context) {
                assertEquals(main, context);
                
                finish();
            }
            
            @Override
            public void failed(BabuDBException error, Object context) {
                fail("Insert failed!");
                
                finish();
            }
        });
        
        waitForFinish();
        
        ig = test2.createInsertGroup();
        ig.addInsert(0, "bla20".getBytes(), "blub20".getBytes());
        ig.addInsert(1, "bla21".getBytes(), "blub21".getBytes());
        ig.addInsert(2, "bla22".getBytes(), "blub22".getBytes());
        test2.insert(ig, main).registerListener(new DatabaseRequestListener<Object>() {
            
            @Override
            public void finished(Object result, Object context) {
                assertEquals(main, context);
                
                finish();
            }
            
            @Override
            public void failed(BabuDBException error, Object context) {
                fail("Insert failed!");
                
                finish();
            }
        });
        
        waitForFinish();
        
        // retrieve the databases
        test0 = repl1.getDatabaseManager().getDatabase("test0");
        test1 = repl2.getDatabaseManager().getDatabase("test1");
        test2 = repl0.getDatabaseManager().getDatabase("test2");
        
        // make some lookups
        test0.lookup(0, "bla00".getBytes(), main).registerListener(new DatabaseRequestListener<byte[]>() {
            
            @Override
            public void finished(byte[] result, Object context) {
                assertEquals(main, context);
                assertEquals("blub00", new String(result));
                
                finish();
            }
            
            @Override
            public void failed(BabuDBException error, Object context) {
                fail("Lookup failed!");
                
                finish();
            }
        });
        
        waitForFinish();
        
        test0.lookup(0, "bla20".getBytes(), main).registerListener(new DatabaseRequestListener<byte[]>() {
            
            @Override
            public void finished(byte[] result, Object context) {
                fail("Lookup succeeded but had to fail!");
                
                finish();
            }
            
            @Override
            public void failed(BabuDBException error, Object context) {
                assertEquals(main, context);
                
                finish();
            }
        });
        
        waitForFinish();
    }
    
    private void waitForFinish() throws InterruptedException {
        synchronized (finishingLock) {
            if (!finishingLock.get())
                finishingLock.wait();
            
            finishingLock.set(false);
        }
    }
    
    private final void finish() {
        synchronized (finishingLock) {
            finishingLock.set(true);
            finishingLock.notify();
        }
    }
}
