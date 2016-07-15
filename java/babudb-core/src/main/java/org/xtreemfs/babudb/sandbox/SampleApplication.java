/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.sandbox;

import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.config.ConfigBuilder;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;

/**
 * Sample application demonstrating the replication plugin feature.
 * 
 * @author flangner
 * @since 04/20/2011
 */
public class SampleApplication {

    /**
     * Method to jump in.
     * 
     * @param args
     * @throws BabuDBException if the application fails. 
     */
    public static void main(String[] args) throws BabuDBException {
        
        System.out.println("Setting up the BabuDB instances ... ");
        
        ConfigBuilder builder0 = new ConfigBuilder();
        builder0.setDataPath("/tmp/babudb0", "/tmp/babudb0/log").setLogAppendSyncMode(SyncMode.SYNC_WRITE);
        builder0.addPlugin("myBabuDBPath/pluginConfig0.properties");
        BabuDBConfig config0 = builder0.build();

        ConfigBuilder builder1 = new ConfigBuilder();
        builder1.setDataPath("/tmp/babudb1", "/tmp/babudb1/log").setLogAppendSyncMode(SyncMode.SYNC_WRITE);
        builder1.addPlugin("myBabuDBPath/pluginConfig1.properties");
        BabuDBConfig config1 = builder1.build();

        BabuDB babuDB0 = BabuDBFactory.createBabuDB(config0);
        BabuDB babuDB1 = BabuDBFactory.createBabuDB(config1);
        
        System.out.println("Creating database \"myDatabase\" ... ");
        
        Database myDb = null;
        try {
            myDb = babuDB0.getDatabaseManager().getDatabase("myDatabase");
        } catch (BabuDBException exc) {
            myDb = babuDB0.getDatabaseManager().createDatabase("myDatabase", 1); 
        }
        
        System.out.println("Inserting \"testValue\" for \"testKey\" into \"myDatabase\" ... ");
        
        myDb.singleInsert(0, "testKey".getBytes(), "testValue".getBytes(), null).get();
        
        System.out.println("Retrieving the test values from both BabuDB instance ... ");
        
        byte[] result1 = babuDB1.getDatabaseManager().getDatabase("myDatabase")
                .lookup(0, "testKey".getBytes(), null).get();
        byte[] result0 = babuDB0.getDatabaseManager().getDatabase("myDatabase")
                .lookup(0, "testKey".getBytes(), null).get();
        
        if ("testValue".equals(new String(result0))) {
            System.out.println("Successfully retrieved \"testValue\" from \"myDatabase\" at the " +
            		"first BabuDB instance.");
        } else {
            System.err.println(new String(result0) + " did not match expected \"testValue\"!");
            System.err.println("Running the sample application has caused a failure!");
            System.exit(1);
        }
        
        if ("testValue".equals(new String(result1))) {
            System.out.println("Successfully retrieved \"testValue\" from \"myDatabase\" at the " +
            		"second BabuDB instance.");
        } else {
            System.err.println(new String(result1) + " did not match expected \"testValue\"!");
            System.err.println("Running the sample application has caused a failure!");
            System.exit(1);
        }
        
        System.out.println("Shutting down the BabuDB instances ... ");
        
        babuDB1.shutdown();
        babuDB0.shutdown();
        
        System.out.println("The sample application has been executed successfully.");
        System.exit(0);
    }
}
