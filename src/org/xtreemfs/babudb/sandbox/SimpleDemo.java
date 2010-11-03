/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb.sandbox;

import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;

public class SimpleDemo {
    
    public static void main(String[] args) throws InterruptedException {
        try {
            // start the database
            BabuDB databaseSystem = BabuDBFactory.createBabuDB(new BabuDBConfig("myDatabase/", "myDatabase/",
                2, 1024 * 1024 * 16, 5 * 60, SyncMode.SYNC_WRITE, 0, 0, false, 16, 1024 * 1024 * 512),null);
            DatabaseManager dbm = databaseSystem.getDatabaseManager();
            
            // create a new database called myDB
            dbm.createDatabase("myDB", 2);
            Database db = dbm.getDatabase("myDB");
            
            // create an insert group for atomic inserts
            DatabaseInsertGroup group = db.createInsertGroup();
            
            // insert one key in each index
            group.addInsert(0, "Key1".getBytes(), "Value1".getBytes());
            group.addInsert(1, "Key2".getBytes(), "Value2".getBytes());
            
            // and execute group insert
            db.insert(group, null).get();
            
            // now do a lookup
            byte[] result = db.lookup(0, "Key1".getBytes(), null).get();
            
            // create a checkpoint for faster start-ups
            databaseSystem.getCheckpointer().checkpoint();
            
            // shutdown database
            databaseSystem.shutdown();
        } catch (BabuDBException ex) {
            ex.printStackTrace();
        }
    }
}
