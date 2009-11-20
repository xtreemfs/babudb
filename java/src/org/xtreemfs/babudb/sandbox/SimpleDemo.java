/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb.sandbox;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseManager;
import org.xtreemfs.include.common.config.BabuDBConfig;

public class SimpleDemo {

    public static void main(String[] args) throws InterruptedException {
        try {           
            //start the database
            BabuDB databaseSystem = BabuDBFactory.createBabuDB(new BabuDBConfig("myDatabase/", 
                    "myDatabase/", 2, 1024 * 1024 * 16, 5 * 60, SyncMode.SYNC_WRITE,0,0, 
                    false));
            DatabaseManager dbm = databaseSystem.getDatabaseManager();
                        
            //create a new database called myDB
            dbm.createDatabase("myDB", 2);
            Database db = dbm.getDatabase("myDB");
            
            //create an insert group for atomic inserts
            BabuDBInsertGroup group = db.createInsertGroup();

            //insert one key in each index
            group.addInsert(0, "Key1".getBytes(), "Value1".getBytes());
            group.addInsert(1, "Key2".getBytes(), "Value2".getBytes());

            //and execute group insert
            db.insert(group,null).get();

            //now do a lookup
            byte[] result = db.lookup(0, "Key1".getBytes(),null).get();

            //create a checkpoint for faster start-ups
            databaseSystem.getCheckpointer().checkpoint();

            //shutdown database
            databaseSystem.shutdown();
        } catch (BabuDBException ex) {
            ex.printStackTrace();
        }
    }  
}
