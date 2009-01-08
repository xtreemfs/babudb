/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb.sandbox;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBInsertGroup;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;

public class SimpleDemo {

    public static void main(String[] args) {
        try {
            //start the database
            BabuDB database = new BabuDB("myDatabase/", "myDatabase/", 2, 1024 * 1024 * 16, 5 * 60, SyncMode.SYNC_WRITE,0,0);

            //create a new database called myDB
            database.createDatabase("myDB", 2);

            //create an insert group for atomic inserts
            BabuDBInsertGroup group = database.createInsertGroup("myDB");

            //insert one key in each index
            group.addInsert(0, "Key1".getBytes(), "Value1".getBytes());
            group.addInsert(1, "Key2".getBytes(), "Value2".getBytes());

            //and execute group insert
            database.syncInsert(group);

            //now do a lookup
            byte[] result = database.syncLookup("myDB", 0, "Key1".getBytes());

            //create a checkpoint for faster start-ups
            database.checkpoint();

            //shutdown database
            database.shutdown();
        } catch (BabuDBException ex) {
            ex.printStackTrace();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        
    }  
}
