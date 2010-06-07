package org.xtreemfs.babudb.sandbox;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseManager;

public class StressTest {
    
    static final int NUM_READS   = 1000000;
    
    static final int NUM_INSERTS = 10000000;
    
    static final int STEP        = 10000;
    
    public static byte[] getBytes(double d) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putDouble(d);
        return bb.array();
    }
    
    public static byte[] getBytes(int i) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        return bb.array();
    }
    
    public static double fromBytes(byte[] bytes) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return bb.getDouble();
    }
    
    public static void main(String[] args) throws Exception {
        
        final Object lock = new Object();
        
        final int numDBs = 5;
        final int numIndices = 1;
        final int numThreads = 1;
        final int maxLogFileSize = 1024 * 1024 * 16;
        // String dbDir = "/scratch/disk1/dbtest";
        final String dbDir = "c:/temp/dbtest";
        
        final int minKeySize = 5;
        final int maxKeySize = 40;
        final int minValSize = 5;
        final int maxValSize = 16384;
        
        // FSUtils.delTree(new File(dbDir));
        
        // start the database
        final BabuDB databaseSystem = BabuDBFactory.createBabuDB(new BabuDBConfig(dbDir, dbDir, numThreads,
                maxLogFileSize, 10, SyncMode.ASYNC, 1000, 0, false, 16, 1024 * 1024 * 256), null);
        DatabaseManager dbm = databaseSystem.getDatabaseManager();
        
        // for (int i = 0; i < numDBs; i++)
        // dbm.createDatabase("DB" + i, numIndices);
        dbm.createDatabase("blub", numIndices);
        
        final Database[] dbs = new Database[numDBs];
        for (int i = 0; i < dbs.length; i++)
            dbs[i] = dbm.getDatabase("DB" + i);
        
        Thread writeThread = new Thread() {
            
            public void run() {
                
                try {
                    
                    System.out.println("starting write thread...");
                    
                    // insert a random entry in a random database
                    
                    for (int i = 0; i < NUM_INSERTS; i++) {
                        
                        int db = (int) (Math.random() * numDBs);
                        byte[] key = randomBytes(minKeySize, maxKeySize);
                        byte[] val = randomBytes(minValSize, maxValSize);
                        int index = (int) (Math.random() * numIndices);
                        
                        dbs[db].singleInsert(index, key, val, null).get();
                        
                        if (i % 1000 == 0) {
                            System.out.print(".");
                            Thread.sleep(5000);
                        }
                        
                        if (i % 10000 == 9999) {
                            System.out.println("\ncheckpoint...");
                            databaseSystem.getCheckpointer().checkpoint();
                            System.out.println("done");
                        }
                        
                        if (i % 40000 == 39999)
                            System.exit(0);
                        
                    }
                    
                    System.out.println("write thread finished");
                    
                } catch (Throwable th) {
                    th.printStackTrace();
                }
            }
        };
        
        Thread readThread = new Thread() {
            
            // public void run() {
            //
            // try {
            //
            // System.out.println("starting read thread...");
            //
            // // insert a random entry in a random database
            //
            // for (int i = 0; i < NUM_READS; i++) {
            //
            // int db = (int) (Math.random() * numDBs);
            // byte[] key = new byte[0];
            // int index = (int) (Math.random() * numIndices);
            //
            // Iterator<Entry<byte[], byte[]>> it = dbs[db]
            // .prefixLookup(index, key, null).get();
            //
            // synchronized (lock) {
            // while (it.hasNext())
            // it.next();
            // }
            // }
            //
            // System.out.println("read thread finished");
            //
            // } catch (Throwable th) {
            // th.printStackTrace();
            // }
            // }
        };
        
        writeThread.start();
        readThread.start();
        
        writeThread.join();
        readThread.join();
        
        // create a checkpoint for faster start-ups
        // databaseSystem.getCheckpointer().checkpoint();
        
        // shutdown database
        databaseSystem.shutdown();
        
    }
    
    private static byte[] randomBytes(int minLen, int maxLen) {
        int len = (int) (Math.random() * (maxLen - minLen)) + minLen;
        byte[] buf = new byte[len];
        for (int i = 0; i < buf.length; i++)
            buf[i] = (byte) (Math.random() * 16384);
        
        return buf;
    }
}
