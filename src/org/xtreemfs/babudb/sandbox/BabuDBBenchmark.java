/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb.sandbox;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.common.logging.Logging;

/**
 *
 * @author bjko
 */
public class BabuDBBenchmark {

    public static final String VER = "0.1";

    private final static byte[] CHARS;

    static {
        int charptr = 0;
        CHARS = new byte[10+26+26];
        for (int i = 48; i < 58; i++)
            CHARS[charptr++] = (byte)i;
        for (int i = 65; i < 91; i++)
            CHARS[charptr++] = (byte)i;
        for (int i = 97; i < 122; i++)
            CHARS[charptr++] = (byte)i;
    }

    private final int numKeys;
    private final int minKeyLength;
    private final int maxKeyLength;

    private final byte[] payload;

    private final BabuDB database;

    public BabuDBBenchmark(String dbDir, SyncMode syncMode, int numKeys, int valueLength, int minKeyLength, int maxKeyLength) throws BabuDBException {
        this.numKeys = numKeys;
        this.minKeyLength = minKeyLength;
        this.maxKeyLength = maxKeyLength;

        payload = new byte[valueLength];
        for (int i = 0; i < valueLength; i++)
            payload[i] = 'v';

        if (numKeys > Math.pow(CHARS.length, maxKeyLength))
            throw new IllegalArgumentException(maxKeyLength+" is too short to create enough unique keys for "+numKeys+" keys");

        //use one worker because we use one database
        database = new BabuDB(dbDir, dbDir, 1, 1, 0, syncMode, 200, 500);
        database.createDatabase("testdb", 1);

        System.out.println("initialized BabuDBBenchmark version "+VER);
        System.out.println("database directory: "+dbDir);
        System.out.println("fsync disabled:     "+syncMode.name());
        System.out.println("min/max key length: "+minKeyLength+"/"+maxKeyLength);
        System.out.println("number of workers:  1");
        System.out.println("auto checkpointing: disabled");
        System.out.println("value size:         "+valueLength);
    }

    public void performInserts() throws BabuDBException {
        long tStart = System.currentTimeMillis();
        for (int i = 0; i < numKeys; i++) {
            byte[] key = createRandomKey();
            database.syncSingleInsert("testdb", 0, key, payload);
        }
        long tEnd = System.currentTimeMillis();
        double dur = (tEnd-tStart);
        System.out.format("insert of %d keys took %.4f seconds\n",numKeys,dur/1000.0);
        System.out.format("       = %.4f s/key\n",(dur/1000.0)/((double)numKeys));
    }

    public void checkpoint() throws BabuDBException, InterruptedException {
        database.checkpoint();
    }

    public void shutdown() {
        database.shutdown();
    }

    private byte[] createRandomKey() {
        final int length = (int) (Math.random() * ((double)(maxKeyLength - minKeyLength)) + minKeyLength);
        assert(length >= minKeyLength);
        assert(length <= maxKeyLength);

        byte[] key = new byte[length];
        for (int i = 0; i < length; i++)
            key[i] = CHARS[(int)Math.random()*(CHARS.length-1)];
        return key;
    }

    public static void main(String[] args) {
        try {
            Logging.start(Logging.LEVEL_WARN);
            BabuDBBenchmark benchmark = new BabuDBBenchmark("/tmp/babubench", SyncMode.FDATASYNC, 10000, 50, 2, 20);
            benchmark.performInserts();
            benchmark.shutdown();
        } catch (Exception ex) {
            System.out.println("FAILED!!!");
            ex.printStackTrace();
        }
    }

}
