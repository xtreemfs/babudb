/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
*/

package org.xtreemfs.babudb.sandbox;

import java.util.Iterator;
import java.util.Map.Entry;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.UserDefinedLookup;
import org.xtreemfs.babudb.lsmdb.LSMLookupInterface;

/**
 *
 * @author bjko
 */
public class BenchmarkWorkerThread extends Thread {

    public static enum BenchmarkOperation {
        INSERT,
        ITERATE,
        LOOKUP,
        UDL,
        DIRECT_LOOKUP
    };

    private final int numKeys;

    private final int id;

    private final int minKeyLength;

    private final int maxKeyLength;

    private final byte[] payload;

    private final BabuDB database;

    private final BenchmarkOperation operation;

    private volatile boolean done;

    private volatile Exception error;

    private volatile double    throughput;

    private final String dbName;

    public BenchmarkWorkerThread(int id, int numKeys, int minKeyLength,
            int maxKeyLength, byte[] payload, BabuDB database, BenchmarkOperation operation) {
        this.id = id;
        this.numKeys = numKeys;
        this.minKeyLength = minKeyLength;
        this.maxKeyLength = maxKeyLength;
        this.payload = payload;
        this.database = database;
        this.operation = operation;
        this.dbName = Integer.toString(id);
        error = null;
        done = false;
    }

    public void run() {
        try {
            switch (operation) {
                case INSERT : performInserts(); break;
                case LOOKUP : performLookups(); break;
                case DIRECT_LOOKUP : performDirectLookups(); break;
                case ITERATE : countKeys(); break;
                case UDL : performUserDefinedLookups(); break;
            }
        } catch (Exception ex) {
            error = ex;
        }
        synchronized (this) {
            done = true;
            this.notifyAll();
        }

    }

    public double waitForResult() throws Exception {
        synchronized (this) {
            if (!done) {
               this.wait();
            }
            if (error != null) {
                throw error;
            }
            return throughput;
        }
    }

    public void performInserts() throws BabuDBException {
        long tStart = System.currentTimeMillis();
        byte[] key = null;
        for (int i = 0; i < numKeys; i++) {
            key = createRandomKey();
            database.syncSingleInsert(dbName, 0, key, payload);
        }
        long tEnd = System.currentTimeMillis();
        double dur = (tEnd-tStart);

        String output = String.format("   thread #%3d: insert of %d keys took %.4f seconds\n", id ,numKeys,dur/1000.0);
        output += String.format("   thread #%3d:       = %10.4f s/key\n",id,(dur/1000.0)/((double)numKeys));
        output += String.format("   thread #%3d:       = %10.4f keys/s\n\n",id,((double)numKeys)/(dur/1000.0));

        throughput = ((double)numKeys)/(dur/1000.0);

        System.out.print(output);
    }

    public void countKeys() throws BabuDBException {
        long tStart = System.currentTimeMillis();
        Iterator<Entry<byte[],byte[]>> iter = database.syncPrefixLookup(dbName, 0, new byte[]{});
        int count = 0;
        while (iter.hasNext()) {
            Entry<byte[],byte[]> e = iter.next();
            count++;
        }
        long tEnd = System.currentTimeMillis();
        double dur = (tEnd-tStart);
        String output = String.format("   thread #%3d: iterating over %d keys took %.4f seconds\n\n",id,count,dur/1000.0);

        throughput = ((double)count)/(dur/1000.0);

        System.out.print(output);
    }

    public void performLookups() throws BabuDBException {
        long tStart = System.currentTimeMillis();
        byte[] key = null;
        int hits = 0;
        for (int i = 0; i < numKeys; i++) {
            key = createRandomKey();
            byte[] value = database.syncLookup(dbName, 0, key);
            if (value != null)
                hits++;
        }
        long tEnd = System.currentTimeMillis();
        double dur = (tEnd-tStart);
        
        String output = String.format("   thread #%3d: lookup of %d keys took %.4f seconds\n",id,numKeys,dur/1000.0);
        output += String.format("   thread #%3d:       = %10.4f s/key\n",id,(dur/1000.0)/((double)numKeys));
        output += String.format("   thread #%3d:       = %10.4f keys/s\n",id,((double)numKeys)/(dur/1000.0));
        output += String.format("   thread #%3d: hit-rate %6.2f%%\n\n",id,(hits*100.0)/(double)numKeys);

        throughput = ((double)numKeys)/(dur/1000.0);

        System.out.print(output);
    }

    public void performDirectLookups() throws BabuDBException {
        long tStart = System.currentTimeMillis();
        byte[] key = null;
        int hits = 0;
        for (int i = 0; i < numKeys; i++) {
            key = createRandomKey();
            byte[] value = database.directLookup(dbName, 0, key);
            if (value != null)
                hits++;
        }
        long tEnd = System.currentTimeMillis();
        double dur = (tEnd-tStart);

        String output = String.format("   thread #%3d: direct lookup of %d keys took %.4f seconds\n",id,numKeys,dur/1000.0);
        output += String.format("   thread #%3d:       = %10.4f s/key\n",id,(dur/1000.0)/((double)numKeys));
        output += String.format("   thread #%3d:       = %10.4f keys/s\n",id,((double)numKeys)/(dur/1000.0));
        output += String.format("   thread #%3d: hit-rate %6.2f%%\n\n",id,(hits*100.0)/(double)numKeys);

        throughput = ((double)numKeys)/(dur/1000.0);

        System.out.print(output);
    }

    public void performUserDefinedLookups() throws BabuDBException {
        long tStart = System.currentTimeMillis();
        int hits = 0;
        for (int i = 0; i < numKeys; i++) {
            final byte[] key = createRandomKey();
            byte[] value = (byte[]) database.syncUserDefinedLookup(dbName, new UserDefinedLookup() {

                public Object execute(LSMLookupInterface database) throws BabuDBException {
                    byte[] result = null;
                    for (int i = 0; i < 20; i++)
                        result = database.lookup(0, key);
                    return result;
                }
            });
            if (value != null)
                hits++;
        }
        long tEnd = System.currentTimeMillis();
        double dur = (tEnd-tStart);

        String output = String.format("   thread #%3d: exec   of %d UDLs took %.4f seconds\n",id,numKeys,dur/1000.0);
        output += String.format("   thread #%3d:       = %10.4f s/UDL\n",id,(dur/1000.0)/((double)numKeys));
        output += String.format("   thread #%3d:       = %10.4f UDLs/s\n",id,((double)numKeys)/(dur/1000.0));
        output += String.format("   thread #%3d: hit-rate %6.2f%%\n\n",id,(hits*100.0)/(double)numKeys);

        throughput = ((double)numKeys)/(dur/1000.0);

        System.out.print(output);
    }

    private byte[] createRandomKey() {
        final int length = (int) (Math.random() * ((double)(maxKeyLength - minKeyLength)) + minKeyLength);
        assert(length >= minKeyLength);
        assert(length <= maxKeyLength);

        byte[] key = new byte[length];
        for (int i = 0; i < length; i++)
            key[i] = BabuDBBenchmark.CHARS[(int)(Math.random()*(BabuDBBenchmark.CHARS.length-1))];
        return key;
    }


}
