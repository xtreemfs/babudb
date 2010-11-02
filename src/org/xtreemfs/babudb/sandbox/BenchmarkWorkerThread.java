/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
*/

package org.xtreemfs.babudb.sandbox;

import java.util.Iterator;
import java.util.Random;
import java.util.Map.Entry;

import org.xtreemfs.babudb.UserDefinedLookup;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exceptions.BabuDBException;
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

    private final Random random;
 
    public BenchmarkWorkerThread(int id, int numKeys, int minKeyLength,
            int maxKeyLength, byte[] payload, BabuDB database, BenchmarkOperation operation) {
        this(id,numKeys,minKeyLength,maxKeyLength,payload,database,operation,"",0L);
    }
    
    public BenchmarkWorkerThread(int id, int numKeys, int minKeyLength,
            int maxKeyLength, byte[] payload, BabuDB database, BenchmarkOperation operation, String dbPrefix, long seed) {
        this.id = id;
        this.numKeys = numKeys;
        this.minKeyLength = minKeyLength;
        this.maxKeyLength = maxKeyLength;
        this.payload = payload;
        this.database = database;
        this.operation = operation;
        this.dbName = dbPrefix+Integer.toString(id);
        this.random = new Random(seed);
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

    public void performInserts() throws BabuDBException, InterruptedException {
        long tStart = System.currentTimeMillis();
        byte[] key = null;
        for (int i = 0; i < numKeys; i++) {
            key = createRandomKey();
            database.getDatabaseManager().getDatabase(dbName).singleInsert(0, key, payload, null).get();
        }
        long tEnd = System.currentTimeMillis();
        double dur = (tEnd-tStart);

        String output = String.format("   thread #%3d: insert of %d keys took %.4f seconds\n", id ,numKeys,dur/1000.0);
        output += String.format("   thread #%3d:       = %10.4f s/key\n",id,(dur/1000.0)/((double)numKeys));
        output += String.format("   thread #%3d:       = %10.4f keys/s\n\n",id,((double)numKeys)/(dur/1000.0));

        throughput = ((double)numKeys)/(dur/1000.0);

        System.out.print(output);
    }

    public void countKeys() throws BabuDBException, InterruptedException {
        long tStart = System.currentTimeMillis();
        Iterator<Entry<byte[],byte[]>> iter = database.getDatabaseManager().getDatabase(dbName).prefixLookup(0, new byte[]{}, null).get();
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

    public void performLookups() throws BabuDBException, InterruptedException {
        long tStart = System.currentTimeMillis();
        byte[] key = null;
        int hits = 0;
        for (int i = 0; i < numKeys; i++) {
            key = createRandomKey();
            byte[] value = database.getDatabaseManager().getDatabase(dbName).lookup(0, key, null).get();
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

    public void performDirectLookups() throws BabuDBException, InterruptedException {
        long tStart = System.currentTimeMillis();
        byte[] key = null;
        int hits = 0;
        for (int i = 0; i < numKeys; i++) {
            key = createRandomKey();
            byte[] value = database.getDatabaseManager().getDatabase(dbName).lookup(0, key, null).get();
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

    public void performUserDefinedLookups() throws BabuDBException, InterruptedException {
        long tStart = System.currentTimeMillis();
        int hits = 0;
        for (int i = 0; i < numKeys; i++) {
            final byte[] key = createRandomKey();
            byte[] value = (byte[]) database.getDatabaseManager().getDatabase(dbName).userDefinedLookup(new UserDefinedLookup() {

                public Object execute(LSMLookupInterface database) throws BabuDBException {
                    byte[] result = null;
                    for (int i = 0; i < 20; i++)
                        result = database.lookup(0, key);
                    return result;
                }
            },null).get();
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

    byte[] createRandomKey() {
        return createRandomKey(random.nextDouble(),random.nextDouble());
    }
    
    private byte[] createRandomKey(double randLength,double randVal) {
        final int length = (int) (randLength * ((double)(maxKeyLength - minKeyLength)) + minKeyLength);
        assert(length >= minKeyLength);
        assert(length <= maxKeyLength);

        byte[] key = new byte[length];
        for (int i = 0; i < length; i++)
            key[i] = BabuDBBenchmark.CHARS[(int)(randVal*(BabuDBBenchmark.CHARS.length-1))];
        
        return key;
    }


}
