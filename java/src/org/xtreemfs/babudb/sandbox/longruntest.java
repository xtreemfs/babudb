/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/
package org.xtreemfs.babudb.sandbox;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.config.BabuDBConfig;
import org.xtreemfs.include.common.logging.Logging;

/**
 *
 * @author bjko
 */
public class longruntest {

    public static final String dbname = "testdb";

    public static final int maxdictentries = 10000;

    private final List<String> dictionary;

    public long numIns,  numInsGroup,  numRem,  numLookup;

    public long tStart,  tEnd;

    private final BabuDB database;

    private final int numIndices;

    private final TreeMap<String, String>[] controlIndices;

    @SuppressWarnings("unchecked")
    public longruntest(String basedir, String dictFile, int numIndices, boolean compression) throws IOException, BabuDBException {
        dictionary = new ArrayList<String>(maxdictentries);

        FileReader fr = new FileReader(dictFile);
        BufferedReader bfr = new BufferedReader(fr);
        String line = null;
        int numEntries = 0;
        do {
            line = bfr.readLine();
            if (line == null) {
                break;
            }
            dictionary.add(line);
            numEntries++;
        } while ((numEntries < maxdictentries) && (line != null));
        bfr.close();
        fr.close();
        this.numIndices = numIndices;

        //checkpoint every 1m and check every 1 min
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(basedir, basedir, 2, 1024 * 128 , 60 * 1, SyncMode.SYNC_WRITE,0,0, compression));
        
        database.getDatabaseManager().createDatabase(dbname, numIndices);

        controlIndices = new TreeMap[numIndices];
        for (int i = 0; i < numIndices; i++) {
            controlIndices[i] = new TreeMap<String, String>();
        }
    }

    public void startTest(int numHours) throws Exception {
        
        final Database db = database.getDatabaseManager().getDatabase(dbname);
        
        tStart = System.currentTimeMillis();
        tEnd = tStart + numHours * 60 * 60 * 1000;
        while (System.currentTimeMillis() < tEnd) {

            //what shall we do?
            final int oper = (int) Math.round(Math.random() * 3);

            switch (oper) {
                case 0:
                     {
                        //single insert
                        final int index = getRandomIndex();
                        final String key = getRandomDictEntry();
                        final String value = getRandomDictEntry();
                        
                        controlIndices[index].put(key, value);
                        db.syncSingleInsert(index, key.getBytes(),
                                value.getBytes());
                        
                        numIns++;
                        System.out.print("+");
                    }
                    ;
                    break;
                case 1:
                     {
                        //groupInsert
                        final int numInGroup = (int) Math.round(Math.random() * (9)) + 1;
                        final BabuDBInsertGroup ig = db.createInsertGroup();
                        for (int i = 0; i < numInGroup; i++) {
                            final int index = getRandomIndex();
                            final String key = getRandomDictEntry();
                            final String value = getRandomDictEntry();
                            controlIndices[index].put(key, value);
                            ig.addInsert(index, key.getBytes(), value.getBytes());
                        }
                        
                        db.syncInsert(ig);
                        
                        numInsGroup++;
                        System.out.print("x");
                    }
                    ;
                    break;
                case 2:
                     {
                        //lookupo
                        final int index = getRandomIndex();
                        final String randKey = getRandomDictEntry();
                        String highKey = controlIndices[index].higherKey(randKey);
                        if (highKey == null) {
                            highKey = randKey;
                        }
                        final String controlResult = controlIndices[index].get(highKey);
                        final byte[] result = db.syncLookup(index, highKey.getBytes());
                        if (((controlResult == null) && (result != null)) ||
                                ((controlResult != null) && (result == null))) {
                            printIndex(index);
                            throw new Exception("LSMTree is invalid (expected null as in control tree) for " + highKey);

                        }
                        if ((controlResult != null) && (result != null)) {
                            String rbResult = new String(result);
                            if (!rbResult.equals(controlResult)) {
                                printIndex(index);
                                throw new Exception("LSMTree is invalid (results are not equal) expected " +
                                        highKey + "=" + controlResult +"("+controlResult.length()+") , got " + rbResult+"("+rbResult.length()+")");
                            }
                        }
                        numLookup++;
                        System.out.print("o");
                    }
                    ;
                    break;
                case 3:
                     {
                        //delete
                        try {
                            final int index = getRandomIndex();
                            final String ftKey = controlIndices[index].firstKey();
                            controlIndices[index].remove(ftKey);
                            db.syncSingleInsert(index, ftKey.getBytes(), null);
                            
                            numRem++;
                            System.out.print("-");
                        } catch (NoSuchElementException ex) {
                            //empty tree
                        }
                    }
                    ;
                    break;
            }
            if ((numIns + numInsGroup) % 2000 == 0) {
                checkIntegrity();
            }
        }

    }

    public void checkIntegrity() throws Exception {
        for (int index = 0; index < controlIndices.length; index++) {
            for (String key : controlIndices[index].keySet()) {
                final byte[] babuResult = database.getDatabaseManager().getDatabase(dbname).syncLookup(index, key.getBytes());
                String bValue = null;
                if (babuResult != null) {
                    bValue = new String(babuResult);
                }
                final String ctrlResult = controlIndices[index].get(key);
                if ( ((ctrlResult == null) && (bValue != null)) |
                    ((ctrlResult != null) && (!ctrlResult.equals(bValue))) ){
                    printIndex(index);
                    throw new Exception("Invalid tree index "+index+" at key "+key);
                }
            }
        }
        System.out.println("\nintegrity check ok.");
    }

    public void shutdown() throws Exception {
        database.getCheckpointer().checkpoint();
        database.shutdown();
    }

    public String getRandomDictEntry() {
        final int rndEntry = (int) Math.round(Math.random() * (dictionary.size() - 1));
        return dictionary.get(rndEntry);
    }

    public int getRandomIndex() {
        return (int) Math.round(Math.random() * (numIndices - 1));
    }

    public static void main(String[] args) {
        try {
            if (args.length != 4) {
                System.out.println("usage: longruntest <dictionary> <basedir> <numIndices> <hours to run>");
                System.exit(2);
            }

            Logging.start(Logging.LEVEL_INFO);

            final String dictFile = args[0];
            final String basedir = args[1];
            final int numIndices = Integer.valueOf(args[2]);
            final int runtime = Integer.valueOf(args[3]);
            final boolean compression = Integer.valueOf(args[4]) == 0 ? false:true;


            longruntest test = new longruntest(basedir, dictFile, numIndices, compression);
            test.startTest(runtime);
            System.out.println("\n\n-------------------------------------------------");
            System.out.println("test finished successfuly");
            System.out.println("");
            System.out.format("# single inserts            %5d\n", test.numIns);
            System.out.format("# single group inserts      %5d\n", test.numInsGroup);
            System.out.format("# deletes                   %5d\n", test.numRem);
            System.out.format("# lookups                   %5d\n", test.numLookup);
            test.shutdown();
        } catch (OutOfMemoryError ex) {
            System.out.println("\n\n-------------------------------------------------");
            System.out.println("test crashed: "+ex);
            ex.printStackTrace();
            System.out.println("\n\n");
            System.out.println(BufferPool.getStatus());
            System.out.println("\n\n");
            System.out.println("free : "+Runtime.getRuntime().freeMemory());
            System.out.println("max  : "+Runtime.getRuntime().maxMemory());
            System.out.println("total: "+Runtime.getRuntime().totalMemory());
            System.exit(1);
        } catch (Exception ex) {
            System.out.println("\n\n-------------------------------------------------");
            System.out.println("test crashed: "+ex);
            ex.printStackTrace();
            System.exit(1);
        }

    }

    private void printIndex(int index) {
        try {
            System.out.println("-------------------------------------------------------");
            System.out.println("TREE INDEX " + index);
            for (String key : controlIndices[index].keySet()) {
                final byte[] babuResult = database.getDatabaseManager().getDatabase(dbname).syncLookup(index, key.getBytes());
                String bValue = null;
                if (babuResult != null) {
                    bValue = new String(babuResult);
                }
                System.out.println(key + " = " + controlIndices[index].get(key) +
                        "("+controlIndices[index].get(key).length()+")"+
                        " / " + bValue+
                        "("+bValue.length()+")");
            }
            System.out.println("-------------------------------------------------------");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
