/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.xtreemfs.babudb.lsmdb.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup.InsertRecord;
import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.log.DiskLogFile;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.common.buffer.ReusableBuffer;
import org.xtreemfs.common.logging.Logging;

/**
 * The BabuDB main class and interface.
 * @author bjko
 */
public class BabuDB {
    
    /**
     * Version (name)
     */
    public static final String BABUDB_VERSION = "0.1.0 RC";
    
    /**
     * Version of the DB on-disk format (to detect incompatabilities).
     */
    public static final int    BABUDB_DB_FORMAT_VERSION = 1;

    /**
     * The database configuration file.
     * Contains the names and number of indices for the databases.
     */
    private static final String DBCFG_FILE = "config.db";

    /**
     * Mapping from database name to database id
     */
    private final Map<String, LSMDatabase> dbNames;

    /**
     * Mapping from dbId to database
     */
    private final Map<Integer, LSMDatabase> databases;

    /**
     * base dir to store database index snapshots in
     */
    private final String baseDir;

    /**
     * directory in which the database logs are stored.
     */
    private final String dbLogDir;

    /**
     * the disk logger is used to write InsertRecordGroups persistently to disk
     */
    private final DiskLogger logger;

    /**
     * object used for synchronizing modifications of the database lists.
     */
    private final Object dbModificationLock;

    /**
     * object used to ensure that only one checkpoint is created synchronously
     */
    private final Object checkpointLock;

    /**
     * object used for locking to ensure that swicth of log file and overlay trees is atomic
     * and not interupted by inserts
     */
    private final ReadWriteLock overlaySwitchLock;

    private final LSMDBWorker[] worker;

    /**
     * ID to assign to next database create
     */
    private int nextDbId;

    /**
     * Checkpointer thread for automatic checkpointing
     */
    //FIXME: public for debugging only!
    public final Checkpointer dbCheckptr;

    private final Map<String, ByteRangeComparator> compInstances;

    /**
     * Starts the BabuDB database
     * @param baseDir directory in which the datbase snapshots are stored
     * @param dbLogDir directory in which the database append logs are stored (can be same as baseDir)
     * @param numThreads number of worker threads to use
     * @param maxLogfileSize a checkpoint is generated if  maxLogfileSize is exceeded
     * @param checkInterval interval between two checks in seconds, 0 disables auto checkpointing
     * @param noFsync if set to true, there is no fsync after commiting to the on-disk log. This can
     *        cause data loss on crash but increases the insert performance.
     */
    public BabuDB(String baseDir, String dbLogDir, int numThreads,
            long maxLogfileSize, int checkInterval, boolean noFsync) throws BabuDBException {
        if (baseDir.endsWith(File.separator)) {
            this.baseDir = baseDir;
        } else {
            this.baseDir = baseDir + File.separatorChar;
        }
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "base dir: " + this.baseDir);

        if (dbLogDir.endsWith(File.separator)) {
            this.dbLogDir = dbLogDir;
        } else {
            this.dbLogDir = dbLogDir + File.separatorChar;
        }
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "db log dir: " + this.dbLogDir);

        dbNames = new HashMap();
        databases = new HashMap();

        nextDbId = 1;

        compInstances = new HashMap();
        compInstances.put(DefaultByteRangeComparator.class.getName(), new DefaultByteRangeComparator());

        loadDBs();

        Logging.logMessage(Logging.LEVEL_INFO, this, "starting log replay");
        LSN nextLSN = replayLogs();
        Logging.logMessage(Logging.LEVEL_INFO, this, "log replay done");

        try {
            logger = new DiskLogger(dbLogDir, nextLSN.getViewId(), nextLSN.getSequenceNo(), noFsync);
            logger.start();
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot start database operations logger", ex);
        }

        dbModificationLock = new Object();
        checkpointLock = new Object();
        overlaySwitchLock = new ReentrantReadWriteLock();

        worker = new LSMDBWorker[numThreads];
        for (int i = 0; i < numThreads; i++) {
            worker[i] = new LSMDBWorker(logger, i, overlaySwitchLock);
            worker[i].start();
        }

        if (checkInterval > 0) {
            dbCheckptr = new Checkpointer(this, logger, checkInterval, maxLogfileSize);
            dbCheckptr.start();
        } else {
            dbCheckptr = null;
        }

        Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB for Java is running (version "+BABUDB_VERSION+")");
    }

    /**
     * shuts down the database.
     * @attention: does not create a final checkpoint!
     */
    public void shutdown() {
        for (LSMDBWorker w : worker) {
            w.shutdown();
        }
        logger.shutdown();
        if (dbCheckptr != null) {
            dbCheckptr.shutdown();
        }
        try {
            logger.waitForShutdown();
            for (LSMDBWorker w : worker) {
                w.waitForShutdown();
            }
            if (dbCheckptr != null) {
                dbCheckptr.waitForShutdown();
            }
        } catch (InterruptedException ex) {
        }
        Logging.logMessage(Logging.LEVEL_INFO, this,"BabuDB shutdown complete.");
    }

    /**
     * NEVER USE THIS EXCEPT FOR UNIT TESTS!
     * Kills the database.
     */
    public void __test_killDB_dangerous() {
        try {
            logger.stop();
            for (LSMDBWorker w : worker) {
                w.stop();
            }
        } catch (IllegalMonitorStateException ex) {
            //we will probably get that when we kill a thread because we do evil stuff here ;-)
        }
    }

    /**
     * Inserts a single key value pair (synchronously)
     * @param database name of database
     * @param indexId index id (0..NumIndices-1)
     * @param key the key
     * @param value the value
     * @throws BabuDBException if the operation failed
     */
    public void syncSingleInsert(String database, int indexId, byte[] key, byte[] value) throws BabuDBException {
        final LSMDatabase db = dbNames.get(database);
        BabuDBInsertGroup irec = new BabuDBInsertGroup(db);
        irec.addInsert(indexId, key, value);

        final AsyncResult result = new AsyncResult();

        asyncInsert(irec, new BabuDBRequestListener() {

            public void insertFinished(Object context) {
                synchronized (result) {
                    result.done = true;
                    result.notify();
                }
            }

            public void lookupFinished(Object context, byte[] value) {
            }

            public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator) {
            }

            public void requestFailed(Object context, BabuDBException error) {
                synchronized (result) {
                    result.done = true;
                    result.error = error;
                    result.notify();
                }
            }
        }, null);
        synchronized (result) {
            try {
                if (!result.done) {
                    result.wait();
                }
            } catch (InterruptedException ex) {
            }
        }
        if (result.error != null) {
            throw result.error;
        }
    }

    /**
     * Inserts a group of key value pair (synchronously)
     * @param irg the insert record group to execute
     * @throws BabuDBException if the operation failed
     */
    public void syncInsert(BabuDBInsertGroup irg) throws BabuDBException {
        //final LSMDatabase db = databases.get(irg.getDatabaseId());

        final AsyncResult result = new AsyncResult();

        asyncInsert(irg, new BabuDBRequestListener() {

            public void insertFinished(Object context) {
                synchronized (result) {
                    result.done = true;
                    result.notify();
                }
            }

            public void lookupFinished(Object context, byte[] value) {
            }

            public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator) {
            }

            public void requestFailed(Object context, BabuDBException error) {
                synchronized (result) {
                    result.done = true;
                    result.error = error;
                    result.notify();
                }
            }
        }, null);
        synchronized (result) {
            try {
                if (!result.done) {
                    result.wait();
                }
            } catch (InterruptedException ex) {
            }
        }
        if (result.error != null) {
            throw result.error;
        }
    }

    /**
     * lookup a single key (synchronously)
     * @param database name of database
     * @param indexId index id (0..NumIndices-1)
     * @param key the key to look up
     * @return a view buffer to the result or null if there is no such entry
     * @throws BabuDBException if the operation failed
     */
    public byte[] syncLookup(String database, int indexId, byte[] key) throws BabuDBException {

        final AsyncResult result = new AsyncResult();

        asyncLookup(database, indexId, key, new BabuDBRequestListener() {

            public void insertFinished(Object context) {
            }

            public void lookupFinished(Object context, byte[] value) {
                synchronized (result) {
                    result.done = true;
                    result.value = value;
                    result.notify();
                }
            }

            public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator) {
            }

            public void requestFailed(Object context, BabuDBException error) {
                synchronized (result) {
                    result.done = true;
                    result.error = error;
                    result.notify();
                }
            }
        }, null);
        synchronized (result) {
            try {
                if (!result.done) {
                    result.wait();
                }
            } catch (InterruptedException ex) {
            }
        }
        if (result.error != null) {
            throw result.error;
        }
        return result.value;
    }
    
    /**
     * lookup a range starting with key
     * @param database name of database
     * @param indexId index id (0..NumIndices-1)
     * @param key the key to start the iterator at
     * @return an iterator to the database starting at the first matching key. Returns key/value pairs in ascending order.
     * @throws BabuDBException if the operation failed
     */
    public Iterator<Entry<byte[], byte[]>> syncPrefixLookup(String database, int indexId, byte[] key) throws BabuDBException {

        final AsyncResult result = new AsyncResult();

        asyncPrefixLookup(database, indexId, key, new BabuDBRequestListener() {

            public void insertFinished(Object context) {
            }

            public void lookupFinished(Object context, byte[] value) {
                
            }

            public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator) {
                synchronized (result) {
                    result.done = true;
                    result.iterator = iterator;
                    result.notify();
                }
            }

            public void requestFailed(Object context, BabuDBException error) {
                synchronized (result) {
                    result.done = true;
                    result.error = error;
                    result.notify();
                }
            }
        }, null);
        synchronized (result) {
            try {
                if (!result.done) {
                    result.wait();
                }
            } catch (InterruptedException ex) {
            }
        }
        if (result.error != null) {
            throw result.error;
        }
        return result.iterator;
    }

    /**
     * Creates a database using the default comparator (bytewise/ASCII string comparator)
     * @param databaseName name of the database
     * @param numIndices number of indices in the database
     * @throws BabuDBException if the database directory cannot be created or the config cannot be saved
     */
    public void createDatabase(String databaseName, int numIndices) throws BabuDBException {

        ByteRangeComparator[] comps = new ByteRangeComparator[numIndices];
        final ByteRangeComparator defaultComparator = compInstances.get(DefaultByteRangeComparator.class.getName());
        for (int i = 0; i < numIndices; i++) {
            comps[i] = defaultComparator;
        }
        createDatabase(databaseName, numIndices, comps);

    }

    /**
     * Creates a new database
     * @param databaseName name, must be unique
     * @param numIndices the number of indices (cannot be changed afterwards)
     * @param comparators an array of ByteRangeComparators for each index (use only one instance)
     * @throws BabuDBException if the database directory cannot be created or the config cannot be saved
     */
    public void createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators) throws BabuDBException {

        synchronized (dbModificationLock) {
            if (dbNames.containsKey(databaseName)) {
                throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + databaseName + "' already exists");
            }
            final int dbId = nextDbId++;
            LSMDatabase db = new LSMDatabase(databaseName, dbId, baseDir + databaseName + "/", numIndices, false, comparators);
            databases.put(dbId, db);
            dbNames.put(databaseName, db);
            saveDBconfig();
        }

    }

    /**
     * Deletes a database
     * @param databaseName name of database to delte
     * @param deleteFiles if true, all snapshots are deleted as well
     * @throws BabuDBException
     */
    public void deleteDatabase(String databaseName, boolean deleteFiles) throws BabuDBException {
        synchronized (dbModificationLock) {
            if (!dbNames.containsKey(databaseName)) {
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '" + databaseName + "' does not exists");
            }
            final LSMDatabase db = dbNames.get(databaseName);
            dbNames.remove(databaseName);
            databases.remove(db.getDatabaseId());
            saveDBconfig();
            if (deleteFiles) {
                //FIXME:
            }

        }
    }
    
    /**
     * Creates a copy of database sourceDB by taking a snapshot, materializing it
     * and loading it as destDB. This does not interrupt operations on sourceDB.
     * @param sourceDB the database to copy
     * @param destDB the new database's name
     */
    public void copyDatabase(String sourceDB, String destDB, byte[] rangeStart, byte[] rangeEnd) throws BabuDBException, IOException {
        
        final LSMDatabase sDB = dbNames.get(sourceDB);
        if (sDB == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '"+sourceDB+"' does not exist");
        
        final int dbId;
        synchronized (dbModificationLock) {
            if (dbNames.containsKey(destDB)) {
                throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + destDB + "' already exists");
            }
            dbId = nextDbId++;
            //just "reserve" the name
            dbNames.put(destDB,null);
            saveDBconfig();
            
        }
        //materializing the snapshot takes some time, we should not hold the lock meanwhile!
        
        int[] snaps = sDB.createSnapshot();
        File dbDir = new File(baseDir+destDB);
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }
        sDB.writeSnapshot(baseDir+destDB+"/", snaps);
        
        //create new DB and load from snapshot
        LSMDatabase dDB = new LSMDatabase(destDB, dbId, baseDir + destDB + "/", sDB.getIndexCount(), true, sDB.getComparators());
        
        //insert real database
        synchronized (dbModificationLock) {
            databases.put(dbId, dDB);
            dbNames.put(destDB, dDB);
            saveDBconfig();
        }
        
    }

    /**
     * Creates a checkpoint of all databases. The in-memory data is merged with the on-disk data and
     * is written to a new snapshot file. Database logs are truncated. This operation is thread-safe.
     * @throws BabuDBException if the checkpoint was not successful
     */
    public void checkpoint() throws BabuDBException, InterruptedException {

        List<LSMDatabase> dbListCopy;

        synchronized (dbModificationLock) {
            dbListCopy = new ArrayList(databases.size());
            for (LSMDatabase db : databases.values()) {
                dbListCopy.add(db);
            }
        }

        synchronized (checkpointLock) {
            try {
                int[][] snapIds = new int[dbListCopy.size()][];
                int i = 0;

                LSN lastWrittenLSN = null;
                try {
                    //critical block...
                    logger.lockLogger();
                    for (LSMDatabase db : dbListCopy) {
                        snapIds[i++] = db.createSnapshot();
                    }
                    lastWrittenLSN = logger.switchLogFile();
                } finally {
                    logger.unlockLogger();
                }
                
                
                i = 0;
                for (LSMDatabase db : dbListCopy) {
                    db.writeSnapshot(lastWrittenLSN.getViewId(), lastWrittenLSN.getSequenceNo(), snapIds[i++]);
                    db.cleanupSnapshot(lastWrittenLSN.getViewId(), lastWrittenLSN.getSequenceNo());
                }

                //FIXME: delete old logfiles!
                //delete all logfile with LSN <= lastWrittenLSN
                File f = new File(dbLogDir);
                String[] logs = f.list(new FilenameFilter() {

                    public boolean accept(File dir, String name) {
                        return name.endsWith(".dbl");
                    }
                });
                if (logs != null) {
                    Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.dbl");
                    for (String log : logs) {
                        Matcher m = p.matcher(log);
                        m.matches();
                        String tmp = m.group(1);
                        int viewId = Integer.valueOf(tmp);
                        tmp = m.group(2);
                        int seqNo = Integer.valueOf(tmp);
                        LSN logLSN = new LSN(viewId, seqNo);
                        if (logLSN.compareTo(lastWrittenLSN) <= 0) {
                            Logging.logMessage(Logging.LEVEL_DEBUG, this, "deleting old db log file: " + log);
                            f = new File(dbLogDir + log);
                            f.delete();
                        }
                    }
                }
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot create checkpoint", ex);
            }
        }

    }
    
    
    /**
     * Creates a snapshot of all indices in a single database. The snapshot is
     * not materialized (use writeSnapshot)
     * and will be removed upon next checkpointing or shutdown.
     * @throws BabuDBException if the checkpoint was not successful
     * @return an array with the snapshot ID for each index in the database
     */
    public int[] createSnapshot(String dbName) throws BabuDBException, InterruptedException {
                
        final LSMDatabase db = dbNames.get(dbName);
        if (db == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '"+dbName+"' does not exist");

        try {
            //critical block...
            logger.lockLogger();
            return db.createSnapshot();
        } finally {
            logger.unlockLogger();
        } 

    }
    
    /**
     * Writes a snapshot to disk.
     * @param dbName the database name
     * @param snapIds the snapshot IDs obtained from createSnapshot
     * @param directory the directory in which the snapshots are written
     * @throws org.xtreemfs.babudb.BabuDBException if the snapshot cannot be written
     */
    public void writeSnapshot(String dbName, int[] snapIds, String directory) throws BabuDBException {
        try {
            final LSMDatabase db = dbNames.get(dbName);
            if (db == null) {
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '" + dbName + "' does not exist");
            }
            db.writeSnapshot(directory, snapIds);
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot write snapshot: "+ex,ex);
        }
    }

    /**
     * Loads the configuration and each database from disk
     * @throws BabuDBException
     */
    private void loadDBs() throws BabuDBException {
        try {
            File f = new File(baseDir + DBCFG_FILE);
            if (f.exists()) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
                final int dbFormatVer = ois.readInt();
                if (dbFormatVer != BABUDB_DB_FORMAT_VERSION) {
                    throw new BabuDBException(ErrorCode.IO_ERROR,"on-disk format (version "+
                            dbFormatVer+") is incompatible with this BabuDB release "+
                            "(uses on-disk format version "+BABUDB_DB_FORMAT_VERSION+")");
                }
                final int numDB = ois.readInt();
                nextDbId =
                        ois.readInt();
                for (int i = 0; i <
                        numDB; i++) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "loading DB...");
                    final String dbName = (String) ois.readObject();
                    final int dbId = ois.readInt();
                    final int numIndex = ois.readInt();
                    ByteRangeComparator[] comps = new ByteRangeComparator[numIndex];
                    for (int idx = 0; idx <
                            numIndex; idx++) {
                        final String className = (String) ois.readObject();
                        ByteRangeComparator comp = compInstances.get(className);
                        if (comp == null) {
                            Class clazz = Class.forName(className);
                            comp =
                                    (ByteRangeComparator) clazz.newInstance();
                            compInstances.put(className, comp);
                        }

                        assert (comp != null);
                        comps[idx] = comp;
                    }

                    LSMDatabase db = new LSMDatabase(dbName, dbId, baseDir + dbName + "/", numIndex, true, comps);
                    databases.put(dbId, db);
                    dbNames.put(dbName, db);
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "loaded DB " + dbName + " successfully.");
                }

                ois.close();
            }

        } catch (InstantiationException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
        } catch (IllegalAccessException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load database config, check path and access rights", ex);
        } catch (ClassNotFoundException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load database config, config file might be corrupted", ex);
        } catch (ClassCastException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load database config, config file might be corrupted", ex);
        }

    }

    /**
     * Replay the database operations log.
     * @return the LSN to assign to the next operation
     * @throws BabuDBException
     */
    private LSN replayLogs() throws BabuDBException {
        if (databases.size() == 0) {
            return new LSN(1, 1);
        }

        try {
            File f = new File(dbLogDir);
            String[] logs = f.list(new FilenameFilter() {

                public boolean accept(File dir, String name) {
                    return name.endsWith(".dbl");
                }
            });
            LSN nextLSN = null;
            if (logs != null) {
                //read list of logs and create a list ordered from min LSN to max LSN
                SortedSet<LSN> orderedLogList = new TreeSet<LSN>();
                Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.dbl");
                for (String log : logs) {
                    Matcher m = p.matcher(log);
                    m.matches();
                    String tmp = m.group(1);
                    int viewId = Integer.valueOf(tmp);
                    tmp =
                            m.group(2);
                    int seqNo = Integer.valueOf(tmp);
                    orderedLogList.add(new LSN(viewId, seqNo));
                }
//apply log entries to databases...
                for (LSN logLSN : orderedLogList) {
                    DiskLogFile dlf = new DiskLogFile(this.dbLogDir, logLSN);
                    LogEntry le = null;
                    while (dlf.hasNext()) {
                        le = dlf.next();
                        //do something
                        ReusableBuffer payload = le.getPayload();
                        InsertRecordGroup ai = InsertRecordGroup.deserialize(payload);
                        insert(ai);
                        le.free();
                    }
//set lsn'
                    if (le != null) {
                        nextLSN = new LSN(le.getViewId(), le.getLogSequenceNo() + 1);
                    }

                }
            }
            if (nextLSN != null) {
                return nextLSN;
            } else {
                return new LSN(1, 1);
            }

        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load database operations log, file might be corrupted", ex);
        } catch (LogEntryException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "corrupted/incomplete log entry in database operations log", ex);
        } catch (Exception ex) {
            throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "cannot load database operations log, unexpected error", ex);
        }

    }

    /**
     * saves the current database config to disk
     * @throws BabuDBException
     */
    private void saveDBconfig() throws BabuDBException {
        /*File f = new File(baseDir+DBCFG_FILE);
        f.renameTo(new File(baseDir+DBCFG_FILE+".old"));*/
        synchronized (dbModificationLock) {
            try {
                FileOutputStream fos = new FileOutputStream(baseDir + DBCFG_FILE + ".in_progress");
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeInt(BABUDB_DB_FORMAT_VERSION);
                oos.writeInt(databases.size());
                oos.writeInt(nextDbId);
                for (int dbId : databases.keySet()) {
                    LSMDatabase db = databases.get(dbId);
                    oos.writeObject(db.getDatabaseName());
                    oos.writeInt(dbId);
                    oos.writeInt(db.getIndexCount());
                    String[] compClasses = db.getComparatorClassNames();
                    for (int i = 0; i <
                            db.getIndexCount(); i++) {
                        oos.writeObject(compClasses[i]);
                    }
                }

                oos.flush();
                fos.flush();
                fos.getFD().sync();
                oos.close();
                File f = new File(baseDir + DBCFG_FILE + ".in_progress");
                f.renameTo(new File(baseDir + DBCFG_FILE));
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "unable to save database configuration", ex);
            }

        }
    }

    /**
     * Insert a full record group. Only to be used by log replay.
     * @param ins
     */
    private void insert(InsertRecordGroup ins) {
        final LSMDatabase db = databases.get(ins.getDatabaseId());
        //ignore deleted databases when recovering!
        if (db == null) {
            return;
        }

        for (InsertRecord ir : ins.getInserts()) {
            LSMTree tree = db.getIndex(ir.getIndexId());
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "insert " + new String(ir.getKey()) + "=" +
                    new String(ir.getValue()) + " into " + db.getDatabaseName() + " " + ir.getIndexId());
            tree.insert(ir.getKey(), ir.getValue());
        }

    }

    /**
     * Insert an group of inserts asynchronously.
     * @param ig the group of inserts
     * @param listener a callback for the result
     * @param context optional context object which is passed to the listener
     * @throws BabuDBException
     */
    public void asyncInsert(BabuDBInsertGroup ig, BabuDBRequestListener listener,
            Object context) throws BabuDBException {
        final InsertRecordGroup ins = ig.getRecord();
        final int dbId = ins.getDatabaseId();
        if (!databases.containsKey(dbId)) {
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
        }

        LSMDBWorker w = worker[dbId % worker.length];
        if (Logging.tracingEnabled()) {
            Logging.logMessage(Logging.LEVEL_TRACE, this, "insert request is sent to worker #" + dbId % worker.length);
        }

        w.addRequest(new LSMDBRequest(databases.get(dbId), listener, ins, context));
    }

    /**
     * Creates a new group of inserts.
     * @param databaseName the database to which the inserts are applied
     * @return a insert record group
     * @throws BabuDBException
     */
    public BabuDBInsertGroup createInsertGroup(
            String databaseName) throws BabuDBException {
        final LSMDatabase db = dbNames.get(databaseName);
        if (db == null) {
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
        }

        return new BabuDBInsertGroup(db);
    }

    /**
     * Lookup a key=value asynchronously
     * @param databaseName database which is queried
     * @param indexId the lookup index
     * @param key the key to lookup
     * @param listener a callback for the result
     * @param context optional context object which is passed to the listener
     * @throws BabuDBException
     */
    public void asyncLookup(String databaseName, int indexId, byte[] key,
            BabuDBRequestListener listener, Object context) throws BabuDBException {
        final LSMDatabase db = dbNames.get(databaseName);
        if (db == null) {
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
        }

        LSMDBWorker w = worker[db.getDatabaseId() % worker.length];
        if (Logging.tracingEnabled()) {
            Logging.logMessage(Logging.LEVEL_TRACE, this, "lookup request is sent to worker #" + db.getDatabaseId() % worker.length);
        }

        w.addRequest(new LSMDBRequest(db, indexId, listener, key, false, context));
    }

    /**
     * Asynchronously prefix lookup for a key.
     * @param databaseName database which is queried
     * @param indexId the lookup index
     * @param key the key to lookup
     * @param listener a callback for the result
     * @param context optional context object which is passed to the listener
     * @throws BabuDBException
     */
    public void asyncPrefixLookup(String databaseName, int indexId, byte[] key,
            BabuDBRequestListener listener, Object context) throws BabuDBException {
        final LSMDatabase db = dbNames.get(databaseName);
        if (db == null) {
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
        }

        LSMDBWorker w = worker[db.getDatabaseId() % worker.length];
        if (Logging.tracingEnabled()) {
            Logging.logMessage(Logging.LEVEL_TRACE, this, "lookup request is sent to worker #" + db.getDatabaseId() % worker.length);
        }

        w.addRequest(new LSMDBRequest(db, indexId, listener, key, true, context));
    }

    private static class AsyncResult {

        public boolean done = false;

        public byte[] value;

        public Iterator<Entry<byte[], byte[]>> iterator;

        public BabuDBException error;

    }
}
