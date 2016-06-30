/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.lsmdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.dev.DatabaseManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.foundation.logging.Logging;

import static org.xtreemfs.babudb.BabuDBFactory.*;

/**
 * <p>
 * Operations to manipulate the DB-config-file.
 * </p>
 * 
 * @author flangner
 * @since 09/02/2009
 */

public class DBConfig {
    
    private final BabuDBInternal dbs;
    
    private final File           configFile;
    
    private boolean              conversionRequired;
    
    private int                  dbFormatVer;
    
    public DBConfig(BabuDBInternal dbs) throws BabuDBException {
        this.dbs = dbs;
        this.configFile = new File(dbs.getConfig().getBaseDir() + dbs.getConfig().getDbCfgFile());
        load();
    }
    
    /**
     * Loads the configuration and each database from disk and replaces the data
     * of the existing DBs, while removing outdated one's.
     * 
     * @throws BabuDBException
     */
    public void reset() throws BabuDBException {
        DatabaseManagerInternal dbman = dbs.getDatabaseManager();
        assert (dbman != null) : "The DatabaseManager is not available!";
        
        ObjectInputStream ois = null;
        try {
            List<Integer> ids = new LinkedList<Integer>();
            if (configFile.exists()) {
                ois = new ObjectInputStream(new FileInputStream(configFile));
                final int dbFormatVer = ois.readInt();
                if (dbFormatVer != BABUDB_DB_FORMAT_VERSION) {
                    throw new BabuDBException(ErrorCode.IO_ERROR, "on-disk format (version " + dbFormatVer
                        + ") is incompatible with this BabuDB release " + "(uses on-disk format version "
                        + BABUDB_DB_FORMAT_VERSION + ")");
                }
                final int numDB = ois.readInt();
                dbman.setNextDBId(ois.readInt());
                for (int i = 0; i < numDB; i++) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "loading DB...");
                    final String dbName = (String) ois.readObject();
                    final int dbId = ois.readInt();
                    final int numIndex = ois.readInt();
                    ByteRangeComparator[] comps = new ByteRangeComparator[numIndex];
                    for (int idx = 0; idx < numIndex; idx++) {
                        final String className = (String) ois.readObject();
                        ByteRangeComparator comp = dbman.getComparatorInstances().get(className);
                        if (comp == null) {
                            Class<?> clazz = Class.forName(className);
                            comp = (ByteRangeComparator) clazz.newInstance();
                            dbman.getComparatorInstances().put(className, comp);
                        }
                        
                        assert (comp != null);
                        comps[idx] = comp;
                    }
                    
                    ids.add(dbId);
                    
                    DatabaseInternal db;
                    try {
                        // reset existing DBs
                        db = dbman.getDatabase(dbId);
                        db.setLSMDB(new LSMDatabase(dbName, dbId, dbs.getConfig().getBaseDir() 
                                + dbName + File.separatorChar, numIndex, true, comps, 
                                dbs.getConfig().getCompression(), 
                                dbs.getConfig().getMaxNumRecordsPerBlock(), 
                                dbs.getConfig().getMaxBlockFileSize(), 
                                dbs.getConfig().getDisableMMap(),
                                dbs.getConfig().getMMapLimit()));
                    } catch (BabuDBException e) {
                        db = new DatabaseImpl(dbs, new LSMDatabase(dbName, dbId, 
                                dbs.getConfig().getBaseDir() + dbName + File.separatorChar, 
                                numIndex, true, comps, dbs.getConfig().getCompression(), 
                                dbs.getConfig().getMaxNumRecordsPerBlock(), 
                                dbs.getConfig().getMaxBlockFileSize(), 
                                dbs.getConfig().getDisableMMap(),
                                dbs.getConfig().getMMapLimit()));
                        
                        dbman.putDatabase(db);
                    }
                    
                    Logging.logMessage(Logging.LEVEL_INFO, this, "loaded DB %s" + " successfully. [LSN %s]",
                        dbName, db.getLSMDB().getOndiskLSN());
                }
            }
            
            // delete remaining outdated DBs
            Set<Integer> outdatedIds = dbman.getAllDatabaseIds();
            outdatedIds.removeAll(ids);
            if (outdatedIds.size() > 0) {
                for (int id : outdatedIds) {
                    dbman.removeDatabaseById(id);
                }
            }
        } catch (InstantiationException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
        } catch (IllegalAccessException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database config, check path and access rights", ex);
        } catch (ClassNotFoundException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database config, config file might be corrupted", ex);
        } catch (ClassCastException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database config, config file might be corrupted", ex);
        } finally {
            if (ois != null)
                try {
                    ois.close();
                } catch (IOException e) {
                    /* I don't care */
                }
        }
    }
    
    /**
     * Loads the configuration and each database from disk.
     * 
     * @throws BabuDBException
     */
    public void load() throws BabuDBException {
        DatabaseManagerInternal dbman = dbs.getDatabaseManager();
        assert (dbman != null) : "The DatabaseManager is not available!";
        
        ObjectInputStream ois = null;
        try {
            if (configFile.exists()) {
                ois = new ObjectInputStream(new FileInputStream(configFile));
                dbFormatVer = ois.readInt();
                if (dbFormatVer != BABUDB_DB_FORMAT_VERSION)
                    conversionRequired = true;
                final int numDB = ois.readInt();
                dbman.setNextDBId(ois.readInt());
                for (int i = 0; i < numDB; i++) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "loading DB...");
                    final String dbName = (String) ois.readObject();
                    final int dbId = ois.readInt();
                    final int numIndex = ois.readInt();
                    ByteRangeComparator[] comps = new ByteRangeComparator[numIndex];
                    for (int idx = 0; idx < numIndex; idx++) {
                        final String className = (String) ois.readObject();
                        ByteRangeComparator comp = dbman.getComparatorInstances().get(className);
                        if (comp == null) {
                            Class<?> clazz = Class.forName(className);
                            comp = (ByteRangeComparator) clazz.newInstance();
                            dbman.getComparatorInstances().put(className, comp);
                        }
                        
                        assert (comp != null);
                        comps[idx] = comp;
                    }
                    
                    if (!conversionRequired) {
                        DatabaseInternal db = new DatabaseImpl(this.dbs, 
                                new LSMDatabase(dbName, dbId, this.dbs.getConfig().getBaseDir()
                            + dbName + File.separatorChar, numIndex, true, comps, dbs.getConfig()
                                .getCompression(), this.dbs.getConfig().getMaxNumRecordsPerBlock(), 
                                dbs.getConfig().getMaxBlockFileSize(), dbs.getConfig().getDisableMMap(),
                                dbs.getConfig().getMMapLimit()));
                        dbman.putDatabase(db);
                        Logging.logMessage(Logging.LEVEL_DEBUG, this, "loaded DB " + dbName
                            + "(" + dbId + ") successfully.");
                    }
                }
            }
            
        } catch (InstantiationException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
        } catch (IllegalAccessException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database config, check path and access rights", ex);
        } catch (ClassNotFoundException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database config, config file might be corrupted", ex);
        } catch (ClassCastException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database config, config file might be corrupted", ex);
        } finally {
            if (ois != null)
                try {
                    ois.close();
                } catch (IOException e) {
                    /* who cares? */
                }
        }
    }
    
    /**
     * saves the current database config to disk
     * 
     * @param filename
     *            path to the config file location
     * @throws BabuDBException
     */
    public void save(String filename) throws BabuDBException {
        DatabaseManagerInternal dbman = dbs.getDatabaseManager();
        
        synchronized (dbman.getDBModificationLock()) {
            try {
                File tempFile = new File(filename + ".in_progress");
                FileOutputStream fos = new FileOutputStream(tempFile);
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeInt(BABUDB_DB_FORMAT_VERSION);
                oos.writeInt(dbman.getAllDatabaseIds().size());
                oos.writeInt(dbman.getNextDBId());
                for (int dbId : dbman.getAllDatabaseIds()) {
                    LSMDatabase db = dbman.getDatabase(dbId).getLSMDB();
                    oos.writeObject(db.getDatabaseName());
                    oos.writeInt(dbId);
                    oos.writeInt(db.getIndexCount());
                    String[] compClasses = db.getComparatorClassNames();
                    for (int i = 0; i < db.getIndexCount(); i++) {
                        oos.writeObject(compClasses[i]);
                    }
                }
                
                oos.flush();
                fos.flush();
                fos.getFD().sync();
                oos.close();
                
                File dbFile = new File(filename);
                
                // try to rename the file
                boolean success = tempFile.renameTo(dbFile);
                if (!success) {
                    // on Windows machines, the target mustn't exist; thus, it
                    // is necessary to sacrifice atomicity and delete the
                    // previous file before
                    dbFile.delete();
                    tempFile.renameTo(dbFile);
                }
                
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "unable to save database configuration", ex);
            }
            
        }
    }
    
    public void save() throws BabuDBException {
        save(dbs.getConfig().getBaseDir() + dbs.getConfig().getDbCfgFile());
    }
    
    public boolean isConversionRequired() {
        return conversionRequired;
    }
    
    public int getDBFormatVersion() {
        return dbFormatVer;
    }
}
