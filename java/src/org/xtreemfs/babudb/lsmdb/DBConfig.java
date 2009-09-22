/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.include.common.logging.Logging;

/**
 * <p>
 * Operations to manipulate the DB-config-file.
 * </p>
 * 
 * @author flangner
 * @since 09/02/2009
 */

public class DBConfig {
    
    private final BabuDB dbs;
    
    private final File configFile;

    public DBConfig(BabuDB dbs) throws BabuDBException {
        this.dbs = dbs;
        this.configFile = new File(this.dbs.getConfig().getBaseDir() + this.dbs.getConfig().getDbCfgFile());
        load();
    }
    
    /**
     * Loads the configuration and each database from disk.
     * 
     * @throws BabuDBException
     */
    public void load() throws BabuDBException {
        DatabaseManagerImpl dbman = (DatabaseManagerImpl) dbs.getDatabaseManager();
        assert (dbman != null) : "The DatabaseManager is not available!";
        
        ObjectInputStream ois = null;
        try {
            if (configFile.exists()) {
                ois = new ObjectInputStream(new FileInputStream(configFile));
                final int dbFormatVer = ois.readInt();
                if (dbFormatVer != BabuDB.BABUDB_DB_FORMAT_VERSION) {
                    throw new BabuDBException(ErrorCode.IO_ERROR, "on-disk format (version " + dbFormatVer
                        + ") is incompatible with this BabuDB release " + "(uses on-disk format version "
                        + BabuDB.BABUDB_DB_FORMAT_VERSION + ")");
                }
                final int numDB = ois.readInt();
                dbman.nextDbId = 
                    ois.readInt();
                for (int i = 0; i < numDB; i++) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "loading DB...");
                    final String dbName = (String) ois.readObject();
                    final int dbId = ois.readInt();
                    final int numIndex = ois.readInt();
                    ByteRangeComparator[] comps = new ByteRangeComparator[numIndex];
                    for (int idx = 0; idx < numIndex; idx++) {
                        final String className = (String) ois.readObject();
                        ByteRangeComparator comp = dbman.compInstances.get(className);
                        if (comp == null) {
                            Class<?> clazz = Class.forName(className);
                            comp = (ByteRangeComparator) clazz.newInstance();
                            dbman.compInstances.put(className, comp);
                        }
                        
                        assert (comp != null);
                        comps[idx] = comp;
                    }
                    
                    Database db = new DatabaseImpl(this.dbs, 
                            new LSMDatabase(dbName, dbId, this.dbs.getConfig()
                            .getBaseDir()
                        + dbName + File.separatorChar, numIndex, true, comps, this.dbs.getConfig().getCompression()));
                    dbman.dbsById.put(dbId, db);
                    dbman.dbsByName.put(dbName, db);
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "loaded DB " + 
                            dbName + " successfully.");
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
     * @throws BabuDBException
     */
    public void save() throws BabuDBException {
        DatabaseManagerImpl dbman = (DatabaseManagerImpl) dbs.getDatabaseManager();

        synchronized (dbman.getDBModificationLock()) {
            try {
                FileOutputStream fos = new FileOutputStream(dbs.getConfig().getBaseDir()
                    + dbs.getConfig().getDbCfgFile() + ".in_progress");
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeInt(BabuDB.BABUDB_DB_FORMAT_VERSION);
                oos.writeInt(dbman.dbsById.size());
                oos.writeInt(dbman.nextDbId);
                for (int dbId : dbman.dbsById.keySet()) {
                    LSMDatabase db = ((DatabaseImpl) dbman.dbsById.get(dbId)).getLSMDB();
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
                File f = new File(dbs.getConfig().getBaseDir() + dbs.getConfig().getDbCfgFile()
                    + ".in_progress");
                f.renameTo(new File(dbs.getConfig().getBaseDir() + dbs.getConfig().getDbCfgFile()));
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "unable to save database configuration", ex);
            }
            
        }
    }
}
