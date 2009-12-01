/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.conversion;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.common.logging.Logging.Category;
import org.xtreemfs.include.common.util.FSUtils;
import org.xtreemfs.include.common.util.OutputUtils;

public class AutoConverter {
    
    static class IndexFileIterator implements Iterator<Entry<byte[], byte[]>> {
        
        private InputStream in;
        
        private ByteBuffer  lenBytes;
        
        public IndexFileIterator(File file) {
            
            try {
                in = new FileInputStream(file);
                lenBytes = ByteBuffer.wrap(new byte[4]);
                
            } catch (IOException exc) {
                Logging.logMessage(Logging.LEVEL_ERROR, Category.db, this,
                    "an error occurred while trying to convert the database: "
                        + OutputUtils.stackTraceToString(exc));
            }
            
        }
        
        @Override
        public boolean hasNext() {
            
            try {
                return in.available() > 0;
                
            } catch (IOException exc) {
                
                Logging.logMessage(Logging.LEVEL_ERROR, Category.db, this,
                    "an error occurred while trying to convert the database: "
                        + OutputUtils.stackTraceToString(exc));
                
                return false;
            }
        }
        
        @Override
        public Entry<byte[], byte[]> next() {
            
            try {
                
                // read the key
                
                int tmp = in.read(lenBytes.array());
                if (tmp != 4) {
                    Logging
                            .logMessage(
                                Logging.LEVEL_ERROR,
                                Category.db,
                                this,
                                "an error occurred while trying to convert the database; database dump corrupted (only %d bytes read, available: %d)",
                                tmp, in.available());
                    return null;
                }
                
                int len = lenBytes.getInt();
                lenBytes.position(0);
                
                final byte[] key = new byte[len];
                int num = in.read(key);
                
                if (num != len) {
                    Logging
                            .logMessage(
                                Logging.LEVEL_ERROR,
                                Category.db,
                                this,
                                "an error occurred while trying to convert the database; expected key length: %d, actual key length: %d",
                                len, num);
                    return null;
                }
                
                // read the value
                tmp = in.read(lenBytes.array());
                if (tmp != 4) {
                    Logging
                            .logMessage(
                                Logging.LEVEL_ERROR,
                                Category.db,
                                this,
                                "an error occurred while trying to convert the database; database dump corrupted (only %d bytes read, available: %d)",
                                tmp, in.available());
                    return null;
                }
                
                len = lenBytes.getInt();
                lenBytes.position(0);
                
                final byte[] val = new byte[len];
                num = in.read(val);
                
                if (num != len) {
                    
                    System.out.println("remaining: " + in.available());
                    
                    Logging
                            .logMessage(
                                Logging.LEVEL_ERROR,
                                Category.db,
                                this,
                                "an error occurred while trying to convert the database; expected value length: %d, actual value length: %d",
                                len, num);
                    return null;
                }
                
                return new Entry<byte[], byte[]>() {
                    
                    @Override
                    public byte[] setValue(byte[] value) {
                        throw new UnsupportedOperationException();
                    }
                    
                    @Override
                    public byte[] getValue() {
                        return val;
                    }
                    
                    @Override
                    public byte[] getKey() {
                        return key;
                    }
                };
                
            } catch (IOException exc) {
                
                Logging.logMessage(Logging.LEVEL_ERROR, Category.db, this,
                    "an error occurred while trying to convert the database: "
                        + OutputUtils.stackTraceToString(exc));
                
                return null;
            }
            
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        public void destroy() throws IOException {
            in.close();
        }
        
    }
    
    public static void initiateConversion(int dbVer, final BabuDBConfig cfg) throws BabuDBException {
        
        if (!DBWriter.checkVersionSupport(dbVer))
            throw new BabuDBException(ErrorCode.IO_ERROR, "on-disk format (version " + dbVer
                + ") is incompatible with this BabuDB release (version " + BabuDB.BABUDB_DB_FORMAT_VERSION
                + "); no automatic conversion possible");
        
        else
            Logging
                    .logMessage(Logging.LEVEL_INFO, Category.db, (Object) null,
                        "starting database conversion");
        
        final File dbDir = new File(cfg.getBaseDir());
        final File dbLogDir = new File(cfg.getDbLogDir());
        final File targetDir = new File(dbDir, ".conversion");
        final File cfgFile = new File(dbDir, cfg.getDbCfgFile());
        final File backupDir = new File(cfg.getBaseDir(), ".backup-" + dbVer);
        final File backupDBDir = new File(backupDir, "database");
        final File backupLogDir = new File(backupDir, "db-log");
        
        if (targetDir.exists())
            FSUtils.delTree(targetDir);
        
        try {
            
            // write version independent database representation
            DBWriter.writeDB(cfg, dbVer, targetDir.getAbsolutePath());
            
            // move everything except for the temporary dump and the config file
            // to a backup directory
            File[] dbFilesToMove = dbDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return !(pathname.equals(targetDir) || pathname.equals(cfgFile) || pathname
                            .equals(backupDir));
                }
            });
            
            File[] logFilesToMove = dbLogDir.equals(dbDir) ? new File[0] : dbLogDir.listFiles();
            
            backupDBDir.mkdirs();
            backupLogDir.mkdirs();
            if (backupDBDir.exists() && backupLogDir.exists()) {
                // move all files to a backup directory
                for (File f : dbFilesToMove)
                    if (!f.renameTo(new File(backupDBDir, f.getName())))
                        throw new BabuDBException(ErrorCode.IO_ERROR,
                            "an error occurred while trying to convert the database: '" + f.getAbsolutePath()
                                + "' could not be moved");
                
                for (File f : logFilesToMove)
                    if (!f.renameTo(new File(backupLogDir, f.getName())))
                        throw new BabuDBException(ErrorCode.IO_ERROR,
                            "an error occurred while trying to convert the database: '" + f.getAbsolutePath()
                                + "' could not be moved");
                
            } else
                throw new BabuDBException(ErrorCode.IO_ERROR,
                    "an error occurred while trying to convert the database: backup directory could not be created");
            
            // copy the config file to the backup directory
            FSUtils.copyTree(cfgFile, new File(backupDir, cfgFile.getName()));
            
        } catch (IOException exc) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "an error occurred while trying to convert the database", exc);
        }
        
    }
    
    public static void completeConversion(BabuDB babuDB) throws BabuDBException {
        
        final File targetDir = new File(babuDB.getConfig().getBaseDir(), "/.conversion");
        
        // get all databases to restore
        File[] dbsToRestore = targetDir.listFiles();
        
        try {
            
            // for each database directory ...
            for (File dbDir : dbsToRestore) {
                
                // get all nested index files
                File[] indexFiles = dbDir.listFiles(new FileFilter() {
                    public boolean accept(File pathname) {
                        return !pathname.isDirectory();
                    }
                });
                
                Database db = babuDB.getDatabaseManager().createDatabase(dbDir.getName(), indexFiles.length);
                for (File indexFile : indexFiles) {
                    
                    int indexId = Integer.parseInt(indexFile.getName());
                    
                    IndexFileIterator it = new IndexFileIterator(indexFile);
                    while (it.hasNext()) {
                        Entry<byte[], byte[]> next = it.next();
                        if (next == null) {
                            throw new BabuDBException(ErrorCode.INTERNAL_ERROR,
                                "database conversion failed, dump corrupted");
                        }
                        
                        db.singleInsert(indexId, next.getKey(), next.getValue(), null).get();
                    }
                    
                    it.destroy();
                }
                
                // retrieve all nested snapshot directories
                File snapRootDir = new File(dbDir, DBWriter.SNAPSHOT_DIR_NAME);
                File[] snapDirs = snapRootDir.listFiles();
                if (snapDirs == null)
                    continue;
                
                // for each snapshot directory ...
                for (File snapDir : snapDirs) {
                    
                    // TODO: restore snapshots
                    
                }
            }
            
        } catch (IOException exc) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "an error has occurred while trying to convert the database", exc);
        }
        
        // delete the version-independent dump
        FSUtils.delTree(targetDir);
        
        Logging.logMessage(Logging.LEVEL_INFO, Category.db, (Object) null, "conversion completed");
    }
    
}
