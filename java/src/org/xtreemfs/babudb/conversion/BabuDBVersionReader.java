/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.conversion;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.xtreemfs.foundation.logging.Logging;

/**
 * A class loader that loads classes from a BabuDB JAR.
 * 
 * @author stender
 * 
 */
public class BabuDBVersionReader extends ClassLoader {
    
    private Map<String, byte[]> classes;
    
    private Object              babuDB;
    
    /**
     * Creates a BabuDB JAR class loader for the given BabuDB data version. It
     * will automatically attempt to load the JAR with the given data version
     * number from 'org.xtreemfs.babudb.conversion.jars'.
     * 
     * @param ver
     *            the data version
     * @param cfgProps
     *            the configuration properties needed to access the old version
     *            of the database
     * @throws IOException
     *             if an I/O error occurred
     */
    public BabuDBVersionReader(int ver, Properties cfgProps) throws IOException {
        
        super(null);
        
        this.classes = new HashMap<String, byte[]>();
        
        // load all classes from JAR
        JarInputStream jis = new JarInputStream(getClass().getResourceAsStream("jars/" + ver + ".jar"));
        for (;;) {
            
            JarEntry next = jis.getNextJarEntry();
            
            if (next == null)
                break;
            
            if (!next.getName().endsWith(".class"))
                continue;
            
            byte[] buf = new byte[4096];
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            
            for (;;) {
                int len = jis.read(buf);
                if (len <= 0)
                    break;
                out.write(buf, 0, len);
            }
            
            String className = next.getName().substring(0, next.getName().length() - ".class".length())
                    .replace('/', '.');
            
            classes.put(className, out.toByteArray());
        }
        
        jis.close();
        
        this.babuDB = initBabuDB(cfgProps);
    }
    
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        
        byte[] classBytes = classes.get(name);
        
        if (classBytes == null)
            return findSystemClass(name);
        
        Class<?> clazz = defineClass(name, classBytes, 0, classBytes.length);
        
        if (resolve)
            resolveClass(clazz);
        
        return clazz;
    }
    
    public Iterator<Entry<byte[], byte[]>> getIndexContent(String dbName, int indexId) {
        
        try {
            
            // retrieve the database manager
            Object dbManager = babuDB.getClass().getMethod("getDatabaseManager").invoke(babuDB);
            
            // retrieve the database
            Object database = dbManager.getClass().getMethod("getDatabase", String.class).invoke(dbManager,
                dbName);
            
            // perform a prefix lookup for the complete index
            Object query = database.getClass().getMethod("prefixLookup", int.class, byte[].class,
                Object.class).invoke(database, indexId, new byte[0], null);
            
            // get the iterator from the query object
            Iterator<Entry<byte[], byte[]>> it = (Iterator<Entry<byte[], byte[]>>) query.getClass()
                    .getMethod("get").invoke(query);
            
            return it;
            
        } catch (Exception exc) {
            Logging.logError(Logging.LEVEL_CRIT, this, exc);
            return null;
        }
    }
    
    public void shutdown() {
        
        if (babuDB == null)
            return;
        
        try {
            babuDB.getClass().getMethod("shutdown").invoke(babuDB);
            babuDB = null;
        } catch (Exception exc) {
            Logging.logError(Logging.LEVEL_ERROR, this, exc);
        }
    }
    
    public Set<String> getAllDatabases() {
        
        try {
            
            // retrieve the database manager
            Object dbManager = babuDB.getClass().getMethod("getDatabaseManager").invoke(babuDB);
            
            // get the list of databases
            Map<String, ?> map = (Map<String, ?>) dbManager.getClass().getMethod("getDatabases").invoke(
                dbManager);
            
            return map.keySet();
            
        } catch (Exception exc) {
            Logging.logError(Logging.LEVEL_ERROR, this, exc);
            return null;
        }
    }
    
    public int getNumIndics(String dbName) {
        
        try {
            
            // retrieve the database manager
            Object dbManager = babuDB.getClass().getMethod("getDatabaseManager").invoke(babuDB);
            
            // retrieve the database
            Object database = dbManager.getClass().getMethod("getDatabase", String.class).invoke(dbManager,
                dbName);
            
            int i = 0;
            try {
                for (;; i++) {
                    
                    // perform a prefix lookup for the complete index
                    Object query = database.getClass().getMethod("lookup", int.class, byte[].class,
                        Object.class).invoke(database, i, new byte[0], null);
                    
                    // get the iterator from the query object
                    query.getClass().getMethod("get").invoke(query);
                }
            } catch (Exception exc) {
                // ignore
            }
            
            return i;
            
        } catch (Exception exc) {
            Logging.logError(Logging.LEVEL_ERROR, this, exc);
            return 0;
        }
    }
    
    public String[] getAllSnapshots(String dbName) {
        
        try {
            
            // retrieve the database manager
            Object snapManager = babuDB.getClass().getMethod("getSnapshotManager").invoke(babuDB);
            
            // retrieve the database
            return (String[]) snapManager.getClass().getMethod("getAllSnapshots", String.class).invoke(
                snapManager, dbName);
            
        } catch (Exception exc) {
            Logging.logError(Logging.LEVEL_ERROR, this, exc);
            return null;
        }
        
    }
    
    public Iterator<Entry<byte[], byte[]>> getIndexContent(String dbName, String snapName, int indexId) {
        
        try {
            
            // retrieve the database manager
            Object snapManager = babuDB.getClass().getMethod("getSnapshotManager").invoke(babuDB);
            
            // retrieve the database
            Object database = snapManager.getClass().getMethod("getSnapshotDB", String.class, String.class)
                    .invoke(snapManager, dbName, snapName);
            
            // perform a prefix lookup for the complete index
            Object query = database.getClass().getMethod("prefixLookup", int.class, byte[].class,
                Object.class).invoke(database, indexId, new byte[0], null);
            
            // get the iterator from the query object
            Iterator<Entry<byte[], byte[]>> it = (Iterator<Entry<byte[], byte[]>>) query.getClass()
                    .getMethod("get").invoke(query);
            
            return it;
            
        } catch (Exception exc) {
            Logging.logError(Logging.LEVEL_CRIT, this, exc);
            return null;
        }
    }
    
    /**
     * Checks if a certain data version JAR is supported by the class loader.
     * 
     * @param ver
     *            the version to be checked
     * 
     * @return <code>true</code>, if it is supported, <code>false</code>,
     *         otherwise
     */
    public static boolean checkVersionSupport(int ver) {
        
        JarInputStream jis = null;
        try {
            jis = new JarInputStream(BabuDBVersionReader.class.getResourceAsStream("jars/" + ver + ".jar"));
            return true;
        } catch (Exception exc) {
            return false;
        } finally {
            if (jis != null)
                try {
                    jis.close();
                } catch (IOException exc) {
                    // ignore
                }
        }
        
    }
    
    protected Object initBabuDB(Properties cfgProps) {
        
        try {
            
            assert (babuDB == null);
            
            // create a BabuDB configuration
            Class<?> cfgCls = loadClass("org.xtreemfs.babudb.config.BabuDBConfig");
            Object cfg = cfgCls.getConstructor(Properties.class).newInstance(cfgProps);
            
            // create a BabuDB instance
            Class<?> babuDBFactoryCls = loadClass("org.xtreemfs.babudb.BabuDBFactory");
            return babuDBFactoryCls.getMethod("createBabuDB", cfgCls).invoke(null, cfg);
            
        } catch (Exception exc) {
            Logging.logError(Logging.LEVEL_ERROR, this, exc);
            return null;
        }
    }
    
}
