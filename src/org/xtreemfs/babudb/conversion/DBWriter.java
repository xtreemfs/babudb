/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.conversion;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.config.BabuDBConfig;

public class DBWriter {
    
    public static final String SNAPSHOT_DIR_NAME = "snapshots";
    
    /**
     * Checks if a certain data version is supported by the writer.
     * 
     * @param ver
     *            the version to be checked
     * 
     * @return <code>true</code>, if it is supported, <code>false</code>,
     *         otherwise
     */
    public static boolean checkVersionSupport(int ver) {
        return BabuDBVersionReader.checkVersionSupport(ver);
    }
    
    /**
     * <p>
     * This method creates version independent representations of all indices
     * located in the database directory specified in the given configuration.
     * The representations is written to separate files, where each file is
     * named like the number of the index it belongs to.
     * 
     * <p>
     * The files are located in subdirectories of the given target directory,
     * where each subdirectory is named like the database it belongs to.
     * </p>
     * 
     * <p>
     * Snapshots are represented alike, as subdirectories with the name of the
     * snapshot located in a subdirectory of the database directory with the
     * name specified in <code>SNAPSHOT_DIR_NAME</code>.
     * </p>
     * 
     * <p>
     * An example of the content of a temporary directory called '/tmp':
     * 
     * <pre>
     *   /tmp/db1/1
     *   /tmp/db2/1
     *   /tmp/db2/2
     *   /tmp/db2/3
     *   /tmp/db2/snapshots/snap1/1
     *   /tmp/db2/snapshots/snap1/2
     * </pre>
     * 
     * </p>
     * 
     * <p>
     * For the version independent representation of an index, we chose a stream
     * of the following format:
     * 
     * <pre>
     *   keylength: 4 bytes | key: keylenth bytes | valuelength: 4 bytes | value: valuelength bytes ...
     * </pre>
     * 
     * </p>
     * 
     * @param cfg
     *            configuration parameters containing information like index and
     *            log file locations and encoding parameters
     * @param ver
     *            the data version from which the database needs to be converted
     * @param targetDir
     *            the target directory to which the version independent
     *            representation is written
     * @exception IOException
     *                if an I/O error occurs while writing the version
     *                independent representation
     */
    public static void writeDB(BabuDBConfig cfg, int ver, String targetDir) throws IOException {
        
        BabuDBVersionReader reader = null;
        
        try {
            
            File trgDir = new File(targetDir);
            trgDir.mkdirs();
            
            // create a reader for the given BabuDB version
            reader = new BabuDBVersionReader(ver, cfg.getProps());
            
            // write all databases
            for (String dbName : reader.getAllDatabases()) {
                
                File dbDir = new File(trgDir, dbName);
                dbDir.mkdir();
                
                // write all indices
                for (int i = 0; i < reader.getNumIndics(dbName); i++)
                    writeIndexFile(dbName, null, i, new File(dbDir, i + ""), reader);
                
                // write all snapshots
                
                String[] snaps = reader.getAllSnapshots(dbName);
                if (snaps.length == 0)
                    continue;
                
                for (String snapName : snaps)
                    for (int i = 0; i < reader.getNumIndics(dbName); i++)
                        writeIndexFile(dbName, snapName, i, new File(dbDir, i + ""), reader);
            }
            
        } finally {
            if (reader != null)
                reader.shutdown();
        }
        
    }
    
    private static void writeIndexFile(String dbName, String snapName, int indexId, File targetFile,
        BabuDBVersionReader reader) throws IOException {
        
        OutputStream out = new FileOutputStream(targetFile);
        
        ByteBuffer lenBytes = ByteBuffer.wrap(new byte[4]);
        
        Iterator<Entry<byte[], byte[]>> it = snapName == null ? reader.getIndexContent(dbName, indexId)
            : reader.getIndexContent(dbName, snapName, indexId);
        while (it.hasNext()) {
            
            Entry<byte[], byte[]> next = it.next();
            
            // write key length + key
            lenBytes.putInt(next.getKey().length);
            lenBytes.position(0);
            out.write(lenBytes.array());
            out.write(next.getKey());
            
            // write value length + value
            lenBytes.putInt(next.getValue().length);
            lenBytes.position(0);
            out.write(lenBytes.array());
            out.write(next.getValue());
        }
        
        out.close();
    }
}
