/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.tools;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.config.ConfigBuilder;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.foundation.util.OutputUtils;

/**
 * A tool that creates a complete dump of a database.
 * 
 * @author stenjan
 * 
 */
public class DBDumpTool {
    
    /**
     * Generates string representations out of byte arrays.
     * 
     * @author stenjan
     * 
     */
    public interface RecordFormatter {
        
        /**
         * Formats a specific key stored in a database.
         * 
         * @param key
         *            the key
         * @return a formatted string representation of the key
         */
        public String formatKey(byte[] key, String databaseName, int indexId);
        
        /**
         * Formats a specific key stored in a database.
         * 
         * @param key
         *            the key
         * @return a formatted string representation of the key
         */
        public String formatValue(byte[] value, byte[] key, String databaseName, int indexId);
        
    }
    
    /**
     * Creates an XML dump of a database.
     * 
     * @param dbDir
     *            the database directory
     * @param logDir
     *            the database log directory
     * @param compression
     *            if compression is enabled
     * @param formatter
     *            the record formatter. The formatter has to ensure that all
     *            returned strings are XML-compliant, which implies that
     *            non-printable and XML characters have to be escaped according
     *            to the rules of well-formed XML documents.
     * @param out
     *            the stream to which the dump is written
     * @throws BabuDBException
     *             if an error occurs while dumping the database
     */
    public static void dumpDB(String dbDir, String logDir, boolean compression, RecordFormatter formatter,
        PrintStream out) throws BabuDBException {
        
        BabuDB databaseSystem = BabuDBFactory.createBabuDB(new ConfigBuilder().setDataPath(dbDir, logDir)
                .setCompressed(compression).build());
        
        out.println("<BabuDB version=\"" + BabuDB.BABUDB_VERSION + "\">");
        
        Map<String, Database> dbs = databaseSystem.getDatabaseManager().getDatabases();
        
        long entryCount = 0;
        for (Entry<String, Database> entry : dbs.entrySet()) {
            
            Database db = entry.getValue();
            out.println("\t<database name=\"" + db.getName() + "\">");
            
            int numIndices = db.getComparators().length;
            for (int i = 0; i < numIndices; i++) {
                
                out.println("\t\t<index number=\"" + i + "\">");
                
                Iterator<Entry<byte[], byte[]>> it = db.prefixLookup(i, new byte[0], null).get();
                while (it.hasNext()) {
                    Entry<byte[], byte[]> next = it.next();
                    out.println("\t\t\t<key>");
                    out.println("\t\t\t\t" + formatter.formatKey(next.getKey(), db.getName(), i));
                    out.println("\t\t\t</key>");
                    out.println("\t\t\t<value>");
                    out.println("\t\t\t\t"
                        + formatter.formatValue(next.getValue(), next.getKey(), db.getName(), i));
                    out.println("\t\t\t</value>");
                    
                    entryCount++;
                }
                
                out.println("\t\t</index>");
            }
            
            out.println("\t</database>");
            
        }
        
        out.println("</BabuDB>");
        
        databaseSystem.shutdown();
    }
    
    public static void main(String[] args) throws Exception {
        
        if (args.length < 2 || args.length > 3) {
            System.out.println("usage: " + DBDumpTool.class.getSimpleName()
                + " <DB directory> <DB log directory> [dump file]");
            return;
        }
        
        final String dbDir = args[0];
        final String logDir = args[1];
        
        final PrintStream out = args.length == 3 ? new PrintStream(args[2]) : System.out;
        
        final RecordFormatter formatter = new RecordFormatter() {
            
            public String formatValue(byte[] value, byte[] key, String databaseName, int indexId) {
                return OutputUtils.byteArrayToHexString(value);
            }
            
            public String formatKey(byte[] key, String databaseName, int indexId) {
                return OutputUtils.byteArrayToHexString(key);
            }
        };
        
        dumpDB(dbDir, logDir, false, formatter, out);
    }
    
}
