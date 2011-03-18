/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.plugin;

import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;

/**
 * A plugin has to provide a class named <b>Main</b> implementing this interface to be loaded 
 * successfully. 
 * 
 * @author flangner
 * @date 11/03/2010
 */
public abstract class PluginMain {
    
    private final static String VERSION_PART_DELIMITER = ".";
    private final static String VERSION_RANGE_DELIMITER = "-";
        
    /**
     * The range of BabuDB versions this Plugin is compatible to.<br>
     * <br>
     * Pattern:<br> 
     * from-to<br>
     * <br>
     * from: 0-9.0-9.0-9<br>
     * to: 0-9.0-9.0-9<br>
     * <br>
     * If you are not sure about how to build a valid compatible-version string, please use the 
     * static method <b>PluginMain.buildCompatibleVersionString()</b>.
     * 
     * @return the pattern of BabuDBVersions this plugin is compatible to.
     */
    public abstract String compatibleBabuDBVersion();
    
    /**
     * Method to initialize the plugin and register its threads at 
     * {@link BabuDB}. 
     * 
     * @param configPath
     * @param babuDB
     * 
     * @throws BabuDBException if plugin could not be initialized successfully.
     * 
     * @return the modified babuDB or some implementation overloading the given 
     *         babuDB.
     */
    public abstract BabuDBInternal start(BabuDBInternal babuDB, String configPath) 
            throws BabuDBException;
    
    public final BabuDBInternal execute(BabuDBInternal babuDB, String configPath)
            throws BabuDBException {
        
        String[] comp = compatibleBabuDBVersion().split(VERSION_RANGE_DELIMITER);
        String[] f = comp[0].split(VERSION_PART_DELIMITER);
        int[] from = new int[] { Integer.parseInt(f[0]), Integer.parseInt(f[1], 
                                 Integer.parseInt(f[2])) };
        
        String[] t = comp[1].split(VERSION_PART_DELIMITER);
        int[] to = new int[] { Integer.parseInt(t[0]), Integer.parseInt(t[1], 
                               Integer.parseInt(t[2])) };
                             
        String[] b = BabuDBFactory.BABUDB_VERSION.split(VERSION_PART_DELIMITER);
        int[] babu = new int[] { Integer.parseInt(b[0]), Integer.parseInt(b[1]),
                                 Integer.parseInt(b[2])};
        
        for (int i = 0; i < 3; i++) {
            if (babu[i] > to[i] || babu[i] < from[i]) {
                throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, "This " +
                		"BabuDB ("+BabuDBFactory.BABUDB_VERSION+") " +
                	        "is not compatible with this plugin (" + 
                	        getClass().getName() + "), which requires " +
                	        "BabuDB to meet " + compatibleBabuDBVersion());
            }
        }
        return start(babuDB, configPath);
    }
    
    /**
     * Builds a compatible-version string for your plugin with the given boundaries. <br>
     * <br>
     * Your string will look like this, if you would provide the same values for the parameters as
     * given in their description:<br>
     * <br>
     * 1.2.3-4.5.6
     * 
     * @param from0 - 1
     * @param from1 - 2
     * @param from2 - 3
     * @param to0 - 4
     * @param to1 - 5
     * @param to2 - 6
     * @return a compatible-version string.
     */
    public static String buildCompatibleVersionString(int from0, int from1,int from2,
                                                      int to0, int to1, int to2) {
        
        return from0 + VERSION_PART_DELIMITER + from1 + VERSION_PART_DELIMITER + from2 + 
               VERSION_RANGE_DELIMITER +
               to0 + VERSION_PART_DELIMITER + to1 + VERSION_PART_DELIMITER + to2;
    }
}
