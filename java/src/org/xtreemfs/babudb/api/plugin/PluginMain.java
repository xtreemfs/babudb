/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.plugin;

import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;

/**
 * A plugin has to provide a class named main implementing this interface to be
 * loaded successfully. 
 * 
 * @author flangner
 * @date 11/03/2010
 */
public abstract class PluginMain {
    
    /**
     * The range of BabuDB versions this Plugin is compatible to.
     * 
     * Pattern: 
     * from|to
     * from: 0-9.0-9.0-9
     * to: 0-9.0-9.0-9
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
        
        String[] comp = compatibleBabuDBVersion().split("|");
        String[] f = comp[0].split(".");
        int[] from = new int[] { Integer.parseInt(f[0]), Integer.parseInt(f[1], 
                                 Integer.parseInt(f[2])) };
        
        String[] t = comp[1].split(".");
        int[] to = new int[] { Integer.parseInt(t[0]), Integer.parseInt(t[1], 
                               Integer.parseInt(t[2])) };
                             
        String[] b = BabuDBFactory.BABUDB_VERSION.split(".");
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
}
