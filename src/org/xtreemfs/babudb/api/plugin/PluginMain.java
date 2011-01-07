/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.plugin;

import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exception.BabuDBException;

/**
 * A plugin has to provide a class named main implementing this interface to be
 * loaded successfully. 
 * 
 * @author flangner
 * @date 11/03/2010
 */
public interface PluginMain {
    
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
    public BabuDBInternal start(BabuDBInternal babuDB, String configPath) 
            throws BabuDBException;
}
