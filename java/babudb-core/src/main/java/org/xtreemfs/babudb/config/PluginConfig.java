/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.config;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Reading the Main class from a plugin's library.
 * 
 * @author flangner
 * @since 04/15/2011
 */
public class PluginConfig extends Config {
    
    protected String pluginLibraryPath;
    
    public PluginConfig(Properties prop) {
        super(prop);
        read();
    }
    
    public PluginConfig(String filename) throws IOException {
        super(filename);
        read();
    }
    
    // for compatibility only
    public PluginConfig () { }
    
    private final void read() {
        pluginLibraryPath = readRequiredString("plugin.jar");
        
        checkArgs(pluginLibraryPath);
    }
    
    private static void checkArgs(String pluginPath) {
        if (pluginPath == null || pluginPath == "") 
            throw new IllegalArgumentException("Path to plugin's library jar is missing.");
        
        File f = new File(pluginPath);
        if (!f.exists() || !f.isFile())
            throw new IllegalArgumentException("Path '" + pluginPath + "' does not exist, " +
            		"or is not a file.");
    }
    
    public String getPluginLibraryPath() {
        return pluginLibraryPath;
    }
}
