/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class to read the dependency paths from properties file.
 * 
 * @author flangner
 * @since 04/01/2011
 */
public class DependencyConfig extends Config {

    private List<String> paths = new ArrayList<String>();
    
    public DependencyConfig(String path) throws IOException {
        super(path);
        read();
    }
    
    private void read() {
        int i = 0;
        String path;
        while ((path = readOptionalString("babudb.repl.dependency." + i++, null)) != null) {
            paths.add(path);
        }
    }
    
    public String[] getDependencyPaths() {
        return paths.toArray(new String[paths.size()]);
    }
}
