/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.plugin;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.plugin.PluginMain;
import org.xtreemfs.foundation.logging.Logging;

import static org.xtreemfs.babudb.BabuDBFactory.*;

/**
 * {@link ClassLoader} for accessing optional plugins for BabuDB.
 * Plugins may access BabuDB via BabuDB Main.start(BabuDBInternal babuDB).
 * 
 * @author flangner
 * @date 11/01/2010
 */
public final class PluginLoader extends ClassLoader {

    private final Map<String, byte[]>   clazzes = new HashMap<String, byte[]>();
        
    private BabuDBInternal              babuDB;
    
    /**
     * Creates a BabuDB JAR class loader for the plugins defined within the
     * configuration file. It will automatically attempt to load its JAR 
     * for the given data version.
     * 
     * @param babuDBImpl
     * 
     * @throws IOException if an I/O error occurred
     */
    private PluginLoader(BabuDBInternal dbs) throws IOException {
        super();
        
        this.babuDB = dbs;
        
        // start all plugins registered
        for (Entry<String, String> plugin : dbs.getConfig().getPlugins().entrySet()) {
            
            String main = null;
            String pluginPath = plugin.getKey();
            String configPath = plugin.getValue();
            try {
                // load all classes from the plugin JARs
                main = loadJar(pluginPath, "Main");
                if (main == null) {
                    throw new Exception("main class (extending PluginMain) not found!");
                }
                
                // load the plugins dependencies
                PluginMain m = (PluginMain) loadClass(main).newInstance();
                for (String depPath : m.getDependencies(configPath)) {
                    Logging.logMessage(Logging.LEVEL_INFO, this, "Loading plugin dependency %s.", 
                            depPath);                 
                    loadJar(depPath);
                }
                
                // start the plugin
                babuDB = m.execute(babuDB, configPath);
            
            } catch (Exception e) {
                if (e.getMessage() == null) Logging.logError(Logging.LEVEL_WARN, this, e);
                throw new IOException("Plugin at '" + pluginPath + "' for version " + BABUDB_VERSION 
                        + ((configPath != null) ? " with config at path " + configPath : "") 
                        + " could not be initialized, because " + e.getMessage() 
                        + "!", e.getCause());
            }
        }
    }
    
    /**
     * Loads all classes from the jar found at path to be available to the ClassLoader.
     * 
     * @param path
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    private final void loadJar(String path) throws FileNotFoundException, IOException {
        loadJar(path, null);
    }
    
    /**
     * Loads all classes from the jar found at path to be available to the ClassLoader.
     * 
     * @param path
     * @param classToSearchFor
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    private final String loadJar(String path, String classToSearchFor) throws FileNotFoundException, 
            IOException {
        
        JarInputStream jis = new JarInputStream(new FileInputStream(path)); 
        
        JarEntry next = null;
        String main = null;
        while ((next = jis.getNextJarEntry()) != null) {
            
            if (!next.getName().endsWith(".class")) {
                continue;
            }
            
            byte[] buf = new byte[4096];
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            
            int len = -1;
            while ((len = jis.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            
            String className = next.getName().substring(0, next.getName().length() - 
                    ".class".length()).replace('/', '.');
                        
            if (classToSearchFor != null && className.endsWith(classToSearchFor)) {
                assert (main == null);
                main = className;
            }
            
            if (!clazzes.containsKey(className)) {
                clazzes.put(className, out.toByteArray());
            } else {
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "Did not load %s from %s, " +
                		"because it already exists.", className, path);
            }
            out.close();
        }
        jis.close();
        
        return main;
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.ClassLoader#loadClass(java.lang.String, boolean)
     */
    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                
        Class<?> clazz = findLoadedClass(name);        
        if (clazz == null) {
            byte[] classBytes = clazzes.get(name);
            if (classBytes != null) {
                clazz = defineClass(name, classBytes, 0, classBytes.length);
            } else {
                clazz = getParent().loadClass(name);
            }
        }
        
        if (resolve) {
            resolveClass(clazz);
        }
        
        return clazz;
    }
    
    /**
     * This methods loads all available plugins and allows the plugins to overload the BabuDB API.
     * 
     * @param babuDB
     * @return the overloaded BabuDB.
     * 
     * @throws IOException if a plugin could not be loaded.
     */
    public final static BabuDBInternal init(BabuDBInternal babuDB) throws IOException {
        
        PluginLoader loader = new PluginLoader(babuDB);
        return loader.babuDB;
    }
}
