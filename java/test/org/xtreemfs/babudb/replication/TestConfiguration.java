/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import static org.xtreemfs.babudb.replication.Replication.*;

public class TestConfiguration {
    public static final int PORT = 34567;
    
    public static final InetSocketAddress master_address = new InetSocketAddress("localhost",MASTER_PORT);
    
    public static final InetSocketAddress slave1_address = new InetSocketAddress("localhost",SLAVE_PORT);
    
    public static final String master_baseDir = "/tmp/lsmdb-test/master/";
    
    public static final String slave1_baseDir = "/tmp/lsmdb-test/slave1/";
}
