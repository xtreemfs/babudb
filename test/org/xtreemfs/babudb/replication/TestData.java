/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import org.xtreemfs.babudb.interfaces.ReplicationInterface.fleaseRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.heartbeatRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.loadRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.localTimeRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicaRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateRequest;

public final class TestData {
    public final static String testDB = "testDB";
    public final static int    testDBID = 1;
    public final static String copyTestDB = "copyDB";
    public final static int    testDBIndices = 2;
    
    public final static String testValue = "testVal";
    public final static String testKey1 = "key1";
    public final static String testKey2 = "key2";
    public final static String testKey3 = "key3";
    
    public static final int replicateOperation = new replicateRequest().getTag();
    public static final int heartbeatOperation = new heartbeatRequest().getTag();
    public static final int replicaOperation = new replicaRequest().getTag();
    public static final int loadOperation = new loadRequest().getTag();
    public static final int fleaseOperation = new fleaseRequest().getTag();
    public static final int localTimeOperation = new localTimeRequest().getTag();
}