//automatically generated from replication.proto at Wed May 11 13:09:56 CEST 2011
//(c) 2011. See LICENSE file for details.

package org.xtreemfs.babudb.pbrpc;

import com.google.protobuf.Message;

public class RemoteAccessServiceConstants {

    public static final int INTERFACE_ID = 10001;
    public static final int PROC_ID_MAKEPERSISTENT = 1;
    public static final int PROC_ID_GETDATABASEBYNAME = 2;
    public static final int PROC_ID_GETDATABASEBYID = 3;
    public static final int PROC_ID_GETDATABASES = 4;
    public static final int PROC_ID_LOOKUP = 5;
    public static final int PROC_ID_PLOOKUP = 6;
    public static final int PROC_ID_PLOOKUPREVERSE = 7;
    public static final int PROC_ID_RLOOKUP = 8;
    public static final int PROC_ID_RLOOKUPREVERSE = 9;

    public static Message getRequestMessage(int procId) {
        switch (procId) {
           case 1: return null;
           case 2: return GlobalTypes.DatabaseName.getDefaultInstance();
           case 3: return GlobalTypes.DatabaseId.getDefaultInstance();
           case 4: return null;
           case 5: return GlobalTypes.Lookup.getDefaultInstance();
           case 6: return GlobalTypes.Lookup.getDefaultInstance();
           case 7: return GlobalTypes.Lookup.getDefaultInstance();
           case 8: return GlobalTypes.RangeLookup.getDefaultInstance();
           case 9: return GlobalTypes.RangeLookup.getDefaultInstance();
           default: throw new RuntimeException("unknown procedure id");
        }
    }


    public static Message getResponseMessage(int procId) {
        switch (procId) {
           case 1: return GlobalTypes.ErrorCodeResponse.getDefaultInstance();
           case 2: return GlobalTypes.Database.getDefaultInstance();
           case 3: return GlobalTypes.Database.getDefaultInstance();
           case 4: return GlobalTypes.Databases.getDefaultInstance();
           case 5: return GlobalTypes.ErrorCodeResponse.getDefaultInstance();
           case 6: return GlobalTypes.EntryMap.getDefaultInstance();
           case 7: return GlobalTypes.EntryMap.getDefaultInstance();
           case 8: return GlobalTypes.EntryMap.getDefaultInstance();
           case 9: return GlobalTypes.EntryMap.getDefaultInstance();
           default: throw new RuntimeException("unknown procedure id");
        }
    }


}