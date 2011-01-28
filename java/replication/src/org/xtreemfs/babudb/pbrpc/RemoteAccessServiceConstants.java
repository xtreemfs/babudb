//automatically generated from replication.proto at Fri Jan 28 12:07:07 CET 2011
//(c) 2011. See LICENSE file for details.

package org.xtreemfs.babudb.pbrpc;

import com.google.protobuf.Message;

public class RemoteAccessServiceConstants {

    public static final int INTERFACE_ID = 10001;
    public static final int PROC_ID_MAKEPERSISTENT = 1;
    public static final int PROC_ID_GETDATABASENAMES = 2;
    public static final int PROC_ID_LOOKUP = 3;
    public static final int PROC_ID_PLOOKUP = 4;
    public static final int PROC_ID_PLOOKUPREVERSE = 5;
    public static final int PROC_ID_RLOOKUP = 6;
    public static final int PROC_ID_RLOOKUPREVERSE = 7;

    public static Message getRequestMessage(int procId) {
        switch (procId) {
           case 1: return GlobalTypes.Type.getDefaultInstance();
           case 2: return null;
           case 3: return GlobalTypes.Lookup.getDefaultInstance();
           case 4: return GlobalTypes.Lookup.getDefaultInstance();
           case 5: return GlobalTypes.Lookup.getDefaultInstance();
           case 6: return GlobalTypes.RangeLookup.getDefaultInstance();
           case 7: return GlobalTypes.RangeLookup.getDefaultInstance();
           default: throw new RuntimeException("unknown procedure id");
        }
    }


    public static Message getResponseMessage(int procId) {
        switch (procId) {
           case 1: return GlobalTypes.ErrorCodeResponse.getDefaultInstance();
           case 2: return GlobalTypes.DatabaseNames.getDefaultInstance();
           case 3: return GlobalTypes.ErrorCodeResponse.getDefaultInstance();
           case 4: return GlobalTypes.EntryMap.getDefaultInstance();
           case 5: return GlobalTypes.EntryMap.getDefaultInstance();
           case 6: return GlobalTypes.EntryMap.getDefaultInstance();
           case 7: return GlobalTypes.EntryMap.getDefaultInstance();
           default: throw new RuntimeException("unknown procedure id");
        }
    }


}