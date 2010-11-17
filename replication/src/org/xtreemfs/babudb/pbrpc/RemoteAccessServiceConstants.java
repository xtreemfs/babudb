//automatically generated from replication.proto at Wed Nov 17 15:07:16 CET 2010
//(c) 2010. See LICENSE file for details.

package org.xtreemfs.babudb.pbrpc;

import com.google.protobuf.Message;

public class RemoteAccessServiceConstants {

    public static final int INTERFACE_ID = 10001;
    public static final int PROC_ID_MAKEPERSISTENT = 1;

    public static Message getRequestMessage(int procId) {
        switch (procId) {
           case 1: return null;
           default: throw new RuntimeException("unknown procedure id");
        }
    }


    public static Message getResponseMessage(int procId) {
        switch (procId) {
           case 1: return null;
           default: throw new RuntimeException("unknown procedure id");
        }
    }


}