//automatically generated from replication.proto at Wed Jan 05 14:46:41 CET 2011
//(c) 2011. See LICENSE file for details.

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