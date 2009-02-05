package org.xtreemfs.babudb.replication;

import java.io.File;

import org.xtreemfs.common.buffer.ReusableBuffer;

public class LSMDatabaseMOCK {

    public static File[] getAllFiles() {
        File[] files = new File[1];
        files[0] = new File("dummy");
        return files;
    }

    public static byte[] getChunk(String fileName, Long from, Long to) {
        return (fileName+":"+from+":"+to).getBytes();
    }

    public static void writeFileChunk(String fileName, ReusableBuffer data,
            long begin, long end) {
        // dummy
    }

}
