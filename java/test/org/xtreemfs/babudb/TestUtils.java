package org.xtreemfs.babudb;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: mikael
 * Date: Nov 22, 2009
 * Time: 7:36:58 PM
 * To change this template use File | Settings | File Templates.
 */
public final class TestUtils {
    public static final boolean deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDirectory(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }

}
