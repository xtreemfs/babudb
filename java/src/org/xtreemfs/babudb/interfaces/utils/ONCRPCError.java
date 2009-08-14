/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb.interfaces.utils;

import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;

/**
 *
 * @author bjko
 */
public class ONCRPCError extends ONCRPCException {
    private static final long serialVersionUID = -1913722310798941140L;
    
    final int accept_stat;
    
    public ONCRPCError(int accept_stat) {
        this.accept_stat = accept_stat;
    }

    public int getAcceptStat() {
        return accept_stat;
    }

    @Override
    public int getTag() {
        throw new RuntimeException("this exception must not be serialized");
    }

    @Override
    public String getTypeName() {
        return "ONCRPCError";
    }

    @Override
    public void serialize(ONCRPCBufferWriter writer) {
        throw new RuntimeException("this exception must not be serialized");
    }

    @Override
    public void deserialize(ReusableBuffer buf) {
        throw new RuntimeException("this exception must not be serialized");
    }

    @Override
    public int calculateSize() {
        throw new RuntimeException("this exception must not be serialized");
    }



}
