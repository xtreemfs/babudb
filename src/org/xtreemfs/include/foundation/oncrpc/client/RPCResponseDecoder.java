/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.include.foundation.oncrpc.client;

import org.xtreemfs.include.common.buffer.ReusableBuffer;

/**
 *
 * @author bjko
 */
public interface RPCResponseDecoder<V> {

    public V getResult(ReusableBuffer data);

}
