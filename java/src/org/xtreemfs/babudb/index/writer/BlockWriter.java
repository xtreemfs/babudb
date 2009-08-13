package org.xtreemfs.babudb.index.writer;

import org.xtreemfs.include.common.buffer.ReusableBuffer;

public interface BlockWriter {

	public abstract void add(byte[] key, byte[] value);

	public abstract ReusableBuffer serialize();

	public abstract byte[] getBlockKey();

}