package org.xtreemfs.babudb.index.reader;

import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.ByteRange;

public interface BlockReader {

	public abstract BlockReader clone();
	
	public abstract ByteRange lookup(byte[] key);

	public abstract Iterator<Entry<ByteRange, ByteRange>> rangeLookup(
			byte[] from, byte[] to, final boolean ascending);

	public abstract MiniPage getKeys();

	public abstract MiniPage getValues();

	public abstract int getNumEntries();

}