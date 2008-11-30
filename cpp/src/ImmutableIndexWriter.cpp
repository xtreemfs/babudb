// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "ImmutableIndexWriter.h"

#include <yield/platform/assert.h>
#include <yield/platform/memory_mapped_file.h>

#include "LogIndex.h"
#include "ImmutableIndex.h"
#include "babudb/KeyOrder.h"

using namespace babudb;

/** Persistent immutable index. Contains key-value data pairs.

	Uses the records of the SequentialFile. Three record types:
	- Offsets: array of fixed-length offsets, contains offsets
	  for the following key records
	- Key: a key
	- Value: a value

	These records are arranged like:

	[[Values] Offsets [Keys]] ... Offset_Offsets [Key]

	The last set of keys are an index into the file.

	TODO: lookup code might be simpler if we point to the last key
		  instead of the first in the range.
*/


void ImmutableIndexWriter::add(Data key, Data value) {
	record_buffer.push_back(std::pair<Data,Data>(key,value));
	data_in_buffer++;

	if(data_in_buffer >= chunk_size)
		flushBuffer();
}

void ImmutableIndexWriter::flushBuffer() {
	if(record_buffer.size() == 0)
		return;

	vector<offset_t> value_offsets;

	// write values and memorize offsets
	for(Buffer::iterator i = record_buffer.begin();
		i != record_buffer.end(); ++i) {
		void* target = writeData(i->second, RECORD_TYPE_VALUE);
		value_offsets.push_back(storage.pointer2offset(target));
	}

	// write offset array
	void* offsets_target = writeData(Data(&value_offsets[0],
			value_offsets.size() * sizeof(offset_t)), RECORD_TYPE_OFFSETS);
	offset_t offsets_offset = storage.pointer2offset(offsets_target);

	// write keys
	for(Buffer::iterator i = record_buffer.begin();
		i != record_buffer.end(); ++i) {
		writeData(i->first, RECORD_TYPE_KEY);
	}

	// memorize first key and offset to values
	index_keys.push_back(record_buffer.begin()->first);
	index_offsets.push_back(offsets_offset);

	record_buffer.clear();
	data_in_buffer = 0;
}

void* ImmutableIndexWriter::writeData(Data data, char type) {
	void* location = storage.getFreeSpace(data.size);
	memcpy(location, data.data, data.size);
	storage.frameData(location, data.size, type,false);
	return location;
}

void ImmutableIndexWriter::finalize() {
	flushBuffer();

	writeData(Data(&index_offsets[0],
			index_offsets.size() * sizeof(offset_t)), RECORD_TYPE_INDEX_OFFSETS);

	for(vector<Data>::iterator i = index_keys.begin(); i != index_keys.end(); i++) {
		writeData(*i, RECORD_TYPE_INDEX_KEY);
	}

	storage.commit();
	storage.close();
}

ImmutableIndexIterator::ImmutableIndexIterator(SequentialFile& file, bool end)
	: file(file), offset_table(0), key(file.end()) {
	if(!end)
		findNextOffsetTable(file.begin());
}

ImmutableIndexIterator::ImmutableIndexIterator(const ImmutableIndexIterator& o)
	: file(o.file), offset_table(o.offset_table), key(o.key), key_no(o.key_no) {}

ImmutableIndexIterator::ImmutableIndexIterator(SequentialFile& file, offset_t* table, SequentialFile::iterator i, int n)
	: file(file), offset_table(table), key(i), key_no(n) {}

void ImmutableIndexIterator::operator ++ () {
	ASSERT_TRUE(key != file.end());
	++key;
	++key_no;

	if(key.isType(RECORD_TYPE_INDEX_OFFSETS))
		key = file.end();
	else if(!key.isType(RECORD_TYPE_KEY)) {
		findNextOffsetTable(key);
	}
}

std::pair<Data,Data> ImmutableIndexIterator::operator * () {
	ASSERT_TRUE(offset_table != NULL);
	ASSERT_TRUE(key.getRecord()->getType() == RECORD_TYPE_KEY);
	SequentialFile::Record* key_rec = key.getRecord();
	SequentialFile::Record* value_rec = file.at(offset_table[key_no]).getRecord();

	return std::pair<Data,Data>(
		Data(key_rec->getPayload(), key_rec->getPayloadSize()),
		Data(value_rec->getPayload(), value_rec->getPayloadSize()));
}

bool ImmutableIndexIterator::operator != ( const ImmutableIndexIterator& other ) {
	return key != other.key;
}

bool ImmutableIndexIterator::operator == ( const ImmutableIndexIterator& other ) {
	return key == other.key;
}

void ImmutableIndexIterator::findNextOffsetTable(SequentialFile::iterator it) {
	while(it != file.end() && !it.isType(RECORD_TYPE_OFFSETS))
		++it;

	if(it != file.end()) {
		ASSERT_TRUE(it.getRecord()->getType() == RECORD_TYPE_OFFSETS);
		offset_table = (offset_t*)it.getRecord()->getPayload();
		++it;
		ASSERT_TRUE(it.getRecord()->getType() == RECORD_TYPE_KEY);
		key = it;
		key_no = 0;
	}
	else {
		offset_table = NULL;
		key = it;
		key_no = -1;
	}
}
