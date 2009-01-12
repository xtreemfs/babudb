// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "ImmutableIndex.h"

#include "ImmutableIndexWriter.h"
#include "SequentialFile.h"

#include "yield/platform/memory_mapped_file.h"
#include "yield/platform/assert.h"

using namespace babudb;

ImmutableIndex::ImmutableIndex(auto_ptr<YIELD::MemoryMappedFile> mm, KeyOrder& order, lsn_t lsn)
: storage(mm), order(order), latest_lsn(lsn), index(MapCompare(order)) {}

bool ImmutableIndex::isIntact() {
	SequentialFile::iterator cursor = storage.rbegin();

	return cursor != storage.rend() && cursor.getRecord()->getType() == RECORD_TYPE_FILE_FOOTER;
}

void ImmutableIndex::load() {
	ASSERT_TRUE(isIntact());
	loadIndex(storage, index);
}

ImmutableIndex::Tree::iterator ImmutableIndex::findChunk(const Data& key) {
	if(index.size() == 0)
		return index.end();

	Tree::iterator cursor = index.lower_bound(key);
	// cursor is equal or larger than key

	if(cursor == index.end()) {
		--cursor; // we might still be in the last partition
	}
	else if(order.less(key,cursor->first)) {
		// cursor is larger than key

		if(cursor != index.begin())
			--cursor;
	}

	return cursor;
}

offset_t* ImmutableIndex::getOffsetTable(offset_t offset_rec_offset) {
	SequentialFile::Record* offsets_rec = storage.offset2record(offset_rec_offset);
	ASSERT_TRUE(offsets_rec->getType() == RECORD_TYPE_OFFSETS);
	offset_t* value_offsets = (offset_t*)offsets_rec->getPayload();
	return value_offsets;
}

Data ImmutableIndex::lookup(Data search_key) {
	ImmutableIndexIterator i = find(search_key);

	if(i != end()) { // i >= search_key
		if(!order.less(search_key, (*i).first)) // ==
			return (*i).second;
		else
			return Data::Empty();
	}
	else
		return Data::Empty();
}

ImmutableIndex::iterator ImmutableIndex::find(Data search_key) {
	Tree::iterator cursor = findChunk(search_key);

	if(cursor == index.end())
		return end(); // not found

	offset_t* value_offsets = getOffsetTable(cursor->second);

	SequentialFile::iterator file_cursor = storage.at(cursor->second);
	++file_cursor; // go to the keys and do linear search
	ASSERT_TRUE(file_cursor.isType(RECORD_TYPE_KEY));

	int record_count = 0;
	for(; file_cursor != storage.end() && file_cursor.isType(RECORD_TYPE_KEY); ++file_cursor) {
		Data key(file_cursor.getRecord()->getPayload(), file_cursor.getRecord()->getPayloadSize());

		if(!order.less(key,search_key)) 		// found key >= search_key
			return ImmutableIndex::iterator(storage,value_offsets,file_cursor,record_count);

		record_count++;
	}

	return end(); // not found
}

ImmutableIndexIterator ImmutableIndex::begin() {
	return ImmutableIndexIterator(storage,false);
}

ImmutableIndexIterator ImmutableIndex::end() {
	return ImmutableIndexIterator(storage,true);
}

bool ImmutableIndex::loadIndex(SequentialFile& storage, std::map<Data,offset_t,MapCompare>&  index) {
	SequentialFile::iterator cursor = storage.rbegin();

	if(cursor.getRecord()->getType() != RECORD_TYPE_FILE_FOOTER)
		return false;

	++cursor; // skip footer

	for(; cursor != storage.rend(); ++cursor) {
		if(cursor.getRecord()->getType() == RECORD_TYPE_INDEX_OFFSETS)
			break;
	}

	offset_t* offsets = (offset_t*)cursor.getRecord()->getPayload();
	size_t no_offsets = cursor.getRecord()->getPayloadSize()/sizeof(offset_t);

	cursor.reverse();
	++cursor;
	ASSERT_TRUE(cursor.getRecord()->getType() == RECORD_TYPE_INDEX_KEY);

	size_t record_count = 0;
	for(; cursor != storage.end(); ++cursor) {	
		if(cursor.getRecord()->getType() != RECORD_TYPE_FILE_FOOTER) {
			Data key(cursor.getRecord()->getPayload(),
					 cursor.getRecord()->getPayloadSize());

			ASSERT_TRUE(record_count < no_offsets);

			index.insert(pair<Data,offset_t>(key,offsets[record_count]));
			record_count++;
		}
	}

	return true;
}
