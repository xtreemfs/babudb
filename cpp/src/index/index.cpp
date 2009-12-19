// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "index.h"
#include "util.h"

#include "index_writer.h"
#include "babudb/log/sequential_file.h"

#include "yield/platform/memory_mapped_file.h"
#include "yield/platform/assert.h"
#include "yield/platform/directory_walker.h"
#include "yield/platform/memory_mapped_file.h"
#include "yield/platform/disk_operations.h"

#include <sstream>
#include <algorithm>

#define POSIX  // for O_ definitions from fcntl.h
#include <fcntl.h>

using namespace babudb;

ImmutableIndex::ImmutableIndex(auto_ptr<YIELD::MemoryMappedFile>  mm,
                               const KeyOrder& order, lsn_t lsn)
    : storage(mm), order(order), latest_lsn(lsn), index(MapCompare(order)) {}

ImmutableIndex* ImmutableIndex::Load(const string& name, lsn_t lsn, const KeyOrder& order) {
  auto_ptr<YIELD::MemoryMappedFile> mmap;
  mmap.reset(new YIELD::MemoryMappedFile(
      name, 1024 * 1024, O_RDONLY|O_SYNC));
  if (mmap.get() == NULL)
     return NULL;

  ImmutableIndex* result = new ImmutableIndex(mmap, order, lsn);
  if (!result->LoadRoot()) {
    delete result;
    return false;
  }
  return result;
}

ImmutableIndexWriter* ImmutableIndex::Create(
    const string& name, lsn_t lsn, size_t chunk_size) {
	std::ostringstream file_name;
	file_name << name << "_" << lsn << ".idx";
	auto_ptr<YIELD::MemoryMappedFile> mfile;
  mfile.reset(new YIELD::MemoryMappedFile(file_name.str(), 1024*1024, O_CREAT|O_RDWR|O_SYNC));
  if (!mfile.get()) {
    return NULL;
  }
	ImmutableIndexWriter* result = new ImmutableIndexWriter(mfile, chunk_size);
  return result;
}

ImmutableIndex::Tree::iterator ImmutableIndex::findChunk(const Buffer& key) {
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

Buffer ImmutableIndex::Lookup(Buffer search_key) {
	ImmutableIndexIterator i = Find(search_key);

	if(i != end()) { // i >= search_key
		if(!order.less(search_key, (*i).first)) // ==
			return (*i).second;
		else
			return Buffer::Empty();
	}
	else
		return Buffer::Empty();
}

ImmutableIndex::iterator ImmutableIndex::Find(Buffer search_key) {
	Tree::iterator cursor = findChunk(search_key);

	if(cursor == index.end())
		return end(); // not found

	offset_t* value_offsets = getOffsetTable(cursor->second);

	SequentialFile::iterator file_cursor = storage.at(cursor->second);
	++file_cursor; // go to the keys and do linear search
	ASSERT_TRUE(file_cursor.isType(RECORD_TYPE_KEY));

	int record_count = 0;
	for(; file_cursor != storage.end() && file_cursor.isType(RECORD_TYPE_KEY); ++file_cursor) {
		Buffer key(file_cursor.getRecord()->getPayload(), file_cursor.getRecord()->getPayloadSize());

		if(!order.less(key,search_key)) 		// found key >= search_key
			return ImmutableIndex::iterator(storage,value_offsets,file_cursor,record_count);

		record_count++;
	}

	return end(); // not found
}

ImmutableIndexIterator ImmutableIndex::begin() const {
	return ImmutableIndexIterator(storage,false);
}

ImmutableIndexIterator ImmutableIndex::end() const {
	return ImmutableIndexIterator(storage,true);
}

bool ImmutableIndex::LoadRoot() {
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
			Buffer key(cursor.getRecord()->getPayload(),
					 cursor.getRecord()->getPayloadSize());

			ASSERT_TRUE(record_count < no_offsets);

			index.insert(pair<Buffer,offset_t>(key,offsets[record_count]));
			record_count++;
		}
	}

	return true;
}

ImmutableIndex::DiskIndices ImmutableIndex::FindIndices(const string& name_prefix) {
	DiskIndices results;

	pair<YIELD::Path,YIELD::Path> dir_prefix = YIELD::Path(name_prefix).split();
	YIELD::DirectoryWalker walker(dir_prefix.first);

	while(walker.hasNext()) {
		auto_ptr<YIELD::DirectoryEntry> entry = walker.getNext();

		lsn_t lsn;
		if(matchFilename(entry->getPath(), dir_prefix.second.getHostCharsetPath(), "idx", lsn)) {
			results.push_back(make_pair(entry->getPath(), lsn));
		}
	}

	return results;
}

static bool MoreRecent(const std::pair<YIELD::Path,lsn_t>& one,
                const std::pair<YIELD::Path,lsn_t> two) {
  return one.second > two.second;
}

ImmutableIndex* ImmutableIndex::LoadLatestIntactIndex(DiskIndices& on_disk, const KeyOrder& order) {
	// find latest intact ImmutableIndex
	lsn_t latest_intact_lsn = 0;
	YIELD::Path latest_intact_path("");
  sort(on_disk.begin(), on_disk.end(), MoreRecent);

	for (DiskIndices::iterator i = on_disk.begin(); i != on_disk.end(); ++i) {
    ImmutableIndex* result = Load(i->first, i->second, order);
    if (result)
      return result;
	}
  return NULL;
}

void ImmutableIndex::CleanupObsolete(const string& file_name, const string& to) {
	DiskIndices on_disk = FindIndices(file_name);
  lsn_t current_lsn = GetLastLSN();

	for (DiskIndices::iterator i = on_disk.begin(); i != on_disk.end(); ++i) {
		if (i->second < current_lsn) {  // not the latest intact index
			pair<YIELD::Path,YIELD::Path> parts = i->first.split();
      YIELD::DiskOperations::rename(i->first, to + parts.second.getHostCharsetPath());
		}
	}
}
