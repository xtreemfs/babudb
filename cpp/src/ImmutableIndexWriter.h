// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef MMUTABLEINDEXWRITER_H
#define MMUTABLEINDEXWRITER_H

#include <memory>
#include <utility>
#include <vector>

#include "babudb/Operation.h"
#include "babudb/KeyOrder.h"
#include "SequentialFile.h"

namespace YIELD { class MemoryMappedFile; }

namespace babudb {

#define RECORD_TYPE_KEY 1
#define RECORD_TYPE_VALUE 2
#define RECORD_TYPE_OFFSETS 3
#define RECORD_TYPE_INDEX_KEY 4
#define RECORD_TYPE_INDEX_OFFSETS 5
#define RECORD_TYPE_FILE_FOOTER 6

class LogIndex;
class ImmutableIndex;
class KeyOrder;

class ImmutableIndexWriter {
public:
	ImmutableIndexWriter(std::auto_ptr<YIELD::MemoryMappedFile> mm, size_t chunk_size)
		: storage(mm), chunk_size(chunk_size), data_in_buffer(0) {}

	void add(Data key, Data value);
	void flushBuffer();

	void* writeData(Data data, char type);
	void finalize();


private:
	typedef std::vector<std::pair<Data,Data> > Buffer;
	Buffer record_buffer;
	size_t data_in_buffer;

	size_t chunk_size;
	SequentialFile storage;

	std::vector<Data> index_keys;
	std::vector<offset_t> index_offsets;
};


class ImmutableIndexIterator {
public:
	ImmutableIndexIterator(SequentialFile& file, bool end);
	ImmutableIndexIterator(const ImmutableIndexIterator& o);
	ImmutableIndexIterator(SequentialFile& file, offset_t* table, SequentialFile::iterator i, int n);

	void operator ++ ();
	std::pair<Data,Data> operator * ();
	bool operator != (const ImmutableIndexIterator& other );
	bool operator == (const ImmutableIndexIterator& other );

private:
	void findNextOffsetTable(SequentialFile::iterator it);

	SequentialFile& file;

	offset_t* offset_table;

	SequentialFile::iterator key; // the current key if offset_table != NULL
	int key_no;		 			  // the ordinal number of the current key
};

};

#endif
