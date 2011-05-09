// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

// A SequentialFile is a sequentially written series of Records. Supports transactional appends.

#ifndef BABUDB_LOG_SEQUENTIALFILE_H
#define BABUDB_LOG_SEQUENTIALFILE_H

#include <memory>
#include <cstdint>

#include "babudb/log/record_frame.h"
#include "babudb/log/record_iterator.h"

namespace babudb {
class RecordIterator;
class LogStats;
class LogStorage;

typedef uint64_t offset_t;
static const offset_t INVALID_OFFSET = 0xFFFFffffFFFFffffULL;

/** An append-only file of records.
*/

#define SEQUENTIALFILE_DB_VERSION 0x01

class SequentialFile
{
public:
	typedef class RecordFrame Record;
	typedef class RecordIterator iterator;

	explicit SequentialFile(LogStorage*);
  ~SequentialFile();

	void close();
	unsigned short getVersion()				{ return database_version; }
	void writeBack( Record* );
	void writeBack();

	void* getFreeSpace(size_t);
	void enlarge();
	void truncate();

	iterator First() const;
	iterator Last() const;
	iterator at(void* pointer) const;		// payload pointer
	iterator at(Record* record) const;
	iterator at(offset_t offset) const;

	bool empty();
	bool isWritable();

	void frameData(void* location, size_t size);
	void* append(size_t size);
  void AppendRaw(void* data, size_t size);
	void moveRecord( offset_t at, offset_t to );
	void erase( offset_t );

	void commit();
	unsigned int rollback();

	void* offset2pointer( offset_t offset ) const;
	offset_t pointer2offset( void* ) const;

	Record* offset2record( offset_t offset ) const;
	offset_t record2offset( Record* ) const;

	bool isValid(Record* record);
	void setFlush(bool do_flush);
	void compact();

private:
	int initialize();
	offset_t findNextAllocatedWord(offset_t);
	bool assertValidRecordChain( void* );

	void copyRecord( Record*, void* );

	std::auto_ptr<LogStorage> memory;

	offset_t next_write_offset;
	unsigned short database_version;
};

}

#endif
