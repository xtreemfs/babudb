// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef EQUENTIALFILE_H
#define EQUENTIALFILE_H

#include <string>
using std::string;
#include <memory>
using std::auto_ptr;

#include "yield/platform/platform_types.h"

#include "RecordFrame.h"
#include "babudb/RecordIterator.h"

namespace YIELD { class MemoryMappedFile; }

namespace babudb {

class RecordIterator;
class LogStats;

#define INVALID_OFFSET			0xFFFFffffFFFFffffULL
typedef uint64_t				offset_t;


/** An append-only file of records.
*/

#define SequentialFile_DB_VERSION 0x0001

class SequentialFile
{
public:
	typedef class RecordFrame Record;
	typedef class RecordIterator iterator;

	SequentialFile(auto_ptr<YIELD::MemoryMappedFile>, LogStats* = NULL);

	int repair();
	bool wasGraceful();
	void close();
	unsigned short getVersion()				{ return database_version; }
	void writeBack( Record* );
	void writeBack();

	void* getFreeSpace(size_t);
	void enlarge();

	iterator begin();
	iterator end();
	iterator rbegin();
	iterator rend();
	iterator at( void* );		// payload pointer
	iterator at( Record* );
	iterator at( offset_t );

	bool empty();

	void frameData(void* location, size_t size, record_type_t type, bool marked);
	void* append(size_t size, record_type_t type, bool);
	void moveRecord( offset_t at, offset_t to );
	void erase( offset_t );

	void commit();
	void rollback();

	void* offset2pointer( offset_t offset ) const;
	offset_t pointer2offset( void* ) const;

	Record* offset2record( offset_t offset ) const;
	offset_t record2offset( Record* ) const;

	bool isValid( Record* );
	void setFlush( bool f );
	void compact();

private:
	void windToEnd();

	void skipEmptyRegion();
	void writeEndOfFile();
	bool assertValidRecordChain( void* );

	void copyRecord( Record*, void* );

	auto_ptr<YIELD::MemoryMappedFile> memory;
	LogStats* stats;

	offset_t next_write_offset;
	unsigned short database_version;
};


};

#endif
