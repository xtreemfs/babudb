// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#define  _CRT_SECURE_NO_WARNINGS

#include "SequentialFile.h"
#include "LogStats.h"
using namespace babudb;

#include <new>
#include <memory>
#include <string>

#include <yield/platform/assert.h>
#include <yield/platform/memory_mapped_file.h>
using namespace YIELD;

#define FIRST_RECORD_OFFSET		8
#define FIRST_RECORD_ADDRESS	(void*)((unsigned char*)memory->getRegionStart() + FIRST_RECORD_OFFSET)


#define SHUTDOWN_MARKER 0xbad1d0ba

SequentialFile::SequentialFile(auto_ptr<MemoryMappedFile> m, LogStats* stats ) : memory( m ), stats( stats )
{
	if(stats == NULL)
		this->stats = new LogStats();

	windToEnd();

	if(wasGraceful())
		rollback();

	database_version = *(unsigned int*)offset2record( 0 );

	this->stats->log_file_length = (unsigned int)memory->getRegionSize();
	this->stats->log_length = (unsigned int)next_write_offset;

	if(next_write_offset == FIRST_RECORD_OFFSET) // new database
		this->stats->gaps_length = this->stats->no_of_gaps = this->stats->no_of_records= 0;
	else
		this->stats->gaps_length = this->stats->no_of_gaps = this->stats->no_of_records = 0xfFFffFF;
}

void SequentialFile::close() {
	writeEndOfFile();
	memory->close();
}

void SequentialFile::compact()
{
	stats->no_of_records = 0;
	offset_t to = FIRST_RECORD_OFFSET;

	for(iterator r = begin(); r != end();)
	{
		unsigned int current_record_size = r.getRecord()->getRecordSize();
		iterator next_record = r; ++next_record;

		offset_t from = record2offset( r.getRecord() );
		moveRecord( from, to );

		to = to + current_record_size;
		r = next_record;

		stats->no_of_records++;
	}

	next_write_offset = to;

	stats->log_length = (unsigned int)next_write_offset;
	stats->no_of_gaps = 0;
	stats->gaps_length = 0;
}

void SequentialFile::skipEmptyRegion()
{
	record_frame_t* raw = (record_frame_t*)offset2record(next_write_offset) - 1;

	while( *raw == 0 && (unsigned char*)raw > FIRST_RECORD_ADDRESS )
		raw--;

	next_write_offset = record2offset( (Record*)raw );
}

bool SequentialFile::empty() {
	return next_write_offset == FIRST_RECORD_OFFSET;
}

void SequentialFile::windToEnd()
{
	next_write_offset = (offset_t)memory->getRegionSize();

	skipEmptyRegion();

	record_frame_t* raw = (record_frame_t*)offset2record( next_write_offset - 2 );

	if(next_write_offset == FIRST_RECORD_OFFSET)		// this is a new empty database, everything is fine
	{
		record_frame_t* start = (record_frame_t*)offset2record( 0 );

		if((memory->getFlags() & DOOF_WRITE) != 0)
			*start = SequentialFile_DB_VERSION;
	}
	else if(((unsigned int*)raw)[0] == SHUTDOWN_MARKER )
	{
		if((memory->getFlags() & DOOF_WRITE) != 0)
			((unsigned int*)raw)[0] = 0;

		next_write_offset = record2offset((Record*)raw);
	}
	else
		next_write_offset = INVALID_OFFSET;
}

void SequentialFile::writeEndOfFile()
{
	if((memory->getFlags() & DOOF_WRITE) == 0)
		return;

	unsigned int* raw = (unsigned int*)offset2record( next_write_offset );

	if( raw + 1 > (unsigned int*)memory->getRegionEnd() )
	{
		enlarge();
		raw = (unsigned int*)offset2record( next_write_offset );
	}

	raw[0] = SHUTDOWN_MARKER;

	memory->writeBack();
}


/** Assert that the given position is the end of a valid record and that after it there is one
	more valid record.

	\result Whether the assertion is true.
*/

bool SequentialFile::assertValidRecordChain( void* raw )
{
	SequentialFile::Record* candidate_end = (Record*)raw;
	SequentialFile::Record* candidate_begin = candidate_end->getStartHeader();

	if( (char*)candidate_begin < memory->getRegionStart() || (char*)candidate_begin >= memory->getRegionEnd() )
		return false;

	if( candidate_end->mightBeHeader() && candidate_begin->mightBeHeaderOf( candidate_end ) )
	{
		// It could be that we have found a valid record, check whether the next one is valid, too.

		record_frame_t* test = (record_frame_t*)candidate_begin - 1;

		// skip 0s

		while( *test == 0 && test > FIRST_RECORD_ADDRESS)
			test--;

		// we're now at the first non-0 byte, check whether there is a valid record

		SequentialFile::Record* next_end   = (Record*)test;
		SequentialFile::Record* next_start = next_end->getStartHeader();

		if( (void*)next_start < memory->getRegionStart() || (void*)next_start >= memory->getRegionEnd() )
			return false;

		if( next_start->mightBeHeaderOf( next_end ) )
			return true;

		// next record would be invalid so this one is a no real record
	}

	return false;
}

/** After a non-graceful shutdown, bring the database back to a consistent state

	\result Undefined
*/

int SequentialFile::repair()
{
	int lost_records = 0;

	// find end, wipe out half-written records, write end of file marker

	next_write_offset = (offset_t)memory->getRegionSize();

	skipEmptyRegion();

	record_frame_t* raw = (record_frame_t*)offset2record( next_write_offset );

	while( (char*)raw > FIRST_RECORD_ADDRESS )
	{
		// check whether at the current position could be a valid record

		if( assertValidRecordChain( raw ) )
			break;

		raw--;
		lost_records = 1;
	}

	raw = raw + 1;

	next_write_offset = record2offset( (SequentialFile::Record*)raw );


	// clean memory after first valid record

	if((memory->getFlags() & DOOF_WRITE) != 0)
		for( raw = raw; (char*)raw < memory->getRegionEnd(); raw++ )
			*raw = 0;

	rollback();		// remove any non-finalized transactions

	return lost_records;
}

bool SequentialFile::wasGraceful() {
	return next_write_offset != INVALID_OFFSET;
}

void SequentialFile::frameData(void* payload, size_t size, record_type_t type, bool marked ) {
	ASSERT_TRUE(ISALIGNED(payload, RECORD_FRAME_ALIGNMENT));
	Record* record = Record::getRecord((char*)payload);

	SequentialFile::Record* new_record = new (record)Record(type, size);
	new_record->mark(marked);
	next_write_offset = record2offset( (SequentialFile::Record*)new_record->getEndOfRecord() );
	ASSERT_TRUE(ISALIGNED((void*)next_write_offset, RECORD_FRAME_ALIGNMENT));
}

void* SequentialFile::append(size_t size, record_type_t type,bool marked) {
	void* location = getFreeSpace(size);
	frameData(location,size,type,marked);
	return location;
}

void SequentialFile::moveRecord( offset_t at, offset_t to )
{
	Record* source = offset2record( at );
	unsigned int size = source->getRecordSize();

	void* dest = offset2record( to );

	memmove( dest, source, size );

	if( to > at ) // move right, overlap or no overlap
		memset( source, 0, (size_t)(to - at) );

	else if( to + size < at ) // move left, no overlap
		memset( source, 0, size );

	else // move left, overlap
		memset( offset2record( to + size ), 0, (size_t)(at - to) );
}

void SequentialFile::commit()
{
	record_frame_t* next_to_current_record = (record_frame_t*)offset2record(next_write_offset);

	next_to_current_record--;
	SequentialFile::Record* current_record = ((Record*)next_to_current_record)->getStartHeader();

	if(current_record->isEndOfTransaction())	// no operations to commit
		return;

	if(current_record->isValid()) {
		current_record->setEndOfTransaction( true );
		writeBack();
	}
	// else: no operations in database
}

unsigned int SequentialFile::rollback() {
	unsigned int rolledback_operations = 0;

	iterator r;
	for(r = rbegin(); r != rend(); ++r)
	{
		if(r.getRecord()->isEndOfTransaction())
			break;

		if((memory->getFlags() & DOOF_WRITE) != 0)
			erase( record2offset( r.getRecord() ) );		// works because prev skips 0's
		rolledback_operations++;
	}

	if( r != rend() )
		next_write_offset = record2offset((Record*)r.getRecord()->getEndOfRecord() );
	else
		next_write_offset = FIRST_RECORD_OFFSET;

	return rolledback_operations;
}

void SequentialFile::erase( offset_t offset )
{
	ASSERT_TRUE((memory->getFlags() & DOOF_WRITE) != 0);

	Record* target = offset2record( offset );
	ASSERT_TRUE(target->isValid());
	offset_t end_offset = offset + target->getRecordSize();

	char* target_end = (char*)target->getEndOfRecord();
	for( char* wiper = (char*)target; wiper < target_end; wiper++ )
		*wiper = 0;

	// fix next_write_offset if it is the last record
	if( end_offset == next_write_offset ) {
		iterator i = at(end_offset);
		--i;

		if(i == begin())
			next_write_offset = 8;
		else
			next_write_offset = record2offset((Record*)i.getRecord()->getEndOfRecord());
	}

	stats->no_of_gaps += 1;
	stats->gaps_length += target->getRecordSize();
	stats->no_of_deletors--;
}

void* SequentialFile::getFreeSpace(size_t size) {
	if( next_write_offset + size + 32 > memory->getRegionSize() )
		enlarge();

	record_frame_t* location = (record_frame_t*)offset2record( next_write_offset );
	ASSERT_TRUE( *location == 0);

	location++;
	return location;
}

void SequentialFile::enlarge()
{
	size_t old_size = memory->getRegionSize();
	size_t new_size = (unsigned int)((float)(old_size < 500000 ? old_size * 6 : old_size * 2));

	memory->resize( new_size );

	stats->log_file_length = (unsigned int)new_size;
}


/** Create in iterator to advance from the given position

	\param rec The record to start from
	\result An iterator to advance
*/

void SequentialFile::setFlush( bool f )
{
//	memory->setFlush( f );
}

void SequentialFile::copyRecord( Record* record, void* destination )
{
	memcpy( destination, record, record->getRecordSize() );
}

// OffsetPointerConversion

void* SequentialFile::offset2pointer( offset_t offset ) const
{
	return offset2record( offset )->getPayload();
}

offset_t SequentialFile::pointer2offset( void* payload ) const
{
	return record2offset( Record::getRecord( (char*)payload ) );
}

// conversion helpers

SequentialFile::Record* SequentialFile::offset2record( offset_t offset ) const
{
	return ((Record*) ( offset + (char *)(memory->getRegionStart()) ) );
}

offset_t SequentialFile::record2offset( SequentialFile::Record* record ) const
{
	return (offset_t)( (char*)record - (char *)memory->getRegionStart() );
}

bool SequentialFile::isValid( SequentialFile::Record* r )
{
	return ((char*)r >= memory->getRegionStart() && (char*)r < memory->getRegionEnd() );
}

void SequentialFile::writeBack( Record* record )
{
	memory->writeBack( record, record->getRecordSize() );
}

void SequentialFile::writeBack()
{
	memory->writeBack();
}

#define ACTUAL_START (char*)memory->getRegionStart() + FIRST_RECORD_OFFSET
#define ACTUAL_SIZE (size_t)(next_write_offset - FIRST_RECORD_OFFSET)


SequentialFile::iterator SequentialFile::begin() {
	iterator i = iterator::rend(ACTUAL_START, ACTUAL_SIZE); i.reverse(); ++i;
	return i;
}

SequentialFile::iterator SequentialFile::rend() {
	return iterator::rend(ACTUAL_START, ACTUAL_SIZE);
}

SequentialFile::iterator SequentialFile::rbegin() {
	iterator i = iterator::end(ACTUAL_START, ACTUAL_SIZE); --i; i.reverse();
	return i;
}

SequentialFile::iterator SequentialFile::end() {
	return iterator::end(ACTUAL_START, ACTUAL_SIZE);
}


SequentialFile::iterator SequentialFile::at( void* payload ) {
	return SequentialFile::iterator(ACTUAL_START, memory->getRegionSize() - FIRST_RECORD_OFFSET, (Record*)payload,true);
}

SequentialFile::iterator SequentialFile::at( Record* record ) {
	return SequentialFile::iterator(ACTUAL_START, memory->getRegionSize() - FIRST_RECORD_OFFSET, record,true);
}

SequentialFile::iterator SequentialFile::at( offset_t offset ) {
	return SequentialFile::iterator(ACTUAL_START, memory->getRegionSize() - FIRST_RECORD_OFFSET, offset2record(offset),true);
}
