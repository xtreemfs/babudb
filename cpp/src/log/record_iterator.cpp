// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)


#include "babudb/log/record_iterator.h"
#include "babudb/buffer.h"
#include "babudb/log/record_frame.h"

#include <yield/platform/assert.h>
using namespace YIELD;
using namespace babudb;

#define ASSERT_VALID_POS(x)			ASSERT_TRUE((char*)x >= (char*)region_start && (char*)x < ((char*)region_start + region_size))
#define REGION_END					((char*)region_start + region_size)
#define ASSERT_COMPATIBILITY(a,b)	ASSERT_TRUE((a).region_start == (b).region_start && (a).region_size == (b).region_size && (a).is_forward_iterator == (b).is_forward_iterator)

void* RecordIterator::operator * ()	const {
	ASSERT_VALID_POS(current);
	return current->getPayload();
}

RecordFrame* RecordIterator::getRecord() const {
	ASSERT_VALID_POS(current);
	return current;
}

Buffer RecordIterator::asData() const {
	ASSERT_VALID_POS(current);
	return Buffer(current->getPayload(), current->getPayloadSize());
}

bool RecordIterator::operator != ( const RecordIterator& other ) const {
	ASSERT_COMPATIBILITY(*this, other); 
	return current != other.current; 
}

bool RecordIterator::operator == ( const RecordIterator& other ) const { 
	ASSERT_COMPATIBILITY(*this, other);	// maybe you changed the database while iterating?
	return current == other.current;
}

void RecordIterator::reverse()							{ is_forward_iterator = !is_forward_iterator; }
unsigned char RecordIterator::getType() const			{ return current->getType(); }
bool RecordIterator::isType( unsigned char t ) const	{ return current->getType() == t; }
size_t RecordIterator::getSize() const					{ return current->getPayloadSize(); }

RecordIterator RecordIterator::begin(void* start, size_t size) {
	RecordIterator i = RecordIterator(start, size, (RecordFrame*)start, true);
	i.windIteratorToStart(); return i;
}

RecordIterator RecordIterator::end(void* start, size_t size) {
	return RecordIterator(start,size,(RecordFrame*)((char*)start+size),true);
}

RecordIterator RecordIterator::rbegin(void* start, size_t size) {
	RecordIterator i = end(start, size);
	i.reverse(); i.windIteratorToStart();  return i;
}

RecordIterator RecordIterator::rend(void* start, size_t size) {
	return RecordIterator(start, size, 0, false);
}

void RecordIterator::plusplus()	{
	record_frame_t* peek;

	if(current == NULL) {	// if we are at rend() move to start
		peek = (record_frame_t*)region_start;
	} 
	else { // check whether there is a next record
		ASSERT_VALID_POS(current);

		peek = (record_frame_t*)current->getEndOfRecord();

		if(peek == (record_frame_t*)REGION_END) {
			current = (RecordFrame*)peek;
			return;
		}

		ASSERT_VALID_POS(peek); // should be still within region
	}

	current = windForwardToNextRecord((RecordFrame*)peek);

	ASSERT_TRUE(ISALIGNED(current, RECORD_FRAME_ALIGNMENT));
}

RecordFrame* RecordIterator::windForwardToNextRecord(RecordFrame* record) {
	ASSERT_TRUE(record != NULL);

	if(record == (RecordFrame*)REGION_END)
		return record;

	record_frame_t* peek = (record_frame_t*)record;

	while((char*)peek < REGION_END && *peek == 0)
		peek++;

	ASSERT_TRUE((char*)peek <= REGION_END);

	record = (RecordFrame*)peek;

	if(peek != (record_frame_t*)REGION_END) {
		ASSERT_VALID_POS(record);
		ASSERT_TRUE(record->isValid());
	}

	return record;
}

void RecordIterator::minusminus() {
	current = windBackwardToNextRecord(current);

	ASSERT_TRUE(ISALIGNED(current, RECORD_FRAME_ALIGNMENT));
}

RecordFrame* RecordIterator::windBackwardToNextRecord(RecordFrame* record) {
	ASSERT_TRUE(record != NULL);

	if(record == region_start)
		return NULL;

	record_frame_t* peek = (record_frame_t*)record - 1;
	ASSERT_VALID_POS(peek);

	while((char*)peek > region_start && *peek == 0)
		peek--;

	if(peek == region_start) 
		return NULL;

	RecordFrame* end = (RecordFrame*)peek;
	record = end->getStartHeader();

	ASSERT_VALID_POS(record);
	ASSERT_TRUE(record->isValid());
	return record;
}

void RecordIterator::windIteratorToStart() {
	if(is_forward_iterator)
		current = windForwardToNextRecord(current);
	else
		current = windBackwardToNextRecord(current);
}