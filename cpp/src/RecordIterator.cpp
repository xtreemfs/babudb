// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/RecordIterator.h"
#include "RecordFrame.h"

#include <yield/platform/assert.h>
using namespace YIELD_NS;
using namespace babudb;

#define ASSERT_VALID_POS(x)		ASSERT_TRUE((char*)x >= (char*)region_start && (char*)x < ((char*)region_start + region_size))
#define REGION_END				((char*)region_start + region_size)

void* RecordIterator::operator * ()	const {
	ASSERT_VALID_POS(current);
	return current->getPayload();
}

RecordFrame* RecordIterator::getRecord()	const {
	ASSERT_VALID_POS(current);
	return current;
}

void RecordIterator::reverse()							{ is_forward_iterator = !is_forward_iterator; }
unsigned char RecordIterator::getType() const			{ return current->getType(); }
bool RecordIterator::isType( unsigned char t ) const	{ return current->getType() == t; }
size_t RecordIterator::getSize() const					{ return current->getPayloadSize(); }

void RecordIterator::plusplus()	{
	record_frame_t* peek;

	if(current == NULL) {
		peek = (record_frame_t*)region_start;
	} else {
		if(current == (RecordFrame*)REGION_END && region_size == 0)
			return;

		ASSERT_VALID_POS(current);

		peek = (record_frame_t*)current->getEndOfRecord();

		if(peek == (record_frame_t*)REGION_END) {
			current = (RecordFrame*)peek;
			return;
		}

		ASSERT_VALID_POS(peek); // should be still within region
	}

	while( (char*)peek < REGION_END && *peek == 0)
		peek++;

	ASSERT_TRUE((char*)peek <= REGION_END);	 // can be end()
	current = (RecordFrame*)peek;

//	ASSERT_VALID_POS(current->getEndOfRecord()); // corner case, guh
	ASSERT_TRUE(ISALIGNED(current, RECORD_FRAME_ALIGNMENT));
}

void RecordIterator::minusminus() {
	ASSERT_TRUE(current != NULL);

	if(current == (RecordFrame*)region_start) {
		current = NULL;
		return;
	}

	record_frame_t* peek = (record_frame_t*)current - 1;
	ASSERT_VALID_POS(peek);

	while( *peek == 0 && peek > region_start )
		peek--;

	if(peek == region_start) {
		current = NULL;
		return;
	}

	RecordFrame* end = (RecordFrame*)peek;
	current = end->getStartHeader();

	ASSERT_VALID_POS(current); //otherwise the file is corrupt

	ASSERT_TRUE(ISALIGNED(current, RECORD_FRAME_ALIGNMENT));
}
