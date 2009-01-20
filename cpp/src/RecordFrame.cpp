// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "RecordFrame.h"

#include <yield/platform/assert.h>
using namespace YIELD;
using namespace babudb;

void* RecordFrame::getPayload()					{ return (char*)this + RECORD_FRAME_SIZE_BYTES; }
RecordFrame* RecordFrame::getRecord( void* p )	{ return (RecordFrame*)((char*)p - RECORD_FRAME_SIZE_BYTES); }

unsigned int RecordFrame::getPayloadSize()		{ return _getLengthField(); }
unsigned int RecordFrame::getRecordSize()		{ return (unsigned int)ALIGN(_getLengthField(), RECORD_FRAME_ALIGNMENT) +  2*RECORD_FRAME_SIZE_BYTES; }

bool RecordFrame::isValid()						{ return mightBeHeader() && mightBeHeaderOf(getFooter()); }


void RecordFrame::setType(record_type_t t) { 
	ASSERT_TRUE(t <= RECORD_MAX_TYPE); 
	record_header h = getHeader(); 
	h.structured_header.type = t; 
	setHeaderAndFooter(h); 
}

RecordFrame::RecordFrame( record_type_t type, size_t size_in_bytes ) {
	ASSERT_TRUE(ISALIGNED((&header_data), RECORD_FRAME_ALIGNMENT));
	ASSERT_TRUE(header_data.plain_header == 0);
	setLength(size_in_bytes);						// ASSERT: memory was prev. filled with 0s
	setType(type);
}
