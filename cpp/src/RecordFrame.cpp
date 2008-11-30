// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "RecordFrame.h"

#include <yield/platform/assert.h>
using namespace YIELD_NS;
using namespace babudb;

void* RecordFrame::getPayload()					{ return (char*)this + RECORD_FRAME_SIZE_BYTES; }
RecordFrame* RecordFrame::getRecord( void* p )	{ return (RecordFrame*)((char*)p - RECORD_FRAME_SIZE_BYTES); }

unsigned int RecordFrame::getPayloadSize()		{ return _getLengthField(); }
unsigned int RecordFrame::getRecordSize()		{ return (unsigned int)ALIGN(_getLengthField(), RECORD_FRAME_ALIGNMENT) +  2*RECORD_FRAME_SIZE_BYTES; }

bool RecordFrame::isValid()						{ return mightBeHeader() && mightBeHeaderOf( getFooter() ); }

record_type_t RecordFrame::getType()			{ return structured_header.type; }
void RecordFrame::setType(record_type_t t)		{ ASSERT_TRUE(t <= RECORD_MAX_TYPE); structured_header.type = t; getFooter()->header = header; }

bool RecordFrame::isMarked()					{ return structured_header.marked == true; }
void RecordFrame::mark( bool m )				{ ASSERT_TRUE(isValid()); structured_header.marked = (m?1:0); getFooter()->header = header; }

RecordFrame::RecordFrame( record_type_t type, size_t size_in_bytes ) {
	ASSERT_TRUE(ISALIGNED((&header), RECORD_FRAME_ALIGNMENT));
	ASSERT_TRUE(header == 0);
	setLength( size_in_bytes );						// ASSERT: memory was prev. filled with 0s
	setType( type );
}
