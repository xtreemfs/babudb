// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)


#include "babudb/log/record_frame.h"

#include <yield/platform/assert.h>
using namespace YIELD;
using namespace babudb;

void* RecordFrame::getPayload()	const { 
  return (char*)this + RECORD_FRAME_SIZE_BYTES; 
}
RecordFrame* RecordFrame::GetRecord( void* p )	{ return (RecordFrame*)((char*)p - RECORD_FRAME_SIZE_BYTES); }

unsigned int RecordFrame::getPayloadSize() const { 
  return _getLengthField();
}
unsigned int RecordFrame::GetRecordSize() const	{ 
  return (unsigned int)ALIGN(_getLengthField(), RECORD_FRAME_ALIGNMENT) +  2*RECORD_FRAME_SIZE_BYTES;
}

bool RecordFrame::isValid()						{ return mightBeHeader() && mightBeHeaderOf(getFooter()); }

RecordFrame::RecordFrame(size_t size_in_bytes ) {
	ASSERT_TRUE(ISALIGNED((&header_data), RECORD_FRAME_ALIGNMENT));
	ASSERT_TRUE(header_data.plain_header == 0);
	setLength(size_in_bytes);						// ASSERT: memory was prev. filled with 0s
}
