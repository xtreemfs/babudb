// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"

#include "yield/platform/memory_mapped_file.h"

#include <memory>
using std::memcpy;
#include <cstring>
using std::strcmp;

using namespace YIELD;

#define TEST_FILE_NAME "MemoryMappedFile_test.dat"
#define TESTSTRING "Test string"


TEST_SUITE( MemoryMappedFile )

class MemoryMappedFileTest : public TestCase
{
public:
	MemoryMappedFileTest() : TestCase( "MemoryMappedFileTest", MemoryMappedFileTestSuite() ) { }

	void setUp()
	{
		tearDown();
	}

	void runTest()
	{
		{
			MemoryMappedFile mmf( TEST_FILE_NAME, strlen( TESTSTRING ) + 1, O_CREAT|O_RDWR|O_SYNC );
			memcpy( mmf.getRegionStart(), TESTSTRING, strlen( TESTSTRING ) + 1 );
			mmf.writeBack();
		}

		MemoryMappedFile mmf( TEST_FILE_NAME, 0 );
		ASSERT_EQUAL( mmf.getRegionSize(), strlen( TESTSTRING ) + 1 );
		ASSERT_EQUAL( strcmp( mmf.getRegionStart(), TESTSTRING ), 0 );
	}

	void tearDown()
	{
		try { DiskOperations::unlink( TEST_FILE_NAME ); } catch( ... ) { }
	}
};

MemoryMappedFileTest MemoryMappedFileTest_inst;

TEST_MAIN( MemoryMappedFile )
