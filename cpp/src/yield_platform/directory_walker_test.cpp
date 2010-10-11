// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/directory_walker.h"
#include "yield/platform/disk_operations.h"
using namespace YIELD;

using namespace std;

#define TEST_DIR_PATH "DirectoryWalker_test"
#ifdef _WIN32
#define TEST_FILE_PATH "DirectoryWalker_test\\file.txt"
#define TEST_SUBDIR_PATH "DirectoryWalker_test\\subdir\\"
#else
#define TEST_FILE_PATH "DirectoryWalker_test/file.txt"
#define TEST_SUBDIR_PATH "DirectoryWalker_test/subdir/"
#endif


DECLARE_TEST_SUITE( babudb )

class DirectoryWalkerTest : public TestCase
{
public:
	DirectoryWalkerTest() : TestCase( "DirectoryWalkerTest", babudbTestSuite() ) { }

	void setUp()
	{
		tearDown();
		DiskOperations::mkdir( TEST_DIR_PATH );
		DiskOperations::touch( TEST_FILE_PATH );
		DiskOperations::mkdir( TEST_SUBDIR_PATH );
	}

	void runTest()
	{
		DirectoryWalker walker( TEST_DIR_PATH );

		bool seen_TEST_FILE_PATH = false, seen_TEST_SUBDIR_PATH = false;
		unsigned int seen_files_count = 0;
		while ( walker.hasNext() )
		{
			auto_ptr<DirectoryEntry> next_directory_entry = walker.getNext();
			const string& next_path = next_directory_entry.get()->getPath().getHostCharsetPath();
			if ( next_path.compare( TEST_FILE_PATH ) == 0 ) seen_TEST_FILE_PATH = true;
			else if ( next_path.compare( TEST_SUBDIR_PATH ) == 0 ) seen_TEST_SUBDIR_PATH = true;
			seen_files_count++;
		}

		if ( seen_files_count != 2 )
		{
			cerr << "expected 2 files, got " << seen_files_count;
			FAIL();
		}
		ASSERT_TRUE( seen_TEST_FILE_PATH );
		ASSERT_TRUE( seen_TEST_SUBDIR_PATH );
	}

	void tearDown()
	{
		try { DiskOperations::rmdir( TEST_SUBDIR_PATH ); } catch ( ... ) { }
		try { DiskOperations::unlink( TEST_FILE_PATH ); } catch ( ... ) { }
		try { DiskOperations::rmdir( TEST_DIR_PATH ); } catch ( ... ) { }
	}
};

DirectoryWalkerTest DirectoryWalkerTest_inst;

TEST_MAIN( DirectoryWalker )

