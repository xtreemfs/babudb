// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/platform_exception.h"
#include "yield/platform/disk_operations.h"
using namespace yield;


#define TEST_FILE_NAME "DiskOperations_test.txt"
#define TEST_DIR_NAME "DiskOperations_test"


TEST( DiskOperations, babudb )
{
	try
	{
		DiskOperations::touch( TEST_FILE_NAME );
		ASSERT_TRUE( DiskOperations::exists( TEST_FILE_NAME ) );
		ASSERT_TRUE( DiskOperations::exists( TEST_FILE_NAME ) );
		fd_t fd = DiskOperations::open( TEST_FILE_NAME, O_RDONLY|O_THROW_EXCEPTIONS );
		ASSERT_TRUE( DiskOperations::exists( TEST_FILE_NAME ) );
		DiskOperations::close( fd );
		DiskOperations::unlink( TEST_FILE_NAME );
		ASSERT_FALSE( DiskOperations::exists( TEST_FILE_NAME ) );

		DiskOperations::mkdir( TEST_DIR_NAME );
		ASSERT_TRUE( DiskOperations::exists( TEST_DIR_NAME ) );
		DiskOperations::rmdir( TEST_DIR_NAME );
		ASSERT_FALSE( DiskOperations::exists( TEST_DIR_NAME ) );

		Path tree_path( Path( TEST_DIR_NAME ) + DISK_PATH_SEPARATOR_STRING + Path( TEST_DIR_NAME ) + DISK_PATH_SEPARATOR_STRING + Path( TEST_DIR_NAME ) );
		DiskOperations::mktree( tree_path );
		ASSERT_TRUE( DiskOperations::exists( tree_path ) );
		DiskOperations::touch( tree_path + TEST_FILE_NAME );
		DiskOperations::rmtree( TEST_DIR_NAME );
		ASSERT_FALSE( DiskOperations::exists( TEST_DIR_NAME ) );
	}
	catch ( PlatformException& exc )
	{
		try { DiskOperations::unlink( TEST_FILE_NAME ); } catch ( ... ) { }
		try { DiskOperations::rmtree( TEST_DIR_NAME ); } catch ( ... ) { }
		throw exc;
	}
};
