// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/path.h"
using namespace yield;


#define TEST_FILE_NAME_ASCII "somefile.txt"
/*
#ifdef _WIN32
#endif
*/


TEST( PathASCII, babudb )
{
	Path path( TEST_FILE_NAME_ASCII );
	ASSERT_TRUE( path.getHostCharsetPath() == TEST_FILE_NAME_ASCII );
#ifdef _WIN32
	path.getWidePath();
#endif
}

/*
#ifdef _WIN32
TEST( PathUnicode, babudb )
{
	Path path( TEST_FILE_NAME_UNICODE );
	ASSERT_TRUE( path.getUTF8Path() == TEST_FILE_NAME_UNICODE, "input path does not match output path" );
	//ASSERT_FALSE( path.getHostCharsetPath() == TEST_FILE_NAME_UNICODE, "input path does not match output path" );
}
#endif
*/

TEST( PathCast, babudb )
{
	Path path( TEST_FILE_NAME_ASCII );
	std::string path_str = ( std::string )path;
	ASSERT_TRUE( path.getHostCharsetPath() == path_str );
#ifdef _WIN32
	std::wstring path_wstr = ( std::wstring )path;
	ASSERT_TRUE( path.getWidePath() == path_wstr );
#endif
}

TEST( PathCat, babudb )
{
	Path path1( "hello" ), path2( TEST_FILE_NAME_ASCII );
	Path path3 = path1 + path2;
#ifdef _WIN32
	ASSERT_TRUE( path3.getHostCharsetPath() == "hello\\somefile.txt" );
#else
	ASSERT_TRUE( path3.getHostCharsetPath() == "hello/somefile.txt" );
#endif
}

TEST( PathSplit, babudb )
{
	{
		Path path( "head" );
		std::pair<Path, Path> split_path = path.split();
		ASSERT_TRUE( ( std::string )split_path.second == "head" );
	}

	{
		Path path( "head" + std::string( DISK_PATH_SEPARATOR_STRING ) );
		std::pair<Path, Path> split_path = path.split();
		ASSERT_TRUE( ( std::string )split_path.first == "head" );
	}

	{
		Path path( "head" + std::string( DISK_PATH_SEPARATOR_STRING ) + "tail" );
		std::pair<Path, Path> split_path = path.split();
		ASSERT_TRUE( ( std::string )split_path.first == "head" );
		ASSERT_TRUE( ( std::string )split_path.second == "tail" );
	}

	{
		Path path = Path( "head1" ) + Path( "head2" ) + Path( "tail" );
		std::pair<Path, Path> split_path = path.split();
		ASSERT_TRUE( split_path.first == ( Path( "head1" ) + Path( "head2" ) ) );
		ASSERT_TRUE( ( std::string )split_path.second == "tail" );
	}
}
