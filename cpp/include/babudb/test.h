// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef BABUDB_TEST_H
#define BABUDB_TEST_H

#include "yield/platform/yunit.h"
#include "yield/platform/platform_exception.h"
#include "yield/platform/disk_operations.h"
#include "yield/platform/directory_walker.h"

#define TEST_OUTPUT_DIR "test_out"

class TestCaseTmpDir : public yield::TestCase {
public:
	TestCaseTmpDir(const char* short_description, yield::TestSuite& test_suite)
		: yield::TestCase(short_description, test_suite) {}

	void setUp() {
		if(yield::DiskOperations::exists(yield::Path(TEST_OUTPUT_DIR) + __short_description))
			yield::DiskOperations::rmtree(yield::Path(TEST_OUTPUT_DIR) + __short_description);
		try {
			yield::DiskOperations::mkdir(yield::Path(TEST_OUTPUT_DIR));
		} catch(yield::PlatformException& ) {}

		yield::DiskOperations::mkdir(yield::Path(TEST_OUTPUT_DIR) + __short_description);
	}

	yield::Path testPath(const std::string& filename = "") {
		return yield::Path(TEST_OUTPUT_DIR) + __short_description + filename;
	}
};

#define EXPECT_EQUAL(stat_a,stat_b) \
	{ if ( !( (stat_a) == (stat_b) ) ) throw yield::AssertionException( __FILE__, __LINE__, #stat_a" != "#stat_b ); }
#define EXPECT_TRUE(stat) \
	{ if ( !( (stat) == true ) ) throw yield::AssertionException( __FILE__, __LINE__, #stat" != true" ); }
#define EXPECT_FALSE(stat) \
	{ if ( !( (stat) == false ) ) throw yield::AssertionException( __FILE__, __LINE__, #stat" != false" ); }


#define TEST_TMPDIR( short_description, TestSuiteName ) \
extern yield::TestSuite& TestSuiteName##TestSuite(); \
class short_description##Test : public TestCaseTmpDir \
{ \
public:\
	short_description##Test() : TestCaseTmpDir( #short_description "Test", TestSuiteName##TestSuite()  ) { }\
  void runTest();\
};\
short_description##Test short_description##Test_inst;\
void short_description##Test::runTest()

#endif
