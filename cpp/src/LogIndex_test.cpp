// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/Database.h"
#include "babudb/StringConfiguration.h"

#include "Log.h"
#include "LogIndex.h"
#include "DataIndex.h"

#include "yield/platform/memory_mapped_file.h"
using YIELD::MemoryMappedFile;
using namespace babudb;

#include "babudb/test.h"

TEST_TMPDIR(LogIndex,babudb)
{
	StringOrder myorder;

	Database* db = new Database(testPath("test"),StringOperationFactory());
	db->registerIndex("testidx",myorder);
	db->startup();

	StringSetOperation op("testidx","Key1","data1");
	db->execute(op);
	db->commit();

	Data result = db->lookup("testidx",DataHolder("Key1"));
	EXPECT_FALSE(result.isEmpty());
	EXPECT_TRUE(strncmp((char*)result.data,"data1",5) == 0);

	result = db->lookup("testidx",DataHolder("Key2"));
	EXPECT_TRUE(result.isEmpty());

	StringSetOperation op2("testidx","Key2","data2");
	db->execute(op2);
	db->commit();

	result = db->lookup("testidx", DataHolder("Key2"));
	EXPECT_FALSE(result.isEmpty());

	// Overwrite
	StringSetOperation op3("testidx","Key1","data3");
	db->execute(op3);
	db->commit();

	result = db->lookup("testidx",DataHolder("Key1"));
	EXPECT_FALSE(result.isEmpty());
	EXPECT_TRUE(strncmp((char*)result.data,"data3",5) == 0);

	// Prefix
	StringSetOperation op4("testidx","Ke4","data4");
	db->execute(op4);
	db->commit();

/*	vector<pair<Data,Data> > results = db->match("testidx",Data("Key2",4));
	EXPECT_TRUE(results.size() == 1);

	results = db->match("testidx",Data("Key"));
	EXPECT_TRUE(results.size() == 2);

	results = db->match("testidx",Data("Ke"));
	EXPECT_TRUE(results.size() == 3);
*/
	db->shutdown();
	delete db;

	// Startup
	db = new Database(testPath("test"),StringOperationFactory());
	db->registerIndex("testidx",myorder);
	db->startup();

	result = db->lookup("testidx",DataHolder("Key2"));
	EXPECT_FALSE(result.isEmpty());

	db->shutdown();
	delete db;
}
