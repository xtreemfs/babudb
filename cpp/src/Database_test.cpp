// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/Database.h"
#include "babudb/StringConfiguration.h"
#include "babudb/test.h"

#include "yield/platform/memory_mapped_file.h"
using YIELD_NS::MemoryMappedFile;
using namespace babudb;

TEST_TMPDIR(Database,babudb)
{
	StringOrder myorder;
	Database* db = new Database(testPath("test").getHostCharsetPath(),StringOperationFactory());
	db->registerIndex("testidx",myorder);
	db->startup();

	StringSetOperation op("testidx", "Key1","data1");
	db->execute(op);

	StringSetOperation op2("testidx", "Key2","data2");
	db->execute(op2);

	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key1")).isEmpty());

	StringSetOperation opdel("testidx", "Key1");
	db->execute(opdel);

	EXPECT_TRUE(db->lookup("testidx",DataHolder("Key1")).isEmpty());

	db->shutdown();
	delete db;
}

TEST_TMPDIR(Database_Reopen,babudb)
{
	StringOrder myorder;
	Database* db = new Database(testPath("test").getHostCharsetPath(),StringOperationFactory());
	db->registerIndex("testidx",myorder);
	db->startup();

	StringSetOperation op("testidx", "Key1","data1");
	db->execute(op);

	StringSetOperation op2("testidx", "Key2","data2");
	db->execute(op2);
	db->commit();

	StringSetOperation opdel("testidx", "Key1");
	db->execute(opdel);

	// no shutdown
	delete db;

	db = new Database(testPath("test").getHostCharsetPath(),StringOperationFactory());
	db->registerIndex("testidx",myorder);
	db->startup();
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key1")).isEmpty());
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key2")).isEmpty());
	db->shutdown();
	delete db;

	db = new Database(testPath("test").getHostCharsetPath(),StringOperationFactory());
	db->registerIndex("testidx",myorder);
	db->startup();
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key1")).isEmpty());
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key2")).isEmpty());
	db->shutdown();
	delete db;
}

TEST_TMPDIR(Database_Migration,babudb)
{
	StringOrder myorder;
	Database* db = new Database(testPath("test").getHostCharsetPath(),StringOperationFactory());
	db->registerIndex("testidx",myorder);
	db->startup();

	StringSetOperation op("testidx", "Key1","data1");
	db->execute(op);
	db->commit();

	StringSetOperation op2("testidx", "Key2","data2");
	db->execute(op2);
	db->commit();

	EXPECT_FALSE(db->migrate("testidx", 10));
	EXPECT_TRUE(db->migrate("testidx", 10));
	db->shutdown();

	StringSetOperation op3("testidx", "Key3","data3");
	db->execute(op3);
	db->commit();
	db->shutdown();
	delete db;

	// now restart
	db = new Database(testPath("test").getHostCharsetPath(),StringOperationFactory());
	db->registerIndex("testidx",myorder);
	db->startup();

	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key1")).isEmpty());
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key2")).isEmpty());
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key3")).isEmpty());

	StringSetOperation op4("testidx", "Key4","data4");
	db->execute(op4);
	db->commit();

	EXPECT_FALSE(db->migrate("testidx", 10));
	EXPECT_TRUE(db->migrate("testidx", 10));
	db->shutdown();
	delete db;

	db = new Database(testPath("test").getHostCharsetPath(),StringOperationFactory());
	db->registerIndex("testidx",myorder);
	db->startup();
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key1")).isEmpty());
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key2")).isEmpty());
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key3")).isEmpty());
	EXPECT_FALSE(db->lookup("testidx",DataHolder("Key4")).isEmpty());
	db->shutdown();
	delete db;
}
