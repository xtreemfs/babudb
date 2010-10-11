// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/cuckoo_hash_table.h"
#include "yield/platform/string_hash.h"
using namespace YIELD;


TEST( CuckooHashTable_normal, babudb )
{
	uint32_t key = string_hash( "key" );
	CuckooHashTable<uint32_t, uint32_t> cht;
	cht.insert( key, 1 );
	uint32_t value = cht.find( key );
	ASSERT_EQUAL( value, 1 );
	ASSERT_EQUAL( cht.size(), 1 );
}

TEST( CuckooHashTable_duplicate,  babudb )
{
	uint32_t key = string_hash( "key" );
	CuckooHashTable<uint32_t, uint32_t> cht;
	cht.insert( key, 1 );
	cht.insert( key, 2 );
	uint32_t value = cht.find( key );
	ASSERT_EQUAL( value, 1 );
	ASSERT_EQUAL( cht.size(), 2 );
}

TEST( CuckooHashTable_erase, babudb )
{
	uint32_t key = string_hash( "key" );
	CuckooHashTable<uint32_t, uint32_t> cht;
	cht.insert( key, 1 );
	uint32_t value = cht.erase( key );
	ASSERT_EQUAL( value, 1 );
	ASSERT_EQUAL( cht.size(), 0 );
	value = cht.find( key );
	ASSERT_EQUAL( value, 0 );
	cht.insert( key, 2 );
	value = cht.find( key );
	ASSERT_EQUAL( value, 2 );
	ASSERT_EQUAL( cht.size(), 1 );
}

TEST( CuckooHashTable_clear, babudb )
{
	uint32_t key = string_hash( "key" );
	CuckooHashTable<uint32_t, uint32_t> cht;
	cht.insert( key, 1 );
	cht.clear();
	uint32_t value = cht.find( key );
	ASSERT_EQUAL( value, 0 );
	ASSERT_EQUAL( cht.size(), 0 );
}

TEST( CuckooHashTable_iterator, babudb )
{
	uint32_t key = string_hash( "key" );
	CuckooHashTable<uint32_t, uint32_t> cht;
	cht.insert( key, 1 );
	for ( CuckooHashTable<uint32_t, uint32_t>::iterator i = cht.begin(); i != cht.begin(); i++ )
	{
		ASSERT_EQUAL( *i, 1 );
	}

	cht.erase( key );
	for ( CuckooHashTable<uint32_t, uint32_t>::iterator i = cht.begin(); i != cht.begin(); i++ )
	{
		FAIL();
	}
}
