// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/string_hash.h"
using namespace YIELD;

#include <cstring>
using std::strlen;


TEST_SUITE( string_hash )

TEST( string_hash, string_hash )
{
	uint32_t test_string_hash_nt1 = string_hash( "test string" );
	uint32_t test_string_hash_nt2 = string_hash( "test string" );
	ASSERT_EQUAL( test_string_hash_nt1, test_string_hash_nt2 );

	uint32_t test_string_hash_len = string_hash( "test string", strlen( "test_string" ), 0 );
	ASSERT_EQUAL( test_string_hash_nt1, test_string_hash_len );

	uint32_t test_string_hash_partial = string_hash( "test " );
	test_string_hash_partial = string_hash( "string", strlen( "string" ), test_string_hash_partial );
	ASSERT_EQUAL( test_string_hash_nt1, test_string_hash_partial );

	uint32_t test_string_hash_partial_len = string_hash( "test ", strlen( "test " ), 0 );
	test_string_hash_partial_len = string_hash( "string", strlen( "string" ), test_string_hash_partial_len );
	ASSERT_EQUAL( test_string_hash_nt1, test_string_hash_partial_len );
}

TEST_MAIN( string_hash )
