// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/shared_library.h"
using namespace YIELD;


TEST_SUITE( SharedLibrary )

TEST( SharedLibrary, SharedLibrary )
{
#ifdef _WIN32
	SharedLibrary winsock( "ws2_32.dll" );
	ASSERT_TRUE( winsock.getFunction( "gethostbyname" ) != NULL );
#endif
}

TEST_MAIN( SharedLibrary )
