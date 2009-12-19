// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/yunit.h"
#include "yield/platform/platform_exception.h"
#include "yield/platform/socket_lib.h"
#include "yield/platform/sockets.h"
using namespace YIELD;

#include <cstring>
using std::strlen;
using std::strcmp;
using std::strncmp;


TEST_SUITE( SocketLib )

TEST( SocketLib_init, SocketLib )
{
	SocketLib::init();
	SocketLib::init();
}

TEST( SocketLib_getLocalHost, SocketLib )
{
	SocketLib::init();
	const char *local_host_name = SocketLib::getLocalHostName(), *local_host_fqdn = SocketLib::getLocalHostFQDN();
	ASSERT_TRUE( strlen( local_host_name ) > 0 );
	ASSERT_TRUE( strlen( local_host_fqdn ) > 0 );
	ASSERT_TRUE( strncmp( local_host_fqdn, local_host_name, strlen( local_host_name ) ) == 0 );
}


TEST( SocketLib_createSocket, SocketLib )
{
	SocketLib::init();
	socket_t s = SocketLib::createSocket( true );
	if ( s == -1 ) throw PlatformException();
	SocketLib::close( s );
	s = SocketLib::createBoundSocket( true, 0, 8081 );
	if ( s == -1 ) throw PlatformException();
	SocketLib::close( s );
}

TEST( SocketLib_pipe, SocketLib )
{
	SocketLib::init();
	socket_t ends[2];
	char m_out = 'm', m_in = 'x';
#ifdef _WIN32
	if ( SocketLib::createConnectedSockets( ends ) == -1 )
		throw PlatformException();
	::send( ends[1], &m_out, 1, 0 );
	::recv( ends[0], &m_in, 1, 0 );
#else
	if ( SocketLib::pipe( ends ) == -1 )
		throw PlatformException();
	::write( ends[1], &m_out, 1 );
	::read( ends[0], &m_in, 1 );
#endif
	ASSERT_EQUAL( m_out, m_in );
}

TEST( SocketLib_setBlocking, SocketLib )
{
	SocketLib::init();
	socket_t s = SocketLib::createSocket( true );
	if ( !SocketLib::setBlocking( s ) ) throw PlatformException();
	if ( !SocketLib::setNonBlocking( s ) ) throw PlatformException();
}

TEST( SocketLib_reverseLookupHost, SocketLib )
{
	SocketLib::init();
	char host[MAX_FQDN_LENGTH];

#ifdef _WIN32
	// String localhost=0
	SocketLib::reverseLookupHost( 0, 22, host, MAX_FQDN_LENGTH );
	ASSERT_TRUE( strcmp( host, "localhost" ) == 0 || strcmp( host, SocketLib::getLocalHostName() ) == 0 || strcmp( host, SocketLib::getLocalHostFQDN() ) == 0 );
// Linux returns "0.0.0.0" for ip = 0
#endif

	// Numeric localhost=0
	SocketLib::reverseLookupHost( 0, 22, host, MAX_FQDN_LENGTH, true );
	char* host_p = host; char dot_count = 0;
	while ( *host_p != 0 ) { if ( *host_p == '.' ) dot_count++; host_p++; }
	ASSERT_EQUAL( dot_count, 3 );

#ifndef __MACH__
	// String localhost=resolved
	unsigned int ip = SocketLib::resolveHost( SocketLib::getLocalHostFQDN() );
	SocketLib::reverseLookupHost( ip, 22, host, MAX_FQDN_LENGTH );
	ASSERT_TRUE( strcmp( host, "localhost" ) == 0 || strcmp( host, SocketLib::getLocalHostFQDN() ) == 0 || strncmp( host, SocketLib::getLocalHostName(), strlen( SocketLib::getLocalHostName() ) ) == 0 );
#endif

	/*
	ip = SocketLib::resolveHost( "microsoft.com" );
	if ( ip != 0 ) // ie. we're online
	{
		SocketLib::reverseLookupHost( ip, 80, host, MAX_FQDN_LENGTH );
		ASSERT_TRUE( strcmp( host, "microsoft.com" ) == 0, "reverseLookupHost failed on microsoft.com" );
	}
	*/
}

TEST( SocketLib_resolveHost, SocketLib )
{
	SocketLib::init();
	unsigned int ip = SocketLib::resolveHost( NULL );
	ASSERT_EQUAL( ip, 0 );
	ip = SocketLib::resolveHost( "" );
	ASSERT_EQUAL( ip, 0 );
	ip = SocketLib::resolveHost( "localhost" );
	ASSERT_TRUE( ip != 0 );
	ip = SocketLib::resolveHost( SocketLib::getLocalHostFQDN() );
	ASSERT_TRUE( ip != 0 );
}

TEST( SocketLib_sockaddr, SocketLib )
{
	sockaddr_in sin;
	SocketLib::getsockaddr_inFromIP( 0, 27095, sin );
	ASSERT_EQUAL( S_ADDR( sin ), INADDR_ANY );
	ASSERT_EQUAL( sin.sin_port, ntohs( 27095 ) );
	unsigned int ip = SocketLib::resolveHost( "localhost" );
	SocketLib::getsockaddr_inFromIP( ip, 27095, sin );
	ASSERT_EQUAL( S_ADDR( sin ), ntohl( ip ) );
	ASSERT_EQUAL( sin.sin_port, ntohs( 27095 ) );
}

TEST_MAIN( SocketLib )

