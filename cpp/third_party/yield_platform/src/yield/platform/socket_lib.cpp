// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/socket_lib.h"
#ifdef _WIN32
#include "yield/platform/windows.h"
#endif
#include "yield/platform/sockets.h"
using namespace YIELD;

#include <cstring>
#include <cstdio>
using namespace std;

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <fcntl.h>
#include <errno.h>
#include <netdb.h> // for getsockname and other lookup functions
#include <netinet/in.h> // for inet_addr
#include <arpa/inet.h>
#include <unistd.h> // for gethostname, getdomainname
#endif


bool SocketLib::have_initted = false;
char SocketLib::local_host_name[MAX_FQDN_LENGTH];
char SocketLib::local_host_fqdn[MAX_FQDN_LENGTH];


void SocketLib::init()
{
	if ( have_initted ) return;

#ifdef _WIN32
	WORD wVersionRequested;
	WSADATA wsaData;

	wVersionRequested = MAKEWORD( 2, 2 );

	WSAStartup( wVersionRequested, &wsaData );
#endif

	gethostname( local_host_name, MAX_FQDN_LENGTH );
	strcpy( local_host_fqdn, local_host_name ); // A default
        char* first_dot = strstr( local_host_name, "." );
        if ( first_dot != NULL ) *first_dot = 0;

#ifdef _WIN32
	DWORD dwStrLength = MAX_FQDN_LENGTH;
	GetComputerNameExA( ComputerNameDnsFullyQualified, local_host_fqdn, &dwStrLength );

	/*
	struct addrinfo addrinfo_hints, *addrinfo_list = NULL;
	memset( &addrinfo_hints, 0, sizeof( addrinfo_hints ) );
	addrinfo_hints.ai_family = AF_INET;
	addrinfo_hints.ai_socktype = SOCK_STREAM;
	addrinfo_hints.ai_protocol = IPPROTO_TCP;

	if ( ::getaddrinfo( local_host_name, NULL, NULL, &addrinfo_list ) == 0 )
	{
		struct addrinfo* addrinfo_last = addrinfo_list;
		while ( addrinfo_last->ai_next ) addrinfo_last = addrinfo_last->ai_next;
		getnameinfo( addrinfo_last->ai_addr, sizeof( struct sockaddr ), local_host_fqdn, MAX_FQDN_LENGTH, NULL, 0, NI_NAMEREQD );
		freeaddrinfo( addrinfo_list );
	}
	*/
#else
	// getnameinfo does not return aliases, which means we get "localhost" on Linux if that's the first
	// entry for 127.0.0.1 in /etc/hosts

#ifndef __sun
	char local_domain_name[MAX_FQDN_LENGTH];
	// getdomainname is not a public call on Solaris, apparently
	if ( getdomainname( local_domain_name, MAX_FQDN_LENGTH ) == 0 &&
		 local_domain_name[0] != 0 &&
		 strcmp( local_domain_name, "(none)" ) != 0 &&
		 strcmp( local_domain_name, local_host_name ) != 0 &&
                 strstr( local_domain_name, "localdomain" ) == NULL )
		strcat( local_host_fqdn, local_domain_name );
	else
	{
#endif
		// Try gethostbyaddr, like Python
		uint32_t local_host_addr = inet_addr( "127.0.0.1" );
		struct hostent* hostents = gethostbyaddr( ( char* )&local_host_addr, sizeof( uint32_t ), AF_INET );
		if ( hostents )
		{
			if ( strchr( hostents->h_name, '.' ) != NULL && strstr( hostents->h_name, "localhost" ) == NULL )
				strcpy( local_host_fqdn, hostents->h_name );
			else
			{
				for ( unsigned char i = 0; hostents->h_aliases[i] != NULL; i++ )
				{
					if ( strchr( hostents->h_aliases[i], '.' ) != NULL && strstr( hostents->h_name, "localhost" ) == NULL )
					{
						strcpy( local_host_fqdn, hostents->h_aliases[i] );
						break;
					}
				}
			}
		}
#ifndef __sun
	}
#endif
#endif

	have_initted = true;
}

void SocketLib::destroy()
{
#ifdef _WIN32
	WSACleanup();
#endif
}

void SocketLib::close( socket_t on_socket, bool with_shutdown )
{
#ifdef _WIN32
	if ( with_shutdown ) ::shutdown( on_socket, SD_BOTH );
	::closesocket( on_socket );
#else
	::close( on_socket );
#endif
}

socket_t SocketLib::createSocket( bool tcp )
{
	if ( tcp )
		return ( socket_t )::socket( AF_INET, SOCK_STREAM, IPPROTO_TCP );
	else
		return ( socket_t )::socket( AF_INET, SOCK_DGRAM, IPPROTO_UDP );
}

socket_t SocketLib::createBoundSocket( bool tcp, unsigned int local_ip, unsigned short local_port )
{
	socket_t bound_socket = createSocket( tcp );
	if ( bound_socket == -1 )
		return -1;

	struct sockaddr_in bind_sockaddr;
	SocketLib::getsockaddr_inFromIP( local_ip, local_port, bind_sockaddr );

	// SO_REUSEADDR caused a double binding on Win32 (= old socket intercepted new socket's conns)
//	int flag = 1;
//	setsockopt( bound_socket, SOL_SOCKET, SO_REUSEADDR, ( const char* ) & flag, 1 );
	// No SO_REUSEADDR: we want the bind to fail if an old port is still active, otherwise we won't be able to accept connections correctly

	if ( ::bind( bound_socket, ( struct sockaddr * ) & bind_sockaddr, sizeof( bind_sockaddr ) ) != -1 )
		return bound_socket;
	else
	{
		SocketLib::close( bound_socket );
		return -1;
	}
}

bool SocketLib::setBlocking( socket_t on_socket )
{
#ifdef _WIN32
	unsigned long val = 0;
	return ioctlsocket( on_socket, FIONBIO, &val ) != SOCKET_ERROR;
#else
	int current_fcntl_flags = fcntl( on_socket, F_GETFL, 0 );
	if ( ( current_fcntl_flags & O_NONBLOCK ) == O_NONBLOCK )
		return fcntl( on_socket, F_SETFL, current_fcntl_flags ^ O_NONBLOCK ) != -1;
	else
		return true;
#endif
}

bool SocketLib::setNonBlocking( socket_t on_socket )
{
#ifdef _WIN32
	unsigned long val = 1; // Anything > 1 turns on non-blocking IO
	return ioctlsocket( on_socket, FIONBIO, &val ) != SOCKET_ERROR;
#else
	int current_fcntl_flags = fcntl( on_socket, F_GETFL, 0 );
	return fcntl( on_socket, F_SETFL, current_fcntl_flags | O_NONBLOCK ) != -1;
#endif
}

bool SocketLib::wouldBlock()
{
#ifdef _WIN32
	return ( WSAGetLastError() == WSAEWOULDBLOCK );
#else
	return ( errno == EWOULDBLOCK || errno == EINPROGRESS );
#endif
}

unsigned long SocketLib::getLastError()
{
#ifdef _WIN32
	return WSAGetLastError();
#else
	return errno;
#endif
}

bool SocketLib::reverseLookupHost( unsigned int ip, unsigned short port, char* host_out, unsigned int host_out_len, bool numeric  )
{
	struct sockaddr_in temp_sockaddr;
	temp_sockaddr.sin_family = AF_INET;
	S_ADDR( temp_sockaddr ) = htonl( ip );
	temp_sockaddr.sin_port = htons( port );

	if ( ::getnameinfo( ( struct sockaddr* )&temp_sockaddr, sizeof( temp_sockaddr ), host_out, host_out_len, NULL, 0, ( numeric ) ? NI_NUMERICHOST : 0 ) == 0 )
		return true;
	else
	{
#ifdef _WIN32
		DWORD dwLastError = GetLastError();
#endif
		host_out[0] = 0;
		return false;
	}
}

unsigned int SocketLib::resolveHost( const char* hostname )
{
	if ( hostname && strlen( hostname ) > 0 )
	{
		struct addrinfo addrinfo_hints;
		struct addrinfo *addrinfo_list = NULL;

		memset( &addrinfo_hints, 0, sizeof( addrinfo_hints ) );
		addrinfo_hints.ai_family = AF_INET;
		addrinfo_hints.ai_socktype = SOCK_STREAM;
		addrinfo_hints.ai_protocol = IPPROTO_TCP;

		if ( ::getaddrinfo( hostname, NULL, &addrinfo_hints, &addrinfo_list ) == 0 )
		{
			unsigned int host_ip =  ntohl( S_ADDR( ( *( ( struct sockaddr_in* )addrinfo_list[0].ai_addr ) ) ) );
			freeaddrinfo( addrinfo_list );
			return host_ip;
		}
		else
			return 0;
	}
	else
		return 0;
}

void SocketLib::getsockaddr_inFromIP( unsigned int ip, unsigned short port, sockaddr_in& into_sin )
{
	memset( &into_sin, 0, sizeof( into_sin ) );
	into_sin.sin_family				= AF_INET;
	into_sin.sin_port				= htons( port ); // If port == 0 the kernel will assign a port
	if ( ip == 0 )
		S_ADDR( into_sin ) = INADDR_ANY;
	else
		S_ADDR( into_sin ) = htonl( ip ); // Should already be in network order, since resolveHost returns that
}

int SocketLib::createConnectedSockets( socket_t ends[2] )
{
	socket_t listen_socket = createSocket( true );
	if ( listen_socket != -1 )
	{
		struct sockaddr_in pipe_sockaddr;
		memset( &pipe_sockaddr, 0, sizeof( pipe_sockaddr ) );
		socklen_t pipe_sockaddr_len = sizeof( pipe_sockaddr );
		pipe_sockaddr.sin_family = AF_INET;
		S_ADDR( pipe_sockaddr ) = htonl( INADDR_LOOPBACK );
		if ( ::bind( listen_socket, ( struct sockaddr* )&pipe_sockaddr, pipe_sockaddr_len ) != -1 )
		{
			if ( ::listen( listen_socket, 1 ) != -1 )
			{
				memset( &pipe_sockaddr, 0, sizeof( pipe_sockaddr ) );
				pipe_sockaddr_len = sizeof( pipe_sockaddr );
				if ( ::getsockname( listen_socket, ( struct sockaddr* )&pipe_sockaddr, &pipe_sockaddr_len ) != -1 )
				{
					ends[1] = createSocket( true );

					if ( ends[1] != -1 )
					{
						pipe_sockaddr_len = sizeof( pipe_sockaddr );
						if ( ::connect( ends[1], ( struct sockaddr* )&pipe_sockaddr, pipe_sockaddr_len ) != -1 )
						{
							memset( &pipe_sockaddr, 0, sizeof( pipe_sockaddr ) );
							pipe_sockaddr_len = sizeof( pipe_sockaddr );
							ends[0] = ( socket_t )::accept( listen_socket, ( struct sockaddr* )&pipe_sockaddr, &pipe_sockaddr_len );

							if ( ends[0] != -1 )
							{
								SocketLib::close( listen_socket );
								return 0;
							}
						}
						else
							SocketLib::close( ends[1] );
					}
				}
			}
		}
		SocketLib::close( listen_socket );
	}

	return -1;
}

int SocketLib::pipe( socket_t ends[2] )
{
#ifdef _WIN32
	return createConnectedSockets( ends );
#else
	return ::pipe( ends );
#endif
}

