// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#ifndef YIELD_PLATFORM_SOCKET_LIB_H
#define YIELD_PLATFORM_SOCKET_LIB_H

#include "yield/platform/platform_types.h"

struct sockaddr_in;

// 255 with trailing 0
#define MAX_FQDN_LENGTH 256


namespace YIELD
{
	class SocketLib
	{
	public:
		static void init();
		static void destroy();

		// IPs (in and out) are in host order

		static const char* getLocalHostName() { return local_host_name; }
		static const char* getLocalHostFQDN() { return local_host_fqdn; }

		static socket_t createSocket( bool tcp );
		static socket_t createBoundSocket( bool tcp, unsigned int local_ip, unsigned short local_port );
		static int createConnectedSockets( socket_t ends[2] );
		static int pipe( socket_t ends[2] );
		static void close( socket_t, bool with_shutdown = false );

		static bool setBlocking( socket_t );
		static bool setNonBlocking( socket_t );
		static bool wouldBlock();
		static unsigned long getLastError();

		static bool reverseLookupHost( unsigned int ip, unsigned short port, char* host_out, unsigned int host_out_len, bool numeric = false );
		static unsigned int resolveHost( const char* );
		//static unsigned int getIPFromsockaddr_in( sockaddr_in& );
		static void getsockaddr_inFromIP( unsigned int ip, unsigned short port, sockaddr_in& into_sin );

		/*
		static bool inline isMulticast( unsigned int ip )
		{
			return ((ip) & 0xf0000000) >> 24 == 0xe0;
		}
		*/

	private:
		static bool have_initted;

		static char local_host_name[MAX_FQDN_LENGTH];
		static char local_host_fqdn[MAX_FQDN_LENGTH];
	};
};

#endif
