// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#ifndef YIELD_PLATFORM_SOCKETS_H
#define YIELD_PLATFORM_SOCKETS_H

#ifdef _WIN32

#ifndef FD_SETSIZE
#define FD_SETSIZE 1024
#endif

#include <winsock2.h>
#pragma comment( lib, "ws2_32.lib" )

typedef int socklen_t;
#define S_ADDR( x_sockaddr_in ) x_sockaddr_in.sin_addr.S_un.S_addr

#undef Yield

#else

// types.h must be included before socket.h
#include <sys/types.h>
#include <sys/socket.h>
// in.h contains IPPROTO_* constants
#include <netinet/in.h>
// tcp.h contains TCP_* constants
#include <netinet/tcp.h>

#define S_ADDR( x_sockaddr_in ) x_sockaddr_in.sin_addr.s_addr

#endif

#endif
