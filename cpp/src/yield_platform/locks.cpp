// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/locks.h"
#include "yield/platform/debug.h"
#include "yield/platform/time.h"
using namespace YIELD;

#ifdef _WIN32
#include "yield/platform/windows.h"
#include <climits>
#else
#include <pthread.h>
#include <unistd.h>
#ifdef __MACH__
#include "CoreServices/CoreServices.h"
#else
#include <semaphore.h>
#endif
#include "yield/platform/time.h"
#if defined(__linux__) || defined(__FreeBSD__) || defined(__sun)
#define YIELD_HAVE_PTHREAD_MUTEX_TIMEDLOCK
#define YIELD_HAVE_SEM_TIMEDWAIT
#include <unistd.h>
#endif

#endif


Mutex::Mutex()
{
#ifdef _WIN32
	if ( ( os_handle = CreateEvent( NULL, FALSE, TRUE, NULL ) ) == NULL ) DebugBreak();
#else
	os_handle = new pthread_mutex_t;
	if ( pthread_mutex_init( ( pthread_mutex_t* )os_handle, NULL ) != 0 ) DebugBreak();
#endif
}

Mutex::~Mutex()
{
#ifdef _WIN32
	if ( os_handle ) CloseHandle( os_handle );
#else
	if ( os_handle ) pthread_mutex_destroy( ( pthread_mutex_t* )os_handle );
	delete ( pthread_mutex_t* ) os_handle;
#endif
}

bool Mutex::acquire()
{
#ifdef _WIN32
	DWORD dwRet = WaitForSingleObjectEx( os_handle, INFINITE, TRUE );
	return dwRet == WAIT_OBJECT_0 || dwRet == WAIT_ABANDONED;
#else
	pthread_mutex_lock( ( pthread_mutex_t* )os_handle );
	return true;
#endif
}

bool Mutex::try_acquire()
{
#ifdef _WIN32
	DWORD dwRet = WaitForSingleObjectEx( os_handle, 0, TRUE );
	return dwRet == WAIT_OBJECT_0 || dwRet == WAIT_ABANDONED;
#else
	return pthread_mutex_trylock( ( pthread_mutex_t* )os_handle ) == 0;
#endif
}

bool Mutex::timed_acquire( timeout_ns_t timeout_ns )
{
#ifdef _WIN32
	DWORD timeout_ms = ( DWORD )( timeout_ns / NS_IN_MS );
	DWORD dwRet = WaitForSingleObjectEx( os_handle, timeout_ms, TRUE );
	return dwRet == WAIT_OBJECT_0 || dwRet == WAIT_ABANDONED;
#else
#ifdef YIELD_HAVE_PTHREAD_MUTEX_TIMEDLOCK
	struct timespec timeout_ts = Time::getNSFromCurrentTimespec( timeout_ns );
	return ( pthread_mutex_timedlock( ( pthread_mutex_t* )os_handle, &timeout_ts ) == 0 );
#else
	if ( pthread_mutex_trylock( ( pthread_mutex_t* )os_handle ) == 0 )
		return true;
	else
	{
		usleep( timeout_ns / 1000 );
		return ( pthread_mutex_trylock( ( pthread_mutex_t* )os_handle ) == 0 );
	}
#endif
#endif
}

void Mutex::release()
{
#ifdef _WIN32
	SetEvent( os_handle );
#else
	pthread_mutex_unlock( ( pthread_mutex_t* )os_handle );
#endif
}


CountingSemaphore::CountingSemaphore( unsigned int max )
{
#if defined(_WIN32)
	os_handle = CreateSemaphore( NULL, 0, ( max == 0 ) ? LONG_MAX : max, NULL );
#elif defined(__MACH__)
	MPSemaphoreID sem_id;
	if ( MPCreateSemaphore( ( max == 0 ) ? 1000000 : max, 0, &sem_id ) != noErr ) DebugBreak();
	os_handle = sem_id;
#else
	os_handle = new sem_t;
	if ( sem_init( ( sem_t* )os_handle, 0, 0 ) != 0 ) DebugBreak(); // POSIX semaphores may be declared but not implemented, as on OS X
#endif
}

CountingSemaphore::~CountingSemaphore()
{
#if defined(_WIN32)
	CloseHandle( os_handle );
#elif defined(__MACH__)
	MPDeleteSemaphore( ( MPSemaphoreID )os_handle );
#else
	sem_destroy( ( sem_t* )os_handle );
	delete ( sem_t* )os_handle;
#endif
}

bool CountingSemaphore::acquire()
{
#if defined(_WIN32)
	DWORD dwRet = WaitForSingleObjectEx( os_handle, INFINITE, TRUE );
	return dwRet == WAIT_OBJECT_0 || dwRet == WAIT_ABANDONED;
#elif defined(__MACH__)
	return ( MPWaitOnSemaphore( ( MPSemaphoreID )os_handle, kDurationForever ) == 0 );
#else
	return ( sem_wait( ( sem_t* )os_handle ) == 0 );
#endif
}

bool CountingSemaphore::try_acquire()
{
#if defined(_WIN32)
	DWORD dwRet = WaitForSingleObjectEx( os_handle, 0, TRUE );
	return dwRet == WAIT_OBJECT_0 || dwRet == WAIT_ABANDONED;
#elif defined(__MACH__)
	return ( MPWaitOnSemaphore( ( MPSemaphoreID )os_handle, 0 );
#else
	return sem_trywait( ( sem_t* )os_handle ) == 0;
#endif
}

bool CountingSemaphore::timed_acquire( timeout_ns_t timeout_ns )
{
#if defined(_WIN32)
	DWORD timeout_ms = ( DWORD )( timeout_ns / NS_IN_MS );
	DWORD dwRet = WaitForSingleObjectEx( os_handle, timeout_ms, TRUE );
	return dwRet == WAIT_OBJECT_0 || dwRet == WAIT_ABANDONED;
#elif defined(__MACH__)
	return ( MPWaitOnSemaphore( ( MPSemaphoreID )os_handle, kDurationMillisecond * ( timeout_ns / NS_IN_MS ) ) == 0 );
#else
#ifdef YIELD_HAVE_SEM_TIMEDWAIT
	struct timespec timeout_ts = Time::getNSFromCurrentTimespec( timeout_ns );
	return ( sem_timedwait( ( sem_t* )os_handle, &timeout_ts ) == 0 );
#else
	if ( sem_trywait( ( sem_t* )os_handle ) == 0 )
		return true;
	else
	{
		useconds_t timeout_us = timeout_ns / 1000;
		usleep( timeout_us );
		return ( sem_trywait( ( sem_t* )os_handle ) == 0 );
	}
#endif
#endif
}

void CountingSemaphore::release()
{
#if defined(_WIN32)
	ReleaseSemaphore( os_handle, 1, NULL );
#elif defined(__MACH__)
	MPSignalSemaphore( ( MPSemaphoreID )os_handle );
#else
	sem_post( ( sem_t* )os_handle );
#endif
}
