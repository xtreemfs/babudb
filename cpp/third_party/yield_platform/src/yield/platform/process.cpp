// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/process.h"
#include "yield/platform/windows.h"
#include "yield/platform/platform_types.h"
#include "yield/platform/thread.h"
#include "yield/platform/locks.h"
using namespace YIELD;

#if defined(_WIN32)
#include "yield/platform/windows.h"
#else
#include <unistd.h> // For pause()
#if defined __linux__
#include <unistd.h> // For readlink
#elif defined(__MACH__)
#include <unistd.h> // For readlink
#include <stdlib.h> // For realpath
#include <mach-o/dyld.h> // For _NSGetExecutablePath
#endif
#endif


Path Process::getExeFilePath( const char* argv0 )
{
#if defined(_WIN32)
	char exe_file_path[MAX_PATH];
	if ( GetModuleFileNameA( NULL, exe_file_path, MAX_PATH ) )
		return exe_file_path;
#elif defined(__linux__)
	char exe_file_path[MAX_PATH];
	int ret;
	if ( ( ret = readlink( "/proc/self/exe", exe_file_path, MAX_PATH ) ) != -1 )
	{
		exe_file_path[ret] = 0;
		return exe_file_path;
	}
#elif defined(__MACH__)
	char exe_file_path[MAX_PATH];
	uint32_t bufsize = MAX_PATH;
	if ( _NSGetExecutablePath( exe_file_path, &bufsize ) == 0 )
	{
		exe_file_path[bufsize] = 0;

		char linked_exe_file_path[MAX_PATH]; int ret;
		if ( ( ret = readlink( exe_file_path, linked_exe_file_path, MAX_PATH ) ) != -1 )
		{
			linked_exe_file_path[ret] = 0;
			return linked_exe_file_path;
		}

		char absolute_exe_file_path[MAX_PATH];
		if ( realpath( exe_file_path, absolute_exe_file_path ) != NULL )
			return absolute_exe_file_path;

		return exe_file_path;
	}
#endif

	return argv0;
}

Path Process::getExeDirPath( const char* argv0 )
{
	Path exe_file_path = getExeFilePath( argv0 );
	return exe_file_path.split().first;
}

#ifdef _WIN32
CountingSemaphore pause_semaphore;

BOOL CtrlHandler( DWORD fdwCtrlType )
{
	if ( fdwCtrlType == CTRL_C_EVENT )
	{
		pause_semaphore.release();
		return TRUE;
	}
	else
		return FALSE;
}
#endif

void Process::pause()
{
#ifdef _WIN32
	SetConsoleCtrlHandler( ( PHANDLER_ROUTINE )CtrlHandler, TRUE );
	pause_semaphore.acquire();
#else
	::pause();
#endif
}

#ifdef _WIN32
int Process::getArgvFromCommandLine( const char* command_line, argv_vector& argv )
{
	char exec_name[MAX_PATH];
	GetModuleFileNameA( NULL, exec_name, MAX_PATH );
	argv.push_back( new char[ strlen( exec_name ) + 1 ] );
	strcpy( argv.back(), exec_name );

	const char *start_p = command_line, *p = start_p;

	while ( *p != 0 )
	{
		while ( *p != ' ' && *p != 0 ) p++;
		char* arg = new char[ ( p - start_p + 1 ) ];
		memcpy( arg, start_p, p - start_p );
		arg[p-start_p] = 0;
		argv.push_back( arg );
		if ( *p != 0 )
		{
			p++;
			start_p = p;
		}
	}

	return ( int )argv.size();
}

int Process::WinMainTomain( LPSTR lpszCmdLine, int ( *main )( int, char** ) )
{
	argv_vector argv;
	getArgvFromCommandLine( lpszCmdLine, argv );
	int ret = main( ( int )argv.size(), &argv[0] );
	return ret;
}
#endif
