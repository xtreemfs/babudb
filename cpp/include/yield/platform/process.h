// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#ifndef YIELD_PLATFORM_PROCESS_H
#define YIELD_PLATFORM_PROCESS_H

#include "yield/platform/path.h"

#include <vector>


namespace YIELD
{
	class Process
	{
	public:
		class argv_vector : public std::vector<char*>
		{
		public:
			~argv_vector()
			{
				while ( size() > 1 )
				{
					delete [] back();
					pop_back();
				}
			}
		};

		static Path getExeFilePath( const char* argv0 );
		static Path getExeDirPath( const char* argv0 );
		static void pause();
	#ifdef _WIN32
		static int getArgvFromCommandLine( const char* command_line, argv_vector& argv );
		static int WinMainTomain( char* lpszCmdLine, int ( *main )( int, char** ) );
	#endif
	};
};

#endif

