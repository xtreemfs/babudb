// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/shared_library.h"
#include "yield/platform/platform_exception.h"
#include "yield/platform/path.h"
using namespace YIELD;

#ifdef _WIN32
#include "yield/platform/windows.h"
#define SHLIBSUFFIX "dll"
#define snprintf _snprintf_s
#else
#include <dlfcn.h>
#include <cctype>
#include <errno.h>
#include <unistd.h>
#ifdef __MACH__
#define SHLIBSUFFIX "dylib"
#else
#define SHLIBSUFFIX "so"
#endif
#ifdef __sun
#include <stdio.h> // For spnrintf
#endif
#endif

#include <cstdlib>
#include <cstdio>
#include <cstring>
using namespace std;


#ifdef _WIN32
#define DLOPEN( file_path ) LoadLibraryExA( file_path, 0, LOAD_WITH_ALTERED_SEARCH_PATH )
#else
#define DLOPEN( file_path ) dlopen( file_path, RTLD_NOW|RTLD_GLOBAL )
#endif


SharedLibrary::SharedLibrary( const Path& file_prefix, const char* argv0 )
: file_prefix( file_prefix )
{
  char file_path[MAX_PATH];

  if ( ( handle = DLOPEN( file_prefix.getHostCharsetPath().c_str() ) ) != NULL )
    return;
  else
  {
    snprintf( file_path, MAX_PATH, "lib%c%s.%s", DISK_PATH_SEPARATOR, file_prefix.getHostCharsetPath().c_str(), SHLIBSUFFIX );
    if ( ( handle = DLOPEN( file_path ) ) != NULL )
      return;
    else
    {
      snprintf( file_path, MAX_PATH, "%s.%s", file_prefix.getHostCharsetPath().c_str(), SHLIBSUFFIX );
      if ( ( handle = DLOPEN( file_path ) ) != NULL )
        return;
      else
      {
        if ( argv0 != NULL )
        {
          const char* last_slash = strrchr( argv0, DISK_PATH_SEPARATOR );
          while ( last_slash != NULL && last_slash != argv0 )
          {
            snprintf( file_path, MAX_PATH, "%.*s%s.%s",
              last_slash - argv0 + 1, argv0, file_prefix.getHostCharsetPath().c_str(), SHLIBSUFFIX );
            if ( ( handle = DLOPEN( file_path ) ) != NULL )
              return;
            else
            {
              snprintf( file_path, MAX_PATH, "%.*slib%c%s.%s",
                  last_slash - argv0 + 1, argv0, DISK_PATH_SEPARATOR, file_prefix.getHostCharsetPath().c_str(), SHLIBSUFFIX );
              if ( ( handle = DLOPEN( file_path ) ) != NULL )
                return;
            }

            last_slash--;
            while ( *last_slash != DISK_PATH_SEPARATOR ) last_slash--;
          }
        }

        throw PlatformException(); // TODO: dlerror() returns the error string
      }
    }
  }
}

SharedLibrary::~SharedLibrary()
{
#ifdef _WIN32
  FreeLibrary( ( HMODULE )handle );
#else
#ifndef _DEBUG
  dlclose( handle ); // Don't dlclose when debugging, because that causes valgrind to lose symbols
#endif
#endif
}

void* SharedLibrary::getFunction( const char* func_name )
{
  void* func_handle;
#ifdef _WIN32
  func_handle = GetProcAddress( ( HMODULE )handle, func_name );
#else
  func_handle = dlsym( handle, func_name );
#endif
  if ( func_handle )
    return func_handle;
  else
    throw PlatformException();
}
