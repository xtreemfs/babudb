// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#include "yield/platform/file.h"
#include "yield/platform/path.h"
#include "yield/platform/stat.h"
using namespace yield;

#ifdef _WIN32
#include "yield/platform/windows.h"
#undef CopyFile
#else
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif
#define _LARGEFILE64_SOURCE 1
#define _FILE_OFFSET_BITS 64
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#ifdef YIELD_HAVE_POSIX_FILE_AIO
#include <aio.h>
#endif
#if defined(__linux__)
// Mac OS X's off_t is already 64-bit
#define lseek lseek64
#elif defined(__sun)
extern off64_t lseek64(int, off64_t, int);
#define lseek lseek64
#endif
#endif

File::File( const Path& path, unsigned long flags )
: flags( flags )
{
  fd = DiskOperations::open( path, flags );
}

#ifdef _WIN32
File::File( char* path, unsigned long flags )
: flags( flags )
{
  fd = DiskOperations::open( path, flags );
}

File::File( const char* path, unsigned long flags )
: flags( flags )
{
  fd = DiskOperations::open( path, flags );
}
#endif

File* File::open( const Path& path, unsigned long flags )
{
  fd_t fd = DiskOperations::open( path, flags );
  if ( fd != INVALID_HANDLE_VALUE )
    return new File( fd );
  else
    return NULL;
}

File::File( fd_t fd, unsigned long flags ) : fd( fd ), flags( flags )
{ }

File::File( const File& other ) : fd( other.fd ), flags( other.flags )
{ }

File::File() : fd( INVALID_HANDLE_VALUE ), flags( 0 )
{ }

File::~File()
{
  if ( ( flags & O_CLOSE_ON_DESTRUCT ) == O_CLOSE_ON_DESTRUCT )
    close();
}

uint64_t File::seek( uint64_t offset, unsigned char whence )
{
#ifdef _WIN32
  LONG dwDistanceToMove = lower32( offset ), dwDistanceToMoveHigh = upper32( offset );
  return SetFilePointer( fd, dwDistanceToMove, &dwDistanceToMoveHigh, whence );
#else
  return lseek( fd, ( off_t )offset, whence );
#endif
}

ssize_t File::read( void* buf, size_t nbyte )
{
#ifdef _WIN32
  DWORD dwBytesRead;
  if ( ReadFile( fd, buf, nbyte, &dwBytesRead, NULL ) )
    return dwBytesRead;
  else
    return -1;
#else
  return ::read( fd, buf, nbyte );
#endif
}

ssize_t File::write( const void* buf, size_t nbyte )
{
#ifdef _WIN32
  DWORD dwBytesWritten;
  if ( WriteFile( fd, buf, nbyte, &dwBytesWritten, NULL ) )
    return dwBytesWritten;
  else
    return -1;
#else
  return ::write( fd, buf, nbyte );
#endif
}

bool File::close()
{
  if ( fd != INVALID_HANDLE_VALUE )
  {
    bool ret = DiskOperations::close( fd, flags );
    fd = INVALID_HANDLE_VALUE;
    return ret;
  }
  else
    return true;
}


bool File::isOpen() 
{
  return fd != INVALID_HANDLE_VALUE;
}

bool File::CopyFile(const std::string& from, const std::string& to) {
  File* source = File::open(from, O_RDONLY);
  if (source == NULL)
    return false;
  File* dest = File::open(to, O_WRONLY|O_TRUNC|O_CREAT);
  if (dest == NULL)
    return false;

  const ssize_t buffer_size = 1024 * 1024;
  char* buffer = new char[buffer_size];

  ssize_t read = buffer_size;
  while (read == buffer_size) {
    read = source->read(buffer, buffer_size);
    if (dest->write(buffer, (size_t)read) != read) {
      delete [] buffer;
      return false;
    }
  }

  delete [] buffer;
  delete source;
  delete dest;
  return true;
}
