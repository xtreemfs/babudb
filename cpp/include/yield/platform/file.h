// Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
// This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

#ifndef YIELD_PLATFORM_FILE_H
#define YIELD_PLATFORM_FILE_H

#include "yield/platform/disk_operations.h"

namespace yield
{
  class Path;

  class File
  {
  public:
    File( const Path& path, unsigned long flags = O_RDONLY|O_THROW_EXCEPTIONS|O_CLOSE_ON_DESTRUCT );
#ifdef _WIN32
    // Since fd_t is void* on Windows we need these to delegate to the const Path& variant rather than the fd_t one
    File( char* path, unsigned long flags = O_RDONLY|O_THROW_EXCEPTIONS|O_CLOSE_ON_DESTRUCT );
    File( const char* path, unsigned long flags = O_RDONLY|O_THROW_EXCEPTIONS|O_CLOSE_ON_DESTRUCT );
#endif
    static File* open( const Path& path, unsigned long flags = O_RDONLY|O_CLOSE_ON_DESTRUCT );
    explicit File( fd_t fd, unsigned long flags = O_THROW_EXCEPTIONS|O_CLOSE_ON_DESTRUCT );
    File( const File& );
    File();
    virtual ~File();

    inline unsigned long getFlags() { return flags; }
    inline fd_t getFD() { return fd; }	

    uint64_t seek( uint64_t offset, unsigned char whence );
    ssize_t read( void* buf, size_t nbyte );
    ssize_t write( const void* buf, size_t nbyte );
    ssize_t write( const std::string& buf ) { return write( buf.c_str(), buf.size() ); }
    ssize_t write( const char* buf ) { return write( buf, std::strlen( buf ) ); }
    virtual bool close();
    bool isOpen();

    static bool CopyFile(const std::string& from, const std::string& to);

  protected:
    fd_t fd;
    unsigned long flags;
  };
}

#endif
