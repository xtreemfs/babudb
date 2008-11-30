// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#ifndef NDEXMERGER_H
#define NDEXMERGER_H

#include "ImmutableIndex.h"
#include "ImmutableIndexWriter.h"
#include "LogIndex.h"

#include <memory>
#include <string>
using std::string;

namespace babudb {

class IndexCreator {
public:
	IndexCreator(std::auto_ptr<ImmutableIndexWriter>,LogIndex&,KeyOrder&);

	virtual void proceed(int );
	virtual void run();
	virtual bool finished();

	string target_name;
	lsn_t target_last_lsn;

protected:
	std::auto_ptr<ImmutableIndexWriter> destination;
	LogIndex& diff;
	LogIndex::iterator diff_it;

	KeyOrder& order;
};

class IndexMerger : public IndexCreator {
public:
	IndexMerger(std::auto_ptr<ImmutableIndexWriter>,LogIndex&,ImmutableIndex&,KeyOrder&);

	virtual void proceed(int );
	virtual bool finished();

private:
	ImmutableIndex& base;
	ImmutableIndex::iterator base_it;
};

};

#endif
