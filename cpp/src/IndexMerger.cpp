// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "IndexMerger.h"
using namespace babudb;

#include <yield/platform/memory_mapped_file.h>

IndexCreator::IndexCreator(std::auto_ptr<ImmutableIndexWriter> dest,LogIndex& from1, KeyOrder& order)
	: destination(dest), diff(from1), order(order), diff_it(diff.begin()) {}

bool IndexCreator::finished() {
	return diff_it == diff.end();
}

void IndexCreator::run() {
	while(!finished())
		proceed(10);
}

void IndexCreator::proceed(int n_steps) {
	for(int i = 0; i < n_steps; ++i) {
		if(finished()) {
			destination->finalize();
			break;
		}
		else {
			destination->add(diff_it->first, diff_it->second);
			++diff_it;
			continue;
		}
	}
}




IndexMerger::IndexMerger(std::auto_ptr<ImmutableIndexWriter> dest,LogIndex& from1,ImmutableIndex& from2, KeyOrder& order)
	: IndexCreator(dest,from1,order), base(from2), base_it(base.begin()) {}

bool IndexMerger::finished() {
	return base_it == base.end() && diff_it == diff.end();
}

static inline Data getKey(LogIndex::iterator it) { return it->first; }
static inline Data getKey(ImmutableIndex::iterator it) { return (*it).first; }

void IndexMerger::proceed(int n_steps) {
	for(int i = 0; i < n_steps; ++i) {
		if(finished()) {
			destination->finalize();
			break;
		}
		else if(base_it == base.end()) {
			destination->add(diff_it->first, diff_it->second);
			++diff_it;
			continue;
		}
		else if(diff_it == diff.end()) {
			destination->add((*base_it).first,(*base_it).second);
			++base_it;
			continue;
		}

		if( !order.less(getKey(base_it),getKey(diff_it)) &&
			!order.less(getKey(diff_it),getKey(base_it))) { // equal

			if(!getKey(diff_it).isDeleted()) 	// diff overwrites key
				destination->add(diff_it->first, diff_it->second);

			++diff_it;
			++base_it;

			continue;
		}

		if(order.less(getKey(base_it),getKey(diff_it))) {
			destination->add((*base_it).first,(*base_it).second);
			++base_it;
			continue;
		}

		if(order.less(getKey(diff_it),getKey(base_it))) {
			destination->add(diff_it->first, diff_it->second);
			++diff_it;
			continue;
		}

		FAIL();
	}
}
