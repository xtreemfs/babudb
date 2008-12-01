// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "babudb/LookupIterator.h"
#include "babudb/KeyOrder.h"

#include "ImmutableIndex.h"
#include "ImmutableIndexWriter.h"
#include "LogIndex.h"

#include <yield/platform/memory_mapped_file.h>
#include <yield/platform/assert.h>
using namespace YIELD;

using namespace babudb;

LookupIterator::LookupIterator(const vector<LogIndex*>& idx, ImmutableIndex* iidx, const KeyOrder& order, const Data& start_key, const Data& end_key)
	: iidx(iidx), order(order), end_key(end_key), iidx_it(NULL) {

	// Initialize each slice
	for(vector<LogIndex*>::const_iterator i = idx.begin();i != idx.end(); ++i) {
		LogIndex::iterator c = (*i)->find(start_key);

		if(c != (*i)->end() && !order.less(end_key, (*c).first)) {
			logi.push_back(*i);
			logi_it.push_back(c);
		}
	}

	if(iidx) {
		ImmutableIndexIterator c = iidx->find(start_key);

		if(c != iidx->end() && !order.less(end_key, (*c).first))
			iidx_it = new ImmutableIndexIterator(c);
	}

	// each iterator is now pointing to a start_key <= key <= end_key

	findMinimalIterator();
}

LookupIterator::~LookupIterator() {
	delete iidx_it;
}

void LookupIterator::findMinimalIterator() {
	// find the smallest most significant key

	current_depth = 0;
	for(int i = 0; i < (int)logi_it.size(); ++i) {
		ASSERT_TRUE(logi_it[i] != logi[i]->end());
		if(order.less(logi_it[i]->first,logi_it[current_depth]->first)) {
			current_depth = i;
		}
	}

	if(iidx_it && order.less((**iidx_it).first,logi_it[current_depth]->first))
		current_depth = -1;
}

void LookupIterator::advanceIterator(int level) {
	if(level >= 0) {
		++logi_it[level];

		// are we done with that slice? remove it...
		if(logi_it[level] == logi[level]->end() || order.less(end_key,logi_it[level]->first)) {
			logi_it.erase(logi_it.begin() + level);
			logi.erase(logi.begin() + level);
		}
	} else {
		++(*iidx_it);

		if(*iidx_it != iidx->end() || order.less(end_key,(**iidx_it).first)) {
			delete iidx_it;
			iidx_it = NULL;
		}
	}
}

void LookupIterator::operator ++ () {
	// 1. Advance all shadowed iterators

	for(int i = current_depth + 1; i < (int)logi_it.size(); ++i) {
		ASSERT_TRUE(logi_it[i] != logi[i]->end());

		if(current_depth != -1 ) {
			if(!order.less(logi_it[current_depth]->first,logi_it[i]->first))
				advanceIterator(i);
		} else {
			if(!order.less((**iidx_it).first,logi_it[i]->first))
				advanceIterator(i);
		}
	}

	if(current_depth != -1 && iidx_it)
		advanceIterator(-1);

	// 2. Advance the current iterator and remove it if it is out of bounds

	advanceIterator(current_depth);

	if(!hasMore())
		return;

	// each iterator is now pointing to a start_key <= key <= end_key

	// 3. find the next position
	findMinimalIterator();

	// 4. Advance over deletor entries
	if(current_depth >= 0) {
		if(logi_it[current_depth]->second.isDeleted())
			this->operator ++();
	} else {
		if((**iidx_it).second.isDeleted())
			this->operator ++();
	}
}

std::pair<Data,Data> LookupIterator::operator * () {
	if(current_depth >= 0) {
		ASSERT_TRUE(logi_it[current_depth] != logi[current_depth]->end());
		ASSERT_FALSE(logi_it[current_depth]->second.isDeleted());
		return make_pair(logi_it[current_depth]->first, logi_it[current_depth]->second);
	} else {
		ASSERT_TRUE(*iidx_it != iidx->end());
		ASSERT_FALSE((**iidx_it).second.isDeleted());
		return make_pair((**iidx_it).first,(**iidx_it).second);
	}
}

bool LookupIterator::hasMore() {
	return logi_it.size() > 0 || iidx_it != NULL;
}

