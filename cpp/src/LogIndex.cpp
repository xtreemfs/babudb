// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Licensed under the BSD License, see LICENSE file for details.

#include "LogIndex.h"
#include "LogSection.h"

#include <utility>
using std::pair;

using namespace babudb;

Data LogIndex::lookup(const Data& search_key) {
	Tree::iterator it = latest_value.find(search_key);

	if(it != latest_value.end()) {
		return it->second;
	}
	else
		return Data::Empty();
}

// Only add Data to the key. Removed keys are represented as Delete values because they
// act as an overlay to less significant indices.

bool LogIndex::add(const Data& new_key, const Data& new_value) {
	Data tree_key = new_key.clone();
	Data tree_value = new_value.clone();
	// index in latest LogIndex
	pair<Tree::iterator,bool> old_entry = latest_value.insert(pair<Data,Data>(tree_key,tree_value));

	if(!old_entry.second) { // already existed
		Data old_key((old_entry.first)->first), old_value((old_entry.first)->second);

		latest_value.erase(old_entry.first);	// remove and free it
		old_key.free();
		old_value.free();

		pair<Tree::iterator,bool> new_entry  = latest_value.insert(pair<Data,Data>(tree_key,tree_value));
		ASSERT_TRUE(new_entry.second);
	}

	return !old_entry.second;
}
