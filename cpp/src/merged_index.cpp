// This file is part of babudb/cpp
//
// Copyright (c) 2008, Felix Hupfeld, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist, Zuse Institute Berlin.
// Copyright (c) 2009, Felix Hupfeld
// Licensed under the BSD License, see LICENSE file for details.
//
// Author: Felix Hupfeld (felix@storagebox.org)

#include "merged_index.h"

#include "babudb/log/log.h"
#include "babudb/log/log_section.h"
#include "babudb/lookup_iterator.h"
#include "babudb/key.h"
#include "log_index.h"
#include "index/index.h"
using namespace babudb;

#include <algorithm>

#include "yield/platform/path.h"
using namespace YIELD;

MergedIndex::MergedIndex(const string& name, const KeyOrder& order)
	: order(order), immutable_index(NULL), name_prefix(name), tail(NULL) {
  ImmutableIndex::DiskIndices indices = ImmutableIndex::FindIndices(name);
  immutable_index = ImmutableIndex::LoadLatestIntactIndex(indices, order);

  tail = new LogIndex(order, GetLastPersistentLSN());
  log_indices.push_back(tail);
}

MergedIndex::~MergedIndex() {
  delete immutable_index;

  for(vector<LogIndex*>::iterator i = log_indices.begin(); i != log_indices.end(); ++i)
    delete *i;
}


lsn_t MergedIndex::GetLastPersistentLSN() {
  if (immutable_index)
    return immutable_index->GetLastLSN();
  else
    return 0;
}

void MergedIndex::Add(const Buffer& key, const Buffer& value) {
	tail->Add(key, value);
}

void MergedIndex::Remove(const Buffer& key) {
  Add(key, Buffer::Deleted());
}

Buffer MergedIndex::Lookup(const Buffer& key) {
	for(vector<LogIndex*>::iterator i = log_indices.begin();
		i != log_indices.end(); ++i) {
		Buffer result = (*i)->lookup(key);
		if(result.isDeleted())
			return Buffer::Empty();

		if(!result.isEmpty())
			return result;
	}

	if(immutable_index)
		return immutable_index->Lookup(key);

	return Buffer::Empty();
}

LookupIterator MergedIndex::Lookup(const Buffer& lower, const Buffer& upper) {
	return LookupIterator(log_indices, immutable_index, order, lower, upper);
}
