#!/bin/bash

# Copyright (c) 2012 Michael Berlin, Zuse Institute Berlin
# Licensed under the BSD License, see LICENSE file for details.

set -e

trap onexit 1 2 3 15 ERR

function onexit() {
    local exit_status=${1:-$?}
    echo ERROR: Exiting $0 with $exit_status
    exit $exit_status
}

replication_dir_in_babudb_trunk="$(dirname "$0")"

cat <<EOF
This script updates the binary .jar files which are required for the BabuDB replication plugin.

The required files Foundation.jar (or PBRPC.jar) and Flease.jar are compiled from a SVN checkout of the XtreemFS trunk.

EOF

if [ -z "$XTREEMFS" ]
then
  known_xtreemfs_dirs="../../../../googlecode-svn/trunk"
  for dir in $known_xtreemfs_dirs
  do
    if [ -d "$dir" ]
    then
      XTREEMFS="$dir"
    fi
  done
fi

if [ -z "$XTREEMFS" ]
then
  echo "The environment variable XTREEMFS was not set. Please point it to a checkout directory of the SVN trunk of the XtreemFS project (svn checkout http://xtreemfs.googlecode.com/svn/trunk/ xtreemfs)."
  exit 1
fi

if [ ! -d "$XTREEMFS" ]
then
  echo "The environment variable XTREEMFS does not point to an existing directory. XTREEMFS = ${XTREEMFS}"
  exit 1
fi

echo "compiling Foundation.jar"
make foundation -C $XTREEMFS >/dev/null
cp -a ${XTREEMFS}/java/foundation/dist/Foundation.jar ${replication_dir_in_babudb_trunk}/../lib/Foundation.jar
echo "finished compiling Foundation.jar"

echo "compiling PBRPC.jar"
ant pbrpc-jar -f ${XTREEMFS}/java/foundation/build.xml >/dev/null
cp -a  ${XTREEMFS}/java/foundation/dist/PBRPC.jar ${replication_dir_in_babudb_trunk}/lib/
echo "finished compiling PBRPC.jar"

echo "compiling Flease.jar"
make flease -C $XTREEMFS >/dev/null
cp -a ${XTREEMFS}/java/flease/dist/Flease.jar ${replication_dir_in_babudb_trunk}/lib/
echo "finished compiling Flease.jar"
