#!/bin/bash
BABUDB_DIR=$PWD

# Run BabuDB JUnit tests
(
  cd $BABUDB_DIR/java
  ant test
)
JUNIT_BABUDB_RESULT=$?

# Run BabuDB replication plugin JUNIT tests
(
  cd $BABUDB_DIR/java/replication
  ant test
)
JUNIT_REPLICATION_RESULT=$?

if [[ $JUNIT_BABUDB_RESULT -eq 0 ]] && [[ $JUNIT_REPLICATION_RESULT -eq 0 ]] && [[ $CPP_BABUDB_RESULT -eq 0 ]]; then
  return 0
else
  return 1
fi

