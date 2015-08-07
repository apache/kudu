#!/bin/bash -x
# Copyright (c) 2014 Cloudera, Inc.
# Confidential Cloudera Information: Covered by NDA.

PROC_NAME=$1
MASTER_ADDRESS=$2
# Comma-separated list of paths to directories.
# Ex: /data/1,/data/2,/data/3
DATA_DIRS=$3
# Single path to a directory. Can be one of DATA_DIRS.
# Ex: /data/1
WAL_AND_GLOGS_DIR=$4

FLAG_FILE=tests/5nodes_test/$PROC_NAME.flags
echo 16777216 | sudo tee /proc/sys/vm/max_map_count
ulimit -c unlimited

# The data dirs all need to be modified to append either kudu-master or kudu-tserver.
# We then append "data" since one of those folders might be the same as $4 below.
DATA_DIRS_LIST=""
while IFS=',' read -ra PATHS; do
  for i in "${PATHS[@]}"; do
    LOCAL_DATA_DIR="$i/$PROC_NAME"
    mkdir $LOCAL_DATA_DIR
    DATA_DIRS_LIST+="$LOCAL_DATA_DIR/data,"
  done
done <<< "$DATA_DIRS"

# The directory provided for the WALs includes the process name and also stores the pidfile and
# the glogs.
WAL_AND_GLOGS_DIR=$WAL_AND_GLOGS_DIR/$PROC_NAME
WAL_DIR=$WAL_AND_GLOGS_DIR/wals
LOG_DIR=$WAL_AND_GLOGS_DIR/glogs

mkdir -p $LOG_DIR

case $PROC_NAME in
kudu-master)
  DATA_DIR_OPTION="--fs_wal_dir=$WAL_DIR --fs_data_dirs=$DATA_DIRS_LIST"
  MASTER_ADDRESS_OPT=--rpc_bind_addresses
  ;;
kudu-tserver)
  DATA_DIR_OPTION="--fs_wal_dir=$WAL_DIR --fs_data_dirs=$DATA_DIRS_LIST"
  MASTER_ADDRESS_OPT=--tserver_master_addrs
  ;;
*)
  echo "Wrong process name"
  exit 1
  ;;
esac

./build/latest/$PROC_NAME -flagfile=$FLAG_FILE --webserver_doc_root=`pwd`/www \
                          $MASTER_ADDRESS_OPT=$MASTER_ADDRESS \
                          $DATA_DIR_OPTION --log_dir=$LOG_DIR &> $PROC_NAME.log &

PID=$!
echo $PID > $WAL_AND_GLOGS_DIR/$PROC_NAME.pid
wait $PID
my_status=$?
echo $my_status > $PROC_NAME.ext
