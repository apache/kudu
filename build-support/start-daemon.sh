#!/bin/bash -x
# Copyright (c) 2014 Cloudera, Inc.
# Confidential Cloudera Information: Covered by NDA.

PROC_NAME=$1
MASTER_ADDRESS=$2

FLAG_FILE=tests/5nodes_test/$PROC_NAME.flags
echo 16777216 | sudo tee /proc/sys/vm/max_map_count
ulimit -c unlimited
DATA_DIR=$3/$PROC_NAME
LOG_DIR=$DATA_DIR/glogs
mkdir -p $LOG_DIR

case $PROC_NAME in
kudu-master)
  DATA_DIR_OPTION=--master_base_dir=$DATA_DIR
  MASTER_ADDRESS_OPT=--master_rpc_bind_addresses
  ;;
kudu-tablet_server)
  DATA_DIR_OPTION=--tablet_server_base_dir=$DATA_DIR
  MASTER_ADDRESS_OPT=--tablet_server_master_addrs
  ;;
*)
  echo "Wrong process name"
  exit 1
  ;;
esac

./build/latest/$PROC_NAME -flagfile=$FLAG_FILE --webserver_doc_root=`pwd`/www \
                          $MASTER_ADDRESS_OPT=$MASTER_ADDRESS:61000 \
                          $DATA_DIR_OPTION --log_dir=$LOG_DIR &> $PROC_NAME.log &

PID=$!
echo $PID > $DATA_DIR/$PROC_NAME.pid
wait $PID
my_status=$?
echo $my_status > $PROC_NAME.ext
