#!/bin/sh

source ../env/bin/activate

DATA_PATH=$1
START_IDX=$2
STOP_IDX=$3
N_ROWS=$4
N_CORES=$5
OUT_DIR=$6

python3 col_comparison_dict.py $DATA_PATH $START_IDX $STOP_IDX $N_ROWS $N_CORES $OUT_DIR
