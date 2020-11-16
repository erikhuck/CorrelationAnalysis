#!/bin/sh

source ../env/bin/activate

DATA_PATH=$1
COL_TYPES_PATH=$2
START_IDX=$3
STOP_IDX=$4
N_ROWS=$5
N_CORES=$6
OUT_DIR=$7

python3 col_comparison_dict.py $DATA_PATH $COL_TYPES_PATH $START_IDX $STOP_IDX $N_ROWS $N_CORES $OUT_DIR
