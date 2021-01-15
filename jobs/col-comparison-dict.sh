#!/bin/sh

source ../env/bin/activate

DATA_PATH=$1
COL_COMP_INPUTS_PATH=$2
JOB_N=$3
N_CORES=$4
OUT_DIR=$5

python3 col_comparison_dict.py $DATA_PATH $COL_COMP_INPUTS_PATH $JOB_N $N_CORES $OUT_DIR
