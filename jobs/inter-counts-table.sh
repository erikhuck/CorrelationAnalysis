#!/bin/sh

source ../env/bin/activate

COMP_DICT_DIR=$1
SUPER_ALPHA=$2
IDX=$3
SECTION_SIZE=$4
TABLE_TYPE=$5

python3 inter_counts_table.py $COMP_DICT_DIR $SUPER_ALPHA $IDX $SECTION_SIZE $TABLE_TYPE
