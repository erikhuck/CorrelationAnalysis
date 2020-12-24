#!/bin/sh

source ../env/bin/activate

COMP_DICT_DIR=$1
SUPER_ALPHA=$2

python3 counts_table.py $COMP_DICT_DIR $SUPER_ALPHA
