#!/bin/sh

source ../env/bin/activate

#sys.argv[1] = K, sys.argv[2] = p value, sys.argv[3] = input directory, sys.argv[4] = output file
K_VALUE=$1
P_VALUE=$2
IN_DIR=$3
OUT_DIR=$4

python3 top_k.py $K_VALUE $P_VALUE $IN_DIR $OUT_DIR
