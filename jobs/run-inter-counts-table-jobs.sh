#!/bin/bash

$TABLE_TYPE=$1

for i in {0..1293}
do
    bash jobs/inter-counts-table.submit $i $TABLE_TYPE
done
