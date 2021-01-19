#!/bin/bash

for i in {0..1293}
do
    bash jobs/alpha-filter.submit $i
done
