#!/bin/bash

for i in {3235..6468}
do
    echo $i
    bash jobs/col-comparison-dict.submit $i
done
