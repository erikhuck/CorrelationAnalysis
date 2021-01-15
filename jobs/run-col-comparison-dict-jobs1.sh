#!/bin/bash

for i in {0..3234}
do
    echo $i
    bash jobs/col-comparison-dict.submit $i
done
