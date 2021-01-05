#!/bin/bash

for i in {0..2} #change this to be #FILES/5
do
    bash jobs/alpha-filter.submit $i
done
