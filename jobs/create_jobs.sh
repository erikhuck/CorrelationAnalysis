#!/bin/bash

for i in {0..2} #change this to be #FILES/5
do
    bash jobs/lowest_p.submit $i
done
