let MEM_START=
let MEM_RANGE=128 - 16
let CPU_RANGE=8-2
let MEM_INCREMENT=MEM_RANGE / 998
let CPU_INCREMENT=CPU_RANGE / 998

for i in {1..999}
do
    let INVERSE_IDX=999 - $i

    # bash jobs/col-comparison-dict.submit $i
done
