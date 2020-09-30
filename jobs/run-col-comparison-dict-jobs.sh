function increment {
    START=$1
    END=$2
    N_IDX=$3

    let RANGE=${START}-${END}
    INCREMENT=`python3 -c "x = $RANGE / ${N_IDX}; print(x)"`
    echo $INCREMENT
}

function as_integer {
    FLOAT=$1

    echo `python -c "print(round(${FLOAT}))"`
}

function update {
    VAR=$1
    INCREMENT=$2

    echo `python3 -c "x = $VAR - ${INCREMENT}; print(x)"`
}

N_IDX=999
CPU=8 # 4
MIN_CPU=2
CPU_INCREMENT=`increment $CPU $MIN_CPU $N_IDX`
MEM=128
MIN_MEM=16
MEM_INCREMENT=`increment $MEM $MIN_MEM $N_IDX`

for IDX in $(seq 1 ${N_IDX})
do
    echo $IDX `as_integer $CPU` `as_integer $MEM`
    # bash jobs/col-comparison-dict.submit $IDX $CPU $MEM
    CPU=`update $CPU $CPU_INCREMENT`
    MEM=`update $MEM $MEM_INCREMENT`
done
