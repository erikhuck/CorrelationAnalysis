TOTAL_N_COLS=1047909 # 1048
N_ROWS=66 # 55

function get_n_total_cells {
    N_ROWS=$1
    N_COLS=$2

    echo `python3 -c "r = ${N_ROWS}; c = ${N_COLS}; t = r * c - (r ** 2 - r) // 2 - r; print(t)"`
}

function get_n_rows {
    N_COLS=$1
    N_TOTAL_CELLS=$2

    echo `python3 -c "from math import sqrt; c = ${N_COLS}; t = ${N_TOTAL_CELLS}; r = -(-2*c + 1 + sqrt((2*c - 1)**2 - 8*t)) / 2; print(int(r))"`
}

# Returns the content of the square root in the formula above to determine whether a square root of a negative is about to be attempted
function sqrt_content {
    N_COLS=$1
    N_TOTAL_CELLS=$2

    echo `python3 -c "c = ${N_COLS}; t = ${N_TOTAL_CELLS}; print((2*c - 1)**2 - 8*t)"`

}

N_TOTAL_CELLS=`get_n_total_cells ${N_ROWS} ${TOTAL_N_COLS}`
echo "Number Of Total Cells: ${N_TOTAL_CELLS}"

# Now we add 1 to the total number of columns to account for the patient ID column
let STOP_IDX=TOTAL_N_COLS+1

# We begin at a start index of 2 to skip the patient ID column
START_IDX=2

JOB_N=1

while [ ${START_IDX} -lt ${STOP_IDX} ]
do
    echo "Job Number: ${JOB_N}"
    echo "Start: ${START_IDX} Stop: ${STOP_IDX}"
    
    let N_COLS=${STOP_IDX}-${START_IDX}
    echo "Number Of Columns: ${N_COLS}"

    # If the content of the square root equates to a negative
    if [ `sqrt_content ${N_COLS} ${N_TOTAL_CELLS}` -lt 0 ]
    then
        # Finish off with the remaining columns
        N_ROWS=${N_COLS}
    else
        # Compute the number of rows needed to produce a given number of cells given a number of columns
        N_ROWS=`get_n_rows ${N_COLS} ${N_TOTAL_CELLS}`
    fi

    echo "Number Of Rows: ${N_ROWS}"
    echo "Number Of Total Cells: `get_n_total_cells ${N_ROWS} ${N_COLS}`"
    echo ""

    # Submit the job with the calculated inputs
    bash jobs/col-comparison-dict.submit ${START_IDX} ${STOP_IDX} ${N_ROWS} # bash jobs/debug-col-comparison-dict.submit ${START_IDX} ${STOP_IDX} ${N_ROWS}

    let START_IDX=START_IDX+N_ROWS
    let JOB_N=JOB_N+1
done

echo $START_IDX
