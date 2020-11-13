import sys
import os
import pickle
#sys.argv[1] = K value, sys.argv[2] = p value, sys.argv[3] = input directory, sys.argv[4] = output file
K = int(sys.argv[1])
Pvalue = float(sys.argv[2])
inputDir = sys.argv[3]
outFile = sys.argv[4]
allDicts = os.listdir(inputDir)
LowestP = {}

for file in allDicts:
    file = os.path.join(inputDir, file)
    if not file.endswith(".p"):
        continue
    pickle_file = open(file, "rb")
    data = pickle.load(pickle_file)
    for p in data:
        if float(data[p]) < Pvalue:
            #do we want to include 0.0 values?
            LowestP.update({p: data[p]})
    pickle_file.close()

LowestP = sorted(LowestP.items(), key = lambda x: x[1])
with open(outFile, 'w') as output:
    output.write(str(LowestP[:K]))
