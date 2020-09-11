import sys
import re
import pickle

dataFile = sys.argv[1]
dictList = sys.argv[2:]

def writeFile(fileName, lineList, head):
	outfile = open(fileName, 'w')
	outfile.write(head)
	for line in lineList:
		outfile.write(line)
	outfile.close()
		


for d in dictList:
	dFile = open(d, 'rb')
	currentDict = pickle.load(dFile)
	dFile.close()
	values = [currentDict[key] for key in currentDict]
	values = list(set(values))
	print(values)
	groups = [[] for i in range(len(values))]
	infile = open(dataFile, 'r')
	head = next(infile)
	for line in infile:
		row = line.strip().split(',') 
		idx = values.index(currentDict[row[0]])
		groups[idx].append(line)
	infile.close()	
	for i in range(len(groups)):
		name = dataFile[0:-4] + "_" + d.split("/")[-1][0:-2] + "_" + str(values[i]) + ".csv"
		writeFile(name, groups[i], head)


		
		
