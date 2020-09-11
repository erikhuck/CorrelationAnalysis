import numpy as np
from scipy.stats import chi2_contingency, pearsonr, f_oneway
import pandas as pd
import time
import sys
from multiprocessing import Pool,freeze_support
from multiprocessing.pool import ThreadPool
from math import ceil

start_time = time.time()
infile = open(sys.argv[1], 'r')
headers = next(infile).strip().split(",")
ncol = len(headers)
df = [[] for i in range(ncol)]	
for line in infile:
	line = line.strip().split(",")
	for j in range(ncol):
		df[j].append(line[j])
infile.close()


infile = open(sys.argv[3], 'r')
colNames = next(infile).strip().split(',')
colTypes = next(infile).strip().split(',')
infile.close()
typeList = []
for i in range(1,len(headers)):
	typeList.append(colTypes[colNames.index(headers[i])])
print("time for setup")
print(time.time() - start_time)
	
def makeMatrix():
	print("starting make Matrix")
	startTime = time.time()
	matrix = []
	for i in range(1,ncol):
		print(i)
		list1 = df[i]   
		p = Pool(None)
		nthreads = ceil((ncol-i)/100) 
		start = i + 1
		print(start)
		end = start + 100
		batches = []
		for t in range(nthreads):	
			if end > ncol:
				end = ncol
			if start == ncol:
				continue
			batches.append([p for p in range(start, end)])
			start = end
			end = start + 100
		argList = [(i, batch) for batch in batches] 
		print("begin multi-threading")
		unorderedRow = p.map(compareBatch, argList)
		p.close()
		print("multi-threading done")
		print(time.time() - startTime)
		unorderedRow.sort()
		endOfRowPartitioned = [unorderedRow[l][1] for l in range(len(unorderedRow))]	
		endOfRow = sum(endOfRowPartitioned, [])
		beginOfRow = [None for k in range(i)]
		row = beginOfRow + endOfRow
		matrix.append(row)
		print("One row in %s seconds"%(time.time() - startTime))
		print("Number of total rows to get through: " + str(ncol))
		exit()
	table = pd.DataFrame(matrix, columns=headers[1:], index=headers[1:])
	table.to_csv(sys.argv[2])


def runContingency(list1, list2):
	idx = list(set(list1))
	cols = list(set(list2))
	ncols = len(cols)
	nrows = len(idx)
	contigTable = [[0 for i in range(ncols)] for j in range(nrows)]
	for i in range(len(list1)):
		rowNum = idx.index(list1[i])
		colNum = cols.index(list2[i])
		contigTable[rowNum][colNum] += 1
	contigTable = pd.DataFrame(contigTable, index=idx, columns=cols)
	p = chi2_contingency(contigTable)[1]
	return p


def anova(num, cat):
	num = [float(x) for x in num]
	categories = list(set(cat))
	table = []
	for c in categories:
		table.append([num[i] for i in range(len(num)) if cat[i] == c])	
	p = f_oneway(*table)[1]
	return p 


def runCorr(list1, list2):
	list1 = [float(x) for x in list1]
	list2 = [float(y) for y in list2]
	results = pearsonr(np.array(list1), np.array(list2))
	p = results[1]
	return p

def compareBatch(args):
	batch = args[1]
	index1 = args[0]
	priority = batch[0]
	resultList = []
	for index in batch:
		index2 = index
		resultList.append(compare(index1, index2))
	return (priority, resultList)

def compare(index1, index2):
	list1 = df[index1]
	list2 = df[index2]
	type1 = typeList[index1-1]
	type2 = typeList[index2-1]
	stat = None
	if type1 == 'nominal' and type2 == 'nominal':
		#print("Contingency")
		start = time.time()
		stat = runContingency(list1, list2)
		#print(time.time()-start)
	elif type1 == 'nominal' and type2 == 'numeric': 
		#print("ANOVA")
		start = time.time()
		stat = anova(list2, list1)
		#print(time.time() - start)
	elif type2 == 'nominal' and type1 == 'numeric':
		#print("ANOVA")
		start = time.time()
		stat = anova(list1, list2)
		#print(time.time() - start)
	elif type1 == 'numeric' and type2 == 'numeric':
		#print("Correlation")
		start = time.time()
		stat = runCorr(list1, list2)
		#print(time.time() - start)
	else:
		print("error at " + self.headers[index1] + " x " + self.headers[index2])
	return stat


print('started')	
startTime = time.time()
freeze_support()
makeMatrix()	
print("time elapsed: %s seconds"%(time.time()-startTime))
