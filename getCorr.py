import numpy as np
from scipy.stats import kruskal, chi2_contingency, pearsonr, f_oneway
import pandas as pd
import time
import sys

def detectType(data):
	try:
		float(data)
	except:
		return('categorical')
	return('numeric')

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

def fillList(fileName, index):
	infile = open(fileName, 'r')
	list = []
	next(infile)
	for line in infile:
		line = line.strip().split(',')
		list.append(line[index])
	infile.close()
	return list

def compare(args):
	list1 = args[0]
	list2 = args[1]
	

startTime = time.time()

fileName = sys.argv[1]
infile = open(fileName, 'r')
head = next(infile).strip().split(',')
nCol = len(head)
print(nCol)
df = []
start = time.time()
for line in infile:
	line = line.strip().split(",")
	df.append(line)
print("reading entire file")
print(time.time()-start)


infile.close()
matrix = []
c = False
a = False
C = False
for i in range(1,nCol):
	start = time.time()
	list1 = [df[n][i] for n in range(len(df))]
	print("Fill list 1")
	print(time.time() - start)
	row = [None for k in range(i+1)]
	for j in range(i+1, nCol):
		start = time.time()
		list2 = [df[m][j] for m in range(len(df))]
		print("Fill list 2")
		print(time.time() - start)
		type1 = detectType(list1[0])
		type2 = detectType(list2[0])
		if type1 == 'categorical' and type2 == 'categorical':
			print("cantingency")
			start = time.time()
			row.append(runContingency(list1, list2))
			print(time.time() - start)
			c = True
		elif type1 == 'categorical' and type2 == 'numeric':
			print("ANOVA")
			start = time.time()
			row.append(anova(list2, list1))
			print(time.time() - start)
			a = True
		elif type2 == 'categorical' and type1 == 'numeric':
			print("ANOVA")
			start = time.time()
			row.append(anova(list1, list2))
			print(time.time() - start)
			a = True
		elif type1 == 'numeric' and type2 == 'numeric':
			print("correlation")
			start = time.time()
			row.append(runCorr(list1, list2))
			print(time.time() - start)
		else:
			print('Error at ' + str(i) + "x" + str(j) + " type1 = " + type1 + " type2 = " + type2)
		if a and c and C:
			exit()
	
	matrix.append(row)
	print("One row in %s seconds"%(time.time() - startTime))
	print("Number of total rows to get through: " + str(ncol))
	exit()
table = pd.DataFrame(matrix, columns=head, index=head)
table.to_csv("results2.csv")
	
print("time elapsed: %s seconds"%(time.time()-startTime))
