"""Creates a column comparison dictionary, a mapping of a tuple of 2 column headers to a p-value representing a
statistical test between those 2 columns"""

from numpy import array
from scipy.stats import chi2_contingency, pearsonr, f_oneway
from os import popen
from pandas import DataFrame
from pickle import load, dump
from time import time
from sys import argv
from multiprocessing import Pool, freeze_support
from math import ceil

"""
Real Data:
Total Columns: 1047909
Section Size: 1048
Number of Sections: 0-999

Debug Data:
Total Columns: 1048
Section Size: 105
Number of Sections: 0-9
"""

dataset: dict = {}
col_types: dict = {}
col_headers: list = []
PTID_COL: str = 'PTID'
CSV_DELIMINATOR: str = ','
NUMERIC_TYPE: str = 'numeric'
NOMINAL_TYPE: str = 'nominal'


def main():
	"""Main method"""

	global col_types
	global col_headers

	data_path: str = argv[1]
	col_types_path: str = argv[2]
	total_n_cols: int = int(argv[3])
	section_size: int = int(argv[4])
	section_idx: int = int(argv[5])
	n_cores: int = int(argv[6])
	out_dir: str = argv[7]

	print('Total Number Of Columns:', total_n_cols)
	print('Section Size:', section_size)
	print('Section Index:', section_idx)
	print('Number of Cores and Threads:', n_cores)

	start_time: float = time()

	df: str = get_cut_command_result(
		section_size=section_size, section_idx=section_idx, total_n_cols=total_n_cols, data_path=data_path
	)
	df: list = df.split('\n')

	# Construct the data set from the input file such that it is transposed (columns are rows and rows are columns)
	col_headers = df[0].split(CSV_DELIMINATOR)

	# Remove the patient ID from the column headers
	if PTID_COL in col_headers:
		assert col_headers[0] == PTID_COL
		assert section_idx == 0
		col_headers = col_headers[1:]

	df: list = df[1:]
	df.remove('')
	assert '' not in df

	col_types = load(open(col_types_path, 'rb'))

	# Construct the data set from which this process's section of the column comparison dictionary will be computed
	for i, row in enumerate(df):
		row = row.split(CSV_DELIMINATOR)

		# Remove the patient ID from the row
		if section_idx == 0:
			assert '_S_' in row[0]
			row = row[1:]

		assert len(row) == len(col_headers)

		# Add each value of the current row to its respective column
		for j, val in enumerate(row):
			header: str = col_headers[j]

			if header not in dataset:
				dataset[header] = []

			col: list = dataset[header]

			if get_type(header=header) == NUMERIC_TYPE:
				val: float = float(val)

			col.append(val)

		# Free the data from the current row that has been saved in the columns dictionary
		df[i] = None
		del row

	del df

	if len(col_headers) > section_size:
		# Since PTID was removed, if on the 0th section, we need to take one less column header to avoid section overlap
		row_headers: list = col_headers[:section_size] if section_idx > 0 else col_headers[:(section_size - 1)]
	else:
		row_headers: list = col_headers

	print('Setup Time: {}\n'.format(time() - start_time))

	start_time: float = time()
	freeze_support()
	comparison_dict: dict = col_comparison_dict(row_headers=row_headers, n_threads=n_cores)

	with open('data/{}/{}.p'.format(out_dir, str(section_idx).zfill(4)), 'wb') as f:
		dump(comparison_dict, f)

	print("Time Spent Computing and Saving All Comparisons:", time() - start_time)


def get_cut_command_result(section_size: int, section_idx: int, total_n_cols: int, data_path: str) -> str:
	"""Gets the portion of the data set needed for this section of the column comparison dictionary"""

	# Computation of the comparison dictionary is divided into sections, each of which is performed by its own process
	# Some sections are larger than others, beginning at their own start index but all going to the final column
	start_idx: int = section_size * section_idx

	# The cut command will load in the section of the data set for this process
	# We need to add 1 to the start column index because the cut command uses 1-indexing rather than 0-indexing
	command: str = 'cut -f {}-{} -d \',\' {}'.format(start_idx + 1, total_n_cols + 1, data_path)
	return popen(command).read()


def col_comparison_dict(row_headers: list, n_threads: int) -> dict:
	"""Constructs the column comparison dictionary with comparisons of each column in a dataset to every other column"""

	n_col: int = len(col_headers)
	comparison_dict: dict = {}
	prev_dict_len: int = len(comparison_dict)

	# For each column in this process's section of the data set, compare to every other column
	for i, row_header in enumerate(row_headers):
		print('{}: {}'.format(str(i).zfill(4), row_header))

		start_time: float = time()

		# Initialize the thread pool
		p = Pool(processes=n_threads)

		print('Time initializing the thread pool:', time() - start_time)
		start_time: float = time()

		# We start at the current index plus 1 so we don't perform redundant comparisons or self-comparisons
		# (e.g. col1 with itself or col1 with col2 and then col2 with col1)
		start: int = i + 1

		# Compute the p-values for the comparisons between the current column and batches of the remaining columns
		n_remaining_cols: int = n_col - start
		batch_size: int = ceil(n_remaining_cols / n_threads) if n_remaining_cols >= n_threads else n_remaining_cols
		end: int = start + batch_size
		batches: list = []

		for _ in range(n_threads):
			if end > n_col:
				end: int = n_col

			if start >= n_col:
				break

			batches.append((start, end))
			start: int = end
			end: int = start + batch_size

		# Each batch will be processed on its own thread to construct its portion of the column comparison dictionary
		arg_list = [(row_header, start, end) for start, end in batches]
		assert len(arg_list) == len(batches)

		print('Time Setting Up The Batches:', time() - start_time)
		print('Batch Size:', batch_size)
		print('Number Of Batches:', len(arg_list))
		start_time: float = time()

		# Add all the batch results (sub-dictionaries) to the main dictionary
		sub_dicts: list = p.map(compare_batch, arg_list)

		print('Time Threading:', time() - start_time)
		start_time: float = time()

		p.close()

		for sub_dict in sub_dicts:
			comparison_dict.update(sub_dict)
			del sub_dict

		# Free the comparisons that have been stored
		del sub_dicts

		assert len(comparison_dict) == prev_dict_len + n_remaining_cols
		prev_dict_len: int = len(comparison_dict)

		print('Time After Threading:', time() - start_time)
		print()

	return comparison_dict


def compare_batch(args: tuple) -> dict:
	"""Runs the correlation algorithm on all the columns in a thread's batch"""

	header1, start, end = args
	result_dict: dict = {}

	for i in range(start, end):
		header2: str = col_headers[i]
		key: tuple = tuple(sorted([header1, header2]))
		result_dict[key] = compare(header1, header2)

	return result_dict


def compare(header1: str, header2: str) -> float:
	"""Computes a correlation between two columns in the data set, given their headers"""

	list1: list = dataset[header1]
	list2: list = dataset[header2]
	type1: str = get_type(header=header1)
	type2: str = get_type(header=header2)
	stat = None

	if type1 == NOMINAL_TYPE and type2 == NOMINAL_TYPE:
		stat: float = run_contingency(list1, list2)
	elif type1 == NOMINAL_TYPE and type2 == NUMERIC_TYPE:
		stat: float = anova(list2, list1)
	elif type2 == NOMINAL_TYPE and type1 == NUMERIC_TYPE:
		stat: float = anova(list1, list2)
	elif type1 == NUMERIC_TYPE and type2 == NUMERIC_TYPE:
		stat: float = run_corr(list1, list2)
	else:
		print("ERROR: Non-specified type at " + header1 + " x " + header2)
		exit(1)

	return stat


def get_type(header: str) -> str:
	"""Gets the data type of a column given its header"""

	# All the MRI and expression data is numeric and thus does not need to be included in the column types
	if header not in col_types:
		return NUMERIC_TYPE

	return col_types[header]


def run_contingency(list1: list, list2: list) -> float:
	"""Runs a comparison of two nominal columns"""

	idx: list = list(set(list1))
	cols: list = list(set(list2))
	n_cols: int = len(cols)
	n_rows: int = len(idx)
	contig_table: list = [[0 for _ in range(n_cols)] for _ in range(n_rows)]

	for i in range(len(list1)):
		row_num: int = idx.index(list1[i])
		col_num: int = cols.index(list2[i])
		contig_table[row_num][col_num] += 1

	contig_table: DataFrame = DataFrame(contig_table, index=idx, columns=cols)
	p: float = chi2_contingency(contig_table)[1]
	return p


def anova(num: list, cat) -> float:
	"""Computes a correlation using analysis of variance where one column is numeric and the other is nominal"""

	categories: list = list(set(cat))
	table: list = []

	for c in categories:
		table.append([num[i] for i in range(len(num)) if cat[i] == c])

	p: float = f_oneway(*table)[1]
	return p 


def run_corr(list1: list, list2: list) -> float:
	"""Computes a correlation coefficient between two numeric columns"""

	results: tuple = pearsonr(array(list1), array(list2))
	p: float = results[1]
	return p


if __name__ == '__main__':
	main()
