"""Creates a column comparison dictionary, a mapping of a tuple of 2 column headers to a p-value which is the result of
a statistical test between those 2 columns. This dictionary represents and is more efficient than a comparison matrix"""

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

dataset_cols: dict = {}
col_types: dict = {}
headers: list = []
PTID_COL: str = 'PTID'
CSV_DELIMINATOR: str = ','
NUMERIC_TYPE: str = 'numeric'
NOMINAL_TYPE: str = 'nominal'


def main():
	"""Main method"""

	global col_types
	global headers

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

	# Construct the data set from the input file as a mapping from a header to its corresponding column
	df: str = get_cut_command_result(
		section_size=section_size, section_idx=section_idx, total_n_cols=total_n_cols, data_path=data_path
	)
	df: list = df.split('\n')

	headers = df[0].split(CSV_DELIMINATOR)

	# Remove the patient ID from the column headers
	if PTID_COL in headers:
		assert headers[0] == PTID_COL
		assert section_idx == 0
		headers = headers[1:]

	df: list = df[1:]
	df.remove('')
	assert '' not in df

	# Load in the column types which will indicate whether a column is numeric or nominal
	col_types = load(open(col_types_path, 'rb'))

	# Construct the data set from which this process's section of the column comparison dictionary will be computed
	for i, row in enumerate(df):
		row = row.split(CSV_DELIMINATOR)

		# Remove the patient ID from the row if this process's section is 0, since patient ID is the first header
		if section_idx == 0:
			assert '_S_' in row[0]
			row = row[1:]

		assert len(row) == len(headers)

		# Add each value of the current row to its respective column
		for j, val in enumerate(row):
			header: str = headers[j]

			if header not in dataset_cols:
				dataset_cols[header] = []

			col: list = dataset_cols[header]

			if get_type(header=header) == NUMERIC_TYPE:
				val: float = float(val)

			col.append(val)

		# Validate that the row was added correctly to the data set's columns
		for j, header in enumerate(headers):
			if get_type(header=header) == NUMERIC_TYPE:
				assert dataset_cols[header][i] == float(row[j])
			else:
				assert dataset_cols[header][i] == row[j]

		# Free the memory usage from the current row that has been saved in the columns dictionary
		df[i] = None
		del row

	# Free the memory usage from the cut command result and ensure the dataset columns were created from it correctly
	del df
	assert len(dataset_cols) == len(headers)
	assert set(dataset_cols.keys()) == set(headers)

	# Get the number of rows in the conceptual comparison matrix which the column comparison dictionary represents
	if len(headers) > section_size:
		# Since PTID was removed, if on the 0th section, we need to have one less row to avoid section overlap
		n_rows: int = section_size if section_idx > 0 else section_size - 1
	else:
		n_rows: int = len(headers)

	print('Setup Time: {}'.format(time() - start_time))
	freeze_support()

	comparison_dict: dict = col_comparison_dict(n_rows=n_rows, n_threads=n_cores)

	with open('data/{}/{}.p'.format(out_dir, str(section_idx).zfill(4)), 'wb') as f:
		dump(comparison_dict, f)


def get_cut_command_result(section_size: int, section_idx: int, total_n_cols: int, data_path: str) -> str:
	"""Gets the portion of the data set needed for this section of the column comparison dictionary"""

	# Computation of the comparison dictionary is divided into sections, each of which is performed by its own process
	# Some sections are larger than others, beginning at their own start index but all going to the final dataset column
	# Each section of the comparison dictionary represents a number of rows in its equivalent comparison matrix
	start_idx: int = section_size * section_idx

	# The cut command will load in the section of the data set for this process
	# We need to add 1 to the start column index because the cut command uses 1-indexing rather than 0-indexing
	command: str = 'cut -f {}-{} -d \',\' {}'.format(start_idx + 1, total_n_cols + 1, data_path)
	return popen(command).read()


def col_comparison_dict(n_rows: int, n_threads: int) -> dict:
	"""Constructs the column comparison dictionary with comparisons of each column in a dataset to every other column.
	This dictionary represents the portion of a square matrix up and to the right of the diagonal, considering the
	diagonal itself is useless and everything below and to the left of it is redundant"""

	n_cols: int = len(headers)
	comparison_dict: dict = {}

	# Initialize the thread pool
	p = Pool(processes=n_threads)

	# Get the list of arguments for each thread which has its own batch of arguments
	arg_list: list = get_arg_list(n_rows=n_rows, n_cols=n_cols, n_threads=n_threads)

	start_time: float = time()

	# Compute the sub dictionary in each thread according to that thread's batch
	sub_dicts: list = p.map(compare_batch, arg_list)

	print('Time Threading:', time() - start_time)

	p.close()
	start_time: float = time()

	# Add all the sub-dictionaries to the main column comparison dictionary
	for sub_dict in sub_dicts:
		comparison_dict.update(sub_dict)
		del sub_dict

	print('Time Stitching Batch Threads:', time() - start_time)

	# Ensure the dictionary represents the number of cells that would be in this process's section of the matrix
	n_cells_left_and_below_diagonal: int = (n_rows ** 2 - n_rows) // 2
	n_cells_in_diagonal: int = n_rows
	n_total_cells: int = n_rows * n_cols - n_cells_left_and_below_diagonal - n_cells_in_diagonal
	assert len(comparison_dict) == n_total_cells

	return comparison_dict


def get_arg_list(n_rows: int, n_cols: int, n_threads: int) -> list:
	"""Creates the list of arguments for each thread"""

	all_indices: list = []

	for i in range(n_rows):
		# Create the row indices and the start and stop column indices for this row in the conceptual matrix
		# We say conceptual because this will result in a comparison dictionary that represents a comparison matrix
		# We do not want to include the conceptual diagonal nor the conceptual cells to the left and below it
		indices: tuple = (i, (i + 1, n_cols))
		all_indices.append(indices)

	batch_size: int = ceil(n_rows / n_threads)
	args: list = []
	start: int = 0

	for i in range(n_threads):
		# If on the last batch, only use the indices that go to the last conceptual row
		if n_rows - start < batch_size:
			stop: int = n_rows
			assert i == n_threads - 1
		else:
			stop: int = start + batch_size

		args.append(all_indices[start:stop])
		start: int = stop

	assert sum(args, []) == all_indices

	return args


def compare_batch(args: tuple) -> dict:
	"""Runs the correlation algorithm on all the columns in a thread's batch"""

	result_dict: dict = {}
	batch_size: int = len(args)

	for i, (row_idx, (col_start, col_stop)) in enumerate(args):
		if i % 10 == 0:
			print('Thread Progress of Batch Beginning at {}: {:.2f}%'.format(args[0][0], i / batch_size * 100))

		for col_idx in range(col_start, col_stop):
			header1: str = headers[row_idx]
			header2: str = headers[col_idx]
			key: tuple = tuple(sorted([header1, header2]))
			result_dict[key] = compare(header1, header2)

	return result_dict


def compare(header1: str, header2: str) -> float:
	"""Computes a correlation between two columns in the data set, given their headers"""

	list1: list = dataset_cols[header1]
	list2: list = dataset_cols[header2]
	assert len(list1) == len(list2)
	type1: str = get_type(header=header1)
	type2: str = get_type(header=header2)
	stat = None

	if type1 == NOMINAL_TYPE and type2 == NOMINAL_TYPE:
		stat: float = run_contingency(list1=list1, list2=list2)
	elif type1 == NOMINAL_TYPE and type2 == NUMERIC_TYPE:
		stat: float = anova(numbers=list2, categories=list1)
	elif type2 == NOMINAL_TYPE and type1 == NUMERIC_TYPE:
		stat: float = anova(numbers=list1, categories=list2)
	elif type1 == NUMERIC_TYPE and type2 == NUMERIC_TYPE:
		stat: float = run_corr(list1=list1, list2=list2)
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


def anova(numbers: list, categories) -> float:
	"""Computes a correlation using analysis of variance where one column is numeric and the other is nominal"""

	unique_categories: list = list(set(categories))
	table: list = []

	for c in unique_categories:
		table.append([numbers[i] for i in range(len(numbers)) if categories[i] == c])

	p: float = f_oneway(*table)[1]
	return p 


def run_corr(list1: list, list2: list) -> float:
	"""Computes a correlation coefficient between two numeric columns"""

	results: tuple = pearsonr(array(list1), array(list2))
	p: float = results[1]
	return p


if __name__ == '__main__':
	main()
