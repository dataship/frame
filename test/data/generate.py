#!/usr/bin/env python
"""Create data for the test suite described by the given specification.
Deleting the file out.json in a subdirectory will cause it to be recreated
with existing data and new "args". Deleting all files in a subdirectory will
case all data to be recreated.

	spec.json contains an array of objects, each object contains

		"N" - a number of rows to generate
		"id" - a list of id columns to generate
			K - number of distinct values to generate
			type - type of data to generate for column
			[{"K" : 3, "type": "int32"}, {"K" : 3, "type": "int32"}],
		"value" - a list of value columns to generate
			[{"K" : 100, "type": "int32"}, {"K" : 100, "type": "int32"}]

Implementing test data generation for a new operation involves two things:
1. creation of a json spec
2. implementing an operation file with a single function named execute

Usage:
	generate.py <directory> <spec.json>
"""
from docopt import docopt
import os
import sys
import json
import numpy as np
import binary_matrix

OUT_FILENAME = "out.json"

extension_map = {
	"int32" : ".i32",
	"uint32" : '.u32',
	"float32" : '.f32',
	"int64" : '.i64',
	"uint64" : '.u64',
	"float64" : '.f64'
}

# function adapted from this issue request on numpy
# https://github.com/numpy/numpy/issues/3155
def random_sample(size=None, dtype=np.float64):

	if type(dtype) == str or type(dtype) == unicode:
		dtype = np.dtype(dtype).type

	type_max = 1 << np.finfo(dtype).nmant
	sample = np.empty(size, dtype=dtype)
	sample[...] = np.random.randint(0, type_max, size=size) / dtype(type_max)
	if size is None:
		sample = sample[()]
	return sample

int_types = set(["int32", "int64", "uint64", "uint32"])
float_types = set(["float32", "float64"])

def create_column(N, K, type="int32"):

	if type in int_types:
		return np.random.randint(0, K, N, dtype=type)

	if type in float_types:
		return K * random_sample(N, dtype=type)

	return np.random.randint(0, K, N, dtype="int32")

def write_result(result, location):
	"""write a dict to a file as a json document"""
	try:
		with open(location, 'w') as f:
			json.dump(result, f, indent=4)
	except Exception as e:
		print("Couldn't write output JSON file: {0}".format(e.message))
		sys.exit(1)


if __name__ == '__main__':
	arguments = docopt(__doc__, version='JSON Groupby Generator')

	# arguments parsed from Usage statement by docopt
	base_directory = os.path.join(arguments['<directory>'], '')
	test_file = arguments['<spec.json>']

	sys.path.insert(0, './' + base_directory)

	operation = __import__("operation")

	with open(test_file, 'r') as f:
		try:
			tests = json.load(f)
		except Exception as e:
			print("Couldn't parse JSON configuration file: {0}".format(e.message))
			sys.exit(1)


	for i in range(len(tests)):

		options = tests[i]
		N = options['N']

		# test directory is a string of four numbers starting at 0001
		directory = base_directory + "{0:0>4}/".format(i + 1)

		if not os.path.exists(directory):
			os.makedirs(directory)

		# if a result exists, skip this data set
		if os.path.exists(directory + OUT_FILENAME):
			print("Skipping {0}".format(directory))
			continue

		id_columns = {}
		for i in range(len(options['id'])):
			name = "id_{0}".format(i)
			spec = options['id'][i]
			dtype = spec['type']
			if dtype not in extension_map:
				dtype = "int32"
			K = spec['K']

			extension = extension_map[dtype]
			if os.path.exists(directory + name + extension):
				column = binary_matrix.read(directory + name + extension)
			else:
				column = create_column(N, K, dtype)
				binary_matrix.write(directory + name + extension, column)

			id_columns[name] = column

		value_columns = {}
		for i in range(len(options['value'])):
			name = "value_{0}".format(i)
			spec = options['value'][i]
			dtype = spec['type']
			if dtype not in extension_map:
				dtype = "int32"
			K = spec['K']

			extension = extension_map[dtype]
			if os.path.exists(directory + name + extension):
				column = binary_matrix.read(directory + name + extension)
			else:
				column = create_column(N, K, dtype)
				binary_matrix.write(directory + name + extension, column)

			value_columns[name] = column

		# run reduction
		arguments = options['arg'] if 'arg' in options else {}
		out = operation.execute(arguments, id_columns, value_columns)

		# write result
		#binary_matrix.write(directory + "out.arr", out.flatten())
		write_result(out, directory + OUT_FILENAME)

		print("Created {0}".format(directory))
