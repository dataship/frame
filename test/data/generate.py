#!/usr/bin/env python
"""Create data for the test suite described by the given specification.
Deleting the file out.json in a subdirectory will cause it to be recreated
with existing data and new "args". Deleting all files in a subdirectory will
case all data to be recreated.

	spec.json contains an array of objects, each object contains

		"N" - a number of rows to generate
		"id" - a list of id columns to generate
		 	[{"M" : 3, "strings" : false}, {"M" : 3, "strings" : false}],
		"value" - a list of value columns to generate
			[{"M" : 100}, {"M" : 100}]

	"in" and "args" parameters
	"in" is an array of arrays which define the size and contents of a matrix
		[[M, N, a, b]] produces a single MxN matrix of numbers in [0, 1) and
		scales it according to: a * X + b. uses `numpy.random.random_sample`

	"args" is a dictionary of arguments to pass to the operation specified for
	the test, along with an array of the input matrices.

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


EXTENSION = ".f32"

def create_column(N, M):
	return np.random.randint(0, M, N)

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
		if os.path.exists(directory + 'out.json'):
			print("Skipping {0}".format(directory))
			continue

		id_names = map(lambda i: "id_{0}".format(i), range(len(options['id'])))
		value_names = map(lambda i: "value_{0}".format(i), range(len(options['value'])))

		all_exist = False
		for name in id_names + value_names:
			all_exist = all_exist & os.path.exists(directory + name + EXTENSION)

		if all_exist:
			id_columns = {name : binary_matrix.read(directory + name + EXTENSION) for name in id_names}
			value_columns = {name : binary_matrix.read(directory + name + EXTENSION) for name in value_names}

		else:
			id_columns = {}
			for i in range(len(id_names)):
				name = id_names[i]
				spec = options['id'][i]
				column = create_column(N, spec['M'])
				id_columns[name] = column

				binary_matrix.write(directory + name + EXTENSION, column)

			value_columns = {}
			for i in range(len(value_names)):
				name = value_names[i]
				spec = options['value'][i]
				column = create_column(N, spec['M'])
				value_columns[name] = column

				binary_matrix.write(directory + name + EXTENSION, column)

		# run reduction
		arguments = options['arg'] if 'arg' in options else {}
		out = operation.execute(arguments, id_columns, value_columns)

		# write result
		#binary_matrix.write(directory + "out.arr", out.flatten())
		write_result(out, directory + "out.json")

		print("Created {0}".format(directory))
