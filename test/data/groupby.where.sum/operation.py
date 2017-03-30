"""sum operation
"""
import pandas as pd
import math

def convert_to_dict(r):

	# returns a dictionary whose keys are tuples
	tupled = r.to_dict()

	# convert tuple keys to nested dictionaries
	dicted = {}
	for (t, k) in tupled.items():
		level = dicted

		# create a nested dictionary for each item in the tuple
		for l in t[:-1]:
			if l in level:
				level = level[l]
			else:
				level[l] = {}
				level = level[l]

		# the last level points to the value
		l = t[-1]
		level[l] = k.item() # convert numpy type to python type

	return dicted

SAMPLE = 10

def execute(options, id_columns, value_columns):
	'''
		id_columns - a dictionary mapping names (strings) to numpy arrays
		value_columns - a dictionary mapping names (strings) to numpy arrays

	'''

	columns = id_columns.copy()
	columns.update(value_columns)

	frame = pd.DataFrame(columns)

	id_name = "id_0"
	value_name = "value_0"

	# create a subset of the column values
	column = id_columns[id_name]
	uniques = set(column[:SAMPLE])
	l = int(math.ceil(len(uniques)/2.0))
	subset = sorted(list(uniques))[:l]
	#print(subset)

	#frame.loc[frame[id_name] == 1, value_name].sum()
	#v = frame.loc[frame[id_name].isin(subset), value_name].sum()
	filtered = frame.loc[frame[id_name].isin(subset)]
	grouped = filtered.groupby(by=list(id_columns.keys()))

	return convert_to_dict(grouped.sum()["value_0"])
