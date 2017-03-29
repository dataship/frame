"""sum operation
"""
import pandas as pd
import math

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
	print(subset)

	#frame.loc[frame[id_name] == 1, value_name].sum()
	v = frame.loc[frame[id_name].isin(subset), value_name].sum()

	return v.item() # convert from numpy type to python type
