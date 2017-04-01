"""mean operation
	find the mean (average) of a column
"""
import pandas as pd
import math

def execute(options, id_columns, value_columns):
	'''
		id_columns - a dictionary mapping names (strings) to numpy arrays
		value_columns - a dictionary mapping names (strings) to numpy arrays

	'''

	columns = id_columns.copy()
	columns.update(value_columns)

	frame = pd.DataFrame(columns)

	v = frame.mean()["value_0"]

	return v.item() # convert from numpy type to python type
