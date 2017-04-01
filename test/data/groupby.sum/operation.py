"""sum operation
"""
import pandas as pd

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

def execute(options, id_columns, value_columns):

	columns = id_columns.copy()
	columns.update(value_columns)
	#print(columns)

	frame = pd.DataFrame(columns)

	g = frame.groupby(by=list(id_columns.keys()))
	return convert_to_dict(g.sum()["value_0"])
