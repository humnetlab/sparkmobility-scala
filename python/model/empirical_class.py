import numpy as np
import pandas as pd
from bidict import bidict

class ODMatrix:
    """
    Take in a origin-destination dataset and transform to od matrix.
    Constructor argument: path to the parquet file that stored registered origin to destination data.
    OD_Matrix would contain two attributes:
    1. a numpy array type normalized matrix (self.matrix)
    2. an index to h3 location dictionary (self.mapping)
    """

    def __init__(self, path):
        self.df = pd.read_parquet(path)
        self.matrix = None
        self.mapping = None
        self.resolution = None
        self.length = None

    def load_matrix(self):
        columns = self.df.columns
        origin_column = columns[0]
        destination_column = columns[1]
        count_column = columns[2]

        # Registered h3 index; get the locations as union of origins and destinations
        registered_origins = self.df[origin_column]
        registered_destinations = self.df[destination_column]
        locations = pd.Series(list(set(registered_origins) & set(registered_destinations)))
        #locations = pd.Series(pd.concat([registered_origins, registered_destinations]).unique())
        self.length = len(locations)

        # self.mapping will store integer index to h3_index pairs
        self.mapping = bidict(zip(locations.index, locations.values))

        # initiate and populate the od matrix
        matrix = np.zeros((self.length, self.length))
        for row in self.df.itertuples(index=False, name=None):
            origin = row[0]
            destination = row[1]
            count = row[2]
            try: origin_id = self.mapping.inv[origin]
            except KeyError: continue
            try: destination_id = self.mapping.inv[destination]
            except KeyError: continue
            matrix[origin_id, destination_id] = count

        # Normalize the matrix
        row_sums = matrix.sum(axis=1, keepdims=True)
        self.matrix = np.divide(matrix, row_sums, where=row_sums != 0)






