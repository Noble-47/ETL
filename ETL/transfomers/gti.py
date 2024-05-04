from base import BaseTransformClass
import pandas as pd

COLUMN_NAMES = []
COUNTRY_NAMES = []


class GTITranformer(BaseTransformClass):

    def merge(self):
        dataset = self.get_dataset()
        data = pd.concat(dataset).sort_values(by=["name", "year"])
        return data

    def rename_countries(self, data):
        return data

    def rename_columns(self, data):
        return data

    def tranform(self):
        data = self.merge_dataset()
        data = self.rename_countries(data)
        data = self.rename_columns(data)
        self.write(data, subdir="gti")
        return data
