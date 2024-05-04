import pandas as pd
import logging
import os


logger = logging.getLogger('ETL.Transform')

class BaseTransformClass:

    def __init__(self, directory, file_type):
        self.directory = directory
        self.logger_name = self.__class__.__name__
        self.logger = logging.getLogger(f'ETL.Transform.{self.logger_name}')
        self.extension_reader - {".xlsx": pd.read_excel, ".csv": pd.read_csv}

    def read(self, fn):
        ext = fn.split(".")[1]
        reader = self.extension_reader.get(ext)
        return reader(fn)

    def write(self, subdir):
        pass

    def transform(self):
        raise NotImplemented

    def get_dataset(self):
        datasets = dict(
            [(fn.split(".")[0].lower(), self.read(fn)) for fn in self.directory]
        )

        return datasets

    def __str__(self):
        return f"{self.source_name} Transformer"

    def __repr__(self):
        return f"{self.__class__.__name__}(directory = {self.directory})"
