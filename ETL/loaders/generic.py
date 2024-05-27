"""
Merge dataset and write to excel
"""

from loaders.base import BaseLoaderClass

from functools import reduce
from pathlib import Path


class GenericMergeLoader(BaseLoaderClass):

    name = "Generic Merge Loader"
    default_data_dir = Path("data/loaded")
    default_save_dir = Path("data/loaded")

    def load(self):
        dataset = self.fetch()
        if len(dataset) > 1:
            merged_data = reduce(
                lambda left, right: pd.merge(
                    left, right, on=["Country Name", "year"], how="outer"
                ),
                dataset,
            ).sort_values(by=["Country Name", "year"])

        else:
            merged_data = dataset[0].sort_values(by=["Country Name", "year"])

        self.write("gti", merged_data)
