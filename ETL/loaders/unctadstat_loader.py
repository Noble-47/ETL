"""
Merge dataset and write to excel
"""

from loaders.base import BaseLoaderClass

from functools import reduce
from pathlib import Path


class UnctadStatLoader(BaseLoader):

    name = "UnctadStat Loader"
    default_data_dir = Path("data/unctadstat/transformed")
    default_save_dir = default_data_dir.parent / "loaded"

    def load(self):
        dataset = self.fetch()
        if len(dataset) > 1:
            merged_data = reduce(
                lambda left, right: pd.merge(
                    left, right, on=["Country Name", "year"], how="left"
                ),
                dataset,
            ).sort_values(by=["Country Name", "year"])

        else:
            merged_data = dataset[0].sort_values(by=["Country Name", "year"])

        self.write("gti", merged_data)
