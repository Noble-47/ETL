"""
Contains the base class for all tranformers class to be defined
All tranformers are to inherit from BaseTransformClass to access
its read method and logger attribute
"""
from utils import IOMixin

from parallelbar import progress_imap
from pathlib import Path
import multiprocessing
import pandas as pd
import logging
import abc

logger = logging.getLogger("ETL.Transform")


class BaseTransformClass(abc.ABC, IOMixin):
    """Base Class for transformers"""

    name = ""

    def __init__(self, data_dir=None, save_dir=None, save_file_type="excel"):
        self.name = self.__class__.name or self.__class__.__name__
        self.logger = logging.getLogger(f"ETL.Transform.{self.name}")
        self.extension_reader = {".xlsx": pd.read_excel, ".csv": pd.read_csv}
        self._save_file_type = save_file_type

        if data_dir is None:
            data_dir = Path("data") / self.name
   
        if save_dir is None:
            save_dir = self.data_dir / "transformed"


        self.data_dir = data_dir if instance(data_dir, Path) else Path(data_dir)
        self.save_dir = save_dir if isinstance(save_dir, Path) else Path(save_dir)
        if not self.save_dir.is_dir():
            self.save_dir.mkdir()

    @abc.abstractmethod
    def transform(self, data_dict):
        """
        Serves as the entry point to the transformer object.
        All tranformation function or steps should be carried out in the function.

        data_dict : A dictionary containing the data file name and the pandas dataframe of the data file

        returns a dictionary containing the data file name and the transformed pandas dataframe
        """
        raise NotImplemented

    def fetch_data(self):
        """
        Reads in the data files contain in the data directory
        returns a dictionary of filename in lower case as the key and
        the read in data content as the value
        """
        try:
            files = [f for f in self.data_dir.iterdir() if f.is_file()]
        except Exception as e:
            self.logger.exception(
                "Encountered An Error trying to read %s ", self.data_dir
            )
            raise e
        else:
            self.logger.info("%d files found in %s folder", len(files), self.data_dir)
        datasets = []
        for fn in files:
            name = fn.name.lower().split(".")[0]
            datasets.append({"name": name, "data": self.read(fn)})
        return datasets

    def run_transformation(self, workers=None, write=True):
        """
        Run the transform method of the given transformation class concurrently
        in a multi-core process using the specified number of `workers`.

        The tranformed data is also passed to a merge method and attaches and returns
        the result of the merge operation
        """
        cpu_count = multiprocessing.cpu_count()
        workers = workers if workers is not None else cpu_count
        self.logger.info(f"Transformation Process: Using {workers} workers")
        # with multiprocessing.Pool(processes=workers) as pool:
        #   transformed_data = pool.imap(self.transform, self.get_dataset())
        transformed_data = progress_imap(
            self.transform, self.fetch_data(), n_cpu=workers
        )
        self.logger.info("Transformation Process: Initializing Merging.")

        self.merged_data = self.merge(transformed_data)

        return self.merged_data

    def __str__(self):
        return f"{self.name} Transformer"

    def __repr__(self):
        return f"{self.__class__.__name__}()"
