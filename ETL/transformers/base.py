"""
Contains the base class for all tranformers class to be defined
All tranformers are to inherit from BaseTransformClass to access
its read method and logger attribute
"""

from parallelbar import progress_imap
from pathlib import Path
import multiprocessing
import pandas as pd
import logging
import abc

logger = logging.getLogger("ETL.Transform")


class BaseTransformClass(abc.ABC):
    """Base Class for transformers"""

    name = ""

    def __init__(self, data_dir=None, save_dir=None, save_file_type="excel"):
        self.name = self.__class__.name or self.__class__.__name__
        self.logger = logging.getLogger(f"ETL.Transform.{self.name}")
        self.extension_reader = {".xlsx": pd.read_excel, ".csv": pd.read_csv}
        self._save_file_type = save_file_type

        if data_dir is None:
            data_dir = Path("data") / self.name

        if isinstance(data_dir, Path):
            self.data_dir = data_dir

        if isinstance(data_dir, str):
            self.data_dir = Path(data_dir)

        if save_dir is None:
            self.save_dir = self.data_dir / "transformed"

        else:
            if isinstance(save_dir, str):
                self.save_dir = Path(save_dir)

            if isinstance(save_dir, Path):
                self.save_dir = save_dir

        if not self.save_dir.is_dir():
            self.save_dir.mkdir()

    def get_extension_and_writer(self):
        return {
            "excel": ("xlsx", pd.DataFrame.to_excel),
            "csv": ("csv", pd.DataFrame.to_csv),
        }.get(self._save_file_type)

    def read(self, fn):
        """Reads in data file using the appriopriate reader for the given file extension"""
        ext = fn.suffix
        reader = self.extension_reader.get(ext)
        return reader(fn)

    def write(self, name, data):
        """Writes data to the folder specified by `self.data_dir`"""
        ext, writer = self.get_extension_and_writer()
        filename = self.save_dir / f"{name}.{ext}"
        writer(data, filename, index=False)

    @abc.abstractmethod
    def transform(self, data_dict):
        """
        Serves as the entry point to the transformer object.
        All tranformation function or steps should be carried out in the function.

        data_dict : A dictionary containing the data file name and the pandas dataframe of the data file

        returns a dictionary containing the data file name and the transformed pandas dataframe
        """
        raise NotImplemented

    def get_dataset(self):
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
            self.logger.info(f"Transformation Initialization: Submitting {fn}")
            datasets.append({"name": name, "data": self.read(fn)})
        return datasets

    def merge(transformed_data):
        raise NotImplemented

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
            self.transform, self.get_dataset(), n_cpu=workers
        )
        self.logger.info("Transformation Process: Initializing Merging.")

        self.merged_data = self.merge(transformed_data)

        return self.merged_data

    def __str__(self):
        return f"{self.name} Transformer"

    def __repr__(self):
        return f"{self.__class__.__name__}()"
