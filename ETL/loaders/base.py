from pathlib import Path
import logging
import abc

from utils.io import IOMixin

class BaseLoaderClass(abc.ABC, IOMixin):
    """Base Loader class"""

    name = ""
    default_data_dir = ""
    default_save_dir = ""

    def __init__(self, data_dir=None, save_dir=None, save_file_type="excel"):
        self.name = self.__class__.name or self.__class__.__name__
        self.logger = logging.getLogger(f"ETL.Loader.{self.name}")
        self.save_file_type = save_file_type
        self.setup_directories(data_dir, save_dir)

    @abc.abstractmethod
    def load(self):
        pass
