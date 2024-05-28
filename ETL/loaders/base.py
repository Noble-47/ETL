from pathlib import Path
import logging
import abc

from utils.io import IOMixin
from report.components import Metric


class BaseLoaderClass(abc.ABC, IOMixin):
    """Base Loader class"""

    name = ""
    default_data_dir = ""
    default_save_dir = ""

    def __init__(
        self, data_dir=None, save_dir=None, save_file_type="excel", metric_class=Metric
    ):
        self.name = self.__class__.name or self.__class__.__name__
        self.logger = logging.getLogger(f"ETL.Loader.{self.name}")
        self.save_file_type = save_file_type
        self.setup_directories(data_dir, save_dir)
        self.setup_metric_component(metric_class)

    def setup_metric_component(self, metric_class):
        self.metric = metric_class(self.name)
        # self.metric.add(name = self.name)
        self.metric.add(save_directory=str(self.save_dir))
        self.metric.add(write_directory=str(self.data_dir))

    @abc.abstractmethod
    def load(self, dataset):
        pass

    def load_data(self):
        dataset = self.fetch()
        self.load(dataset)
        return self.metric
