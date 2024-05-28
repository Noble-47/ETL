"""
Include logic for pipeline

# pipeline a way of running things in order
    - extractors : asynchronous in multiple threads 
    - transformers : mulitprocesse per transformer
    - loaders : single process 
"""

import concurrent.futures
import logging.config
import config
import atexit
import tqdm

from report.components import ProcessMetricFactory, Report


class Pipeline:

    def __init__(self):
        # self.logger = logging.getLogger("ETL.Pipeline")
        self.extractors = []
        self.transformers = []
        self.loaders = []
        self.setup_logging()
        self.report = Report()
        self.process_metric_factory = ProcessMetricFactory()

    def setup_logging(self):
        log_config = config.load_logging_config()
        logging.config.dictConfig(log_config)
        queue_handler = logging.getHandlerByName("queue_handler")
        if queue_handler is not None:
            queue_handler.listener.start()
            atexit.register(queue_handler.listener.stop)
        self.logger = logging.getLogger("ETL.pipeline")

    def create_object(cls_):
        cls, params = cls_
        return cls(**params)

    def add(self, **kwargs):
        for kw, obj_list in kwargs.items():
            if kw == "logger":
                continue

            if not hasattr(self, kw):
                error_msg = f"KeyError: Pipeline received an invalid key, {kw} with values {kwargs.get(kw)}"
                self.logger.exception(error_msg)
                raise Exception(error_msg)
            attr = getattr(self, kw)
            attr.extend(obj_list)

    def add_extractor(self, extractor):
        self.extractor.append(extractor)

    def add_transformer(self, tranformer):
        self.transformers.append(transformer)

    def add_loaders(self, loaders):
        self.loaders.append(loaders)

    def clear(self):
        # remove all processes
        self.extractors = []
        self.transformers = []
        self.loaders = []
        self.process = []

    def outline(self):
        outline_str = "ETL Pipeline\n"
        outline_str += "├── Extractors\n"
        for extractor in self.extractors:
            outline_str += f"│   ├── {extractor.name}\n"

        outline_str += "├── Transformers\n"
        for transformer in self.transformers:
            outline_str += f"│   ├── {transformer.name}\n"

        outline_str += "└── Loaders\n"
        for loader in self.loaders:
            outline_str += f"    ├── {loader.name}\n"
        return outline_str

    def run_extractors(self):
        extraction_metric = self.process_metric_factory("Extraction")
        total = sum(extractor.download_tasks for extractor in self.extractors)
        with tqdm.tqdm(total=total, desc="Extraction") as pbar:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(extractor.extract, pbar)
                    for extractor in self.extractors
                ]

                for future in concurrent.futures.as_completed(futures):
                    extraction_metric.add(future.result().emit())
                    pbar.update(1)

        self.report.add_process_metric(extraction_metric.emit())

    def run_transformers(self):
        transformation_metric = self.process_metric_factory("Transformation")
        print("Transformers:")
        for transformer in self.transformers:
            print("\t", transformer.name, end="\n\t")
            summary = transformer.run_transformation()
            transformation_metric.add(summary.emit())

        self.report.add_process_metric(transformation_metric.emit())

    def run_loaders(self):
        load_metric = self.process_metric_factory("Loading")
        print("Loaders")
        for loader in self.loaders:
            print("\t", loader.name)
            summary = loader.load_data()
            load_metric.add(summary.emit())
        self.report.add_process_metric(load_metric.emit())

    def run(self):
        # controls the step by step running process of the pipeline
        # determines how each step would run
        self.run_extractors()
        self.run_transformers()
        self.run_loaders()
        return self.report
