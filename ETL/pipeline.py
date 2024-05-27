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

class Pipeline:
    
    def __init__(self): 
        #self.logger = logging.getLogger("ETL.Pipeline")
        self.extractors = []
        self.transformers = []
        self.loaders = []
        self.setup_logging()

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
        with tqdm.tqdm(total=len(self.extractors), desc="Extraction") as pbar:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(extractor.extract) for extractor in self.extractors]

                for future in concurrent.futures.as_completed(futures):
                    pbar.update(1)

    def run_transformers(self):
        print("Transformers:")
        for transformer in self.transformers:
            print("\t", transformer.name, end = "\n\t")
            transformer.run_transformation()
                    
    def run_loaders(self):
        print("Loaders")
        for loader in self.loaders:
            print("\t", loader.name, end="\n\t")
            loader.load()

    def run(self):
        # controls the step by step running process of the pipeline
        # determines how each step would run
        self.run_extractors()
        self.run_transformers()
        self.run_loaders()

        

