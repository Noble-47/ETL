
from transformers.gti import GTITransformer
from extractors.gti import gti
import logging.config
import config
import atexit
import pathlib
def setup_logging():
    log_config = config.load_logging_config()
    logging.config.dictConfig(log_config)
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)

setup_logging()
logger = logging.getLogger("ETL")
gti.run()
data_dir = pathlib.Path("../data/gti")

#gti_transformer = GTITransformer(data_dir)
#gti_transformer.run_transformation()
#
