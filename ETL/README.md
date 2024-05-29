# Python ETL Project

## Description
Borne out of the need for a module that makes the extraction, transformation and loading of world economic data intuitive, simple and adaptable to various conditions. 
This  is an ETL (Extract, Transform, Load) pipeline that focuses on getting, transforming, and loading data from a variety of data sources, all using Python.The components are implemented using Object-Oriented Programming (OOP) and are highly extensible and adaptable.

## Main Functionalities
The project consists of the major components:
1. **Extraction**: Handles getting data from web sources as CSV or Excel using asynchronous requests.
2. **Transformation**: Uses pandas to transform the data. Transformations include filtering columns, renaming columns, or replacing data values.
3. **Loading**: Performs basic loading, merging data from various data sources into one file.
4. **Pipeline**: Runs a list of extractors, transformers, and loaders, and generates a summary of the operations.
5. **Report**: Enables the reporting of summaries/metrics of each component of the ETL.

## Installation
To install the required dependencies, run:
```bash
pip install -r requirements.txt
```

## Dependencies:

    pandas
    aiohttp
    aiofile
    tqdm
    parallelbar
    black

## Usage

```python
from extractors.unctadstat import UnctadStatExtractor
from extractors.gti import GTIExtractor

from transformers.unctadstat import UnctadStatTransformer
from transformers.gti import GTITransformer

from loaders.unctadstat_loader import UnctadStatLoader
from loaders.generic import GenericMergeLoader
from loaders.gti_loader import GTILoader

from pipeline import Pipeline

pipeline = Pipeline()

pipeline.add(
    extractors=[
        GTIExtractor(**{"start": 2011, "end": 2024, "upload": 2024}),
        UnctadStatExtractor(**{"variables": ["US.PCI", "US.TermsOfTrade", "US.GDPComponent"]})
    ],
    transformers=[
        GTITransformer(),
        UnctadStatTransformer()
    ],
    loaders=[UnctadStatLoader(), GTILoader(), GenericMergeLoader()]
)

print(pipeline.outline())
pipeline.run()
```

## Project Structure

```
├── config.py
├── configs
│   └── log.json
├── extractors
│   ├── base.py
│   ├── gti.py
│   ├── __init__.py
│   └── unctadstat.py
├── __init__.py
├── loaders
│   ├── base.py
│   ├── generic.py
│   ├── gti_loader.py
│   ├── __init__.py
│   └── unctadstat_loader.py
├── logs
│   ├── error.log
│   └── info.log
├── pipeline.py
├── report
│   ├── components.py
│   ├── __init__.py
│   └── parsers.py
├── requirements.txt
├── test.py
├── transformers
│   ├── base.py
│   ├── gti.py
│   ├── __init__.py
│   └── unctadstat.py
└── utils
    ├── __init__.py
    ├── io.py
    └── log.py
```

# ETL Components:

## Extraction

This extraction component is part of a larger ETL (Extract, Transform, Load) project. Its primary function is to download data from the web in various formats such as CSV, Excel, or ZIP files. The component is designed with flexibility and reusability in mind, leveraging an abstract base class to handle core extraction functionalities.

## Class Structure

### BaseExtractor

The BaseExtractor class is an abstract base class that provides the foundational methods and structure for all extractor implementations. This class should be subclassed to create specific extractors tailored to different data sources.

### Attributes
    
    - **name (str)**: The name of the extractor.
    - **domain (str)**: The domain associated with the extractor.
    - **default_save_dir (str)**: The default directory to save downloaded files.
    - **save_dir (str)**: The directory where files will be saved.
    - **metric_class (class)**: The class used for tracking extraction metrics.
    - **logger (Logger)**: Logger for the extractor.
    - **progress_bar (ProgressBar)**: Progress bar for tracking download progress.
    - **download_tasks (int)**: Number of download tasks.

### Methods
    
    - **__init__(self, save_dir=None, metric_class=Metric)**: Initializes the extractor, sets up the save directory, and initializes the metric component.
    - **get_links(self)**: Abstract method that must be implemented in subclasses. It should return a list of links or yield links to be scheduled for download.
    - **async handle_request(self, session, link)**: Handles the setup for downloading a link, returns a download object containing the name and content of the download.
    - **async start_request(self)**: Creates and starts download tasks using links from get_links.
    - **async write(self)**: Creates write tasks to save downloaded content.
    - **async write_download(self, download)**: Writes the content of the download object to a file asynchronously using aiofiles.
    - **extract(self, pbar=None)**: Entry point that orchestrates the extraction process and returns a metric object summarizing the extraction.

## Data Classes

### Download
    
    - **name (str)**: The name of the downloaded file.
    - **content (bytes)**: The content of the downloaded file.

### Link
    
    - **url (str)**: The URL to be downloaded.
    - **name (str)**: The name associated with the link.
    - **encoding (str)**: The encoding type of the content.
    - **headers (dict)**: HTTP headers to be used in the request.
    - **is_json_content (bool)**: Flag indicating if the content is JSON.

## Usage
To create a specific extractor, subclass BaseExtractor and implement the get_links method. Here is an example of how to create a custom extractor:

```python

from your_module import BaseExtractor, Link

class MyCustomExtractor(BaseExtractor):
    
    name = "CustomExtractor"
    domain = "example.com"
    default_save_dir = "/path/to/save"

    def get_links(self):
        # Implement logic to generate or retrieve links
        links = [Link(url="http://example.com/data1.csv", name="data1"),
                 Link(url="http://example.com/data2.csv", name="data2")]
        return links


# Usage
extractor = MyCustomExtractor()
extractor.extract()
```

## Implementation Details
    
   - **Asynchronous Operations**: The use of async methods (handle_request, start_request, write, write_download) ensures that the extraction process is efficient and non-blocking, allowing multiple downloads to occur concurrently.
   - **Metrics and Logging**: The extractor logs its activities and tracks metrics to provide detailed summaries of the extraction process, aiding in monitoring and debugging.
   - **Progress Tracking**: The progress_bar attribute helps in tracking the download progress, making it easier to monitor long-running extractions.

