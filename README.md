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
    
    - `name (str)`: The name of the extractor.
    - `domain (str)`: The domain associated with the extractor.
    - `default_save_dir (str)`: The default directory to save downloaded files.
    - `save_dir (str)`: The directory where files will be saved.
    - `metric_class (class)`: The class used for tracking extraction metrics.
    - `logger (Logger)`: Logger for the extractor.
    - `progress_bar (ProgressBar)`: Progress bar for tracking download progress.
    - `download_tasks (int)`: Number of download tasks.

### Methods
    
    - `__init__(self, save_dir=None, metric_class=Metric)`: Initializes the extractor, sets up the save directory, and initializes the metric component.
    - `get_links(self)`: Abstract method that must be implemented in subclasses. It should return a list of links or yield links to be scheduled for download.
    - `async handle_request(self, session, link)`: Handles the setup for downloading a link, returns a download object containing the name and content of the download.
    - `async start_request(self)`: Creates and starts download tasks using links from get_links.
    - `async write(self)**: Creates write tasks to save downloaded content.
    - `async write_download(self, download)`: Writes the content of the download object to a file asynchronously using aiofiles.
    - `extract(self, pbar=None)`: Entry point that orchestrates the extraction process and returns a metric object summarizing the extraction.

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


# Transformation Component of ETL Project

This transformation component is a part of a larger ETL (Extract, Transform, Load) project. It is designed to transform data obtained from various sources, leveraging a base class to provide core transformation functionalities. The component supports concurrent data transformation using multiple CPU cores to enhance performance and efficiency.

## Class Structure

### `BaseTransformClass`

The `BaseTransformClass` is an abstract base class that provides foundational methods and structure for all transformer implementations. This class should be subclassed to create specific transformers tailored to different data transformation needs.

#### Attributes

- `name` (str): The name of the transformer.
- `default_data_dir` (str): The default directory where raw data files are stored.
- `default_save_dir` (str): The default directory where transformed files are saved.
- `save_file_type` (str): The file type for saving transformed data (default is "excel").
- `logger` (Logger): Logger for the transformer.
- `metric` (Metric): Metric object to track transformation metrics.

#### Methods

- `__init__(self, data_dir=None, save_dir=None, save_file_type="excel", metric_class=Metric)`: Initializes the transformer, sets up directories, and initializes the metric component.
- `@abc.abstractmethod def transform(self, data_dict)`: Abstract method that must be implemented in subclasses. It defines the transformation logic to be applied to the data.
- `def fetch_data(self)`: Fetches the raw data to be transformed. This method can be overridden to implement specific data fetching logic.
- `def run_transformation(self, workers=None)`: Runs the transformation process concurrently using multiple CPU cores. It merges the transformed data and returns the result of the merge operation along with transformation metrics.

### Implementation Details

- **Parallel Processing**: The `run_transformation` method uses the `parallelbar` library to run transformations concurrently, utilizing multiple CPU cores to process data files in parallel.
- **Metrics and Logging**: The transformer logs its activities and tracks metrics to provide detailed summaries of the transformation process, aiding in monitoring and debugging.
- **Progress Tracking**: The transformation process includes a progress bar to visually track the progress of data transformations, enhancing user experience for long-running operations.

## Usage

To create a specific transformer, subclass `BaseTransformClass` and implement the `transform` method. Here is an example of how to create a custom transformer:

```python
from your_module import BaseTransformClass, Metric

class MyCustomTransformer(BaseTransformClass):
    
    name = "CustomTransformer"
    default_data_dir = "/path/to/data"
    default_save_dir = "/path/to/save"

    def transform(self, data_dict):
        # Implement the transformation logic here
        transformed_data = some_transformation_function(data_dict)
        return transformed_data

# Usage
transformer = MyCustomTransformer()
transformer.run_transformation()
```

## Example Subclass

Here is a more detailed example of a subclass that transforms data obtained from [vision of humanity](https://www.visionofhumanity.org/)

```python
import pandas as pd

class GTITransformer(BaseTransformClass):
    parent = Path("data/gti")
    default_data_dir = parent / "extracted"
    default_save_dir = parent / "transformed"

    filter_prefix = "index"
    countries_to_replace = {
        "Egypt, Arab Rep.": "Egypt",
        "Democratic Republic of the Congo": "Congo, Dem. Rep.",
        "Republic of the Congo": "Congo, Rep.",
        "Cote d' Ivoire": "Cote d'Ivoire",
    }

    column_names = {
        "code": "Country Code",
        "name": "Country Name",
        "year": "year",
        f"{filter_prefix}_over": "overall",
        f"{filter_prefix}_inci": "inci",
        f"{filter_prefix}_fat": "fat",
        f"{filter_prefix}_inj": "inj",
        f"{filter_prefix}_prop": "prop",
    }

    def rename_countries(self, data):
        """
        Renames Countries spelt differently in order to have
        a uniform data across all data source
        """
        data["Country Name"] = data["Country Name"].replace(self.countries_to_replace)
        return data

    def rename_columns(self, data):
        """
        Renames data columns
        """
        data = data.rename(columns=self.column_names)
        return data

    def filter_dataset(self, df):
        "Return columns that contains the filter_prefix"
        self.logger.info("Filtering dataset")
        columns = ["name", "code", "year"]
        columns.extend(
            [column for column in df.columns if column.startswith(self.filter_prefix)]
        )
        return df[columns]

    def transform(self, data_dict):
        name = data_dict.get("name")
        data = data_dict.get("data")

        data = self.filter_dataset(data)
        data = self.rename_columns(data)
        data = self.rename_countries(data)
        self.write(name, data)

        return data


# Usage
transformer = GTITransformer()
metric = transformer.run_transformation()
print(metric.summary())
```


# Loader Component of ETL Project

## Overview

This loader component is part of a larger ETL (Extract, Transform, Load) project. It is responsible for loading transformed data into the desired storage format or location. The component is designed with flexibility and reusability in mind, utilizing an abstract base class to define core loading functionalities.

## Class Structure

### `BaseLoaderClass`

The `BaseLoaderClass` is an abstract base class that provides foundational methods and structure for all loader implementations. This class should be subclassed to create specific loaders tailored to different loading needs.

#### Attributes

- `name` (str): The name of the loader.
- `default_data_dir` (str): The default directory where input data files are stored.
- `default_save_dir` (str): The default directory where loaded files are saved.
- `save_file_type` (str): The file type for saving loaded data (default is "excel").
- `logger` (Logger): Logger for the loader.
- `metric` (Metric): Metric object to track loading metrics.

#### Methods

- `__init__(self, data_dir=None, save_dir=None, save_file_type="excel", metric_class=Metric)`: Initializes the loader, sets up directories, and initializes the metric component.
- `setup_metric_component(self, metric_class)`: Sets up the metric component for tracking loader metrics.
- `@abc.abstractmethod def load(self, dataset)`: Abstract method that must be implemented in subclasses. It defines the logic for loading the dataset.
- `def load_data(self)`: Fetches the data and runs the load method, then returns the metric object.

### Implementation Details

- **Metric Tracking**: The loader tracks various metrics related to the loading process, such as the number of files read, written, and specific operations performed. These metrics aid in monitoring and debugging the loading process.
- **Logging**: The loader logs its activities, providing detailed insights into the loading process.

## Usage

To create a specific loader, subclass `BaseLoaderClass` and implement the `load` method. Here is an example of how to create a custom loader:

```python
from your_module import BaseLoaderClass, Metric
from pathlib import Path
import pandas as pd
from functools import reduce

class GenericMergeLoader(BaseLoaderClass):

    name = "Generic Merge Loader"
    default_data_dir = Path("data/loaded")
    default_save_dir = Path("data/loaded")

    def load(self, dataset):
        len_read_files = len(dataset)
        self.metric.add(number_of_files_read=len_read_files)
        if len_read_files > 1:
            merged_data = reduce(
                lambda left, right: pd.merge(
                    left, right, on=["Country Name", "year"], how="outer"
                ),
                dataset,
            ).sort_values(by=["Country Name", "year"])
        else:
            merged_data = dataset[0].sort_values(by=["Country Name", "year"])

        self.write("merged_data", merged_data)
        self.metric.add(number_of_written_files=1)
        self.metric.add(operations=["concatenation", "sorting"])

# Usage
loader = GenericMergeLoader()
metric = loader.load_data()
print(metric.summary())
```

## Example Subclass

Here is a more detailed example of a subclass that loads data by merging multiple datasets:

```python
import pandas as pd
from functools import reduce

class CSVLoader(BaseLoaderClass):
    
    name = "CSVLoader"
    default_data_dir = Path("data/csv_files")
    default_save_dir = Path("data/loaded_files")

    def load(self, dataset):
        len_read_files = len(dataset)
        self.metric.add(number_of_files_read=len_read_files)
        
        if len_read_files > 1:
            merged_data = reduce(
                lambda left, right: pd.merge(
                    left, right, on=["ID", "Date"], how="outer"
                ),
                dataset,
            ).sort_values(by=["ID", "Date"])
        else:
            merged_data = dataset[0].sort_values(by=["ID", "Date"])

        self.write("merged_data", merged_data)
        self.metric.add(number_of_written_files=1)
        self.metric.add(operations=["merge", "sort"])

    def fetch(self):
        # Custom logic to fetch CSV files
        csv_files = [pd.read_csv(f) for f in self.data_dir.iterdir() if f.suffix == '.csv']
        return csv_files

# Usage
loader = CSVLoader()
metric = loader.load_data()
print(metric.summary())
```

# Reporting Component of ETL Project

## Overview

The reporting component is a crucial part of the ETL (Extract, Transform, Load) process, responsible for generating summaries and detailed reports of the various ETL operations. Each component of the ETL pipeline (Extraction, Transformation, Loading) has a metric property that tracks and stores metrics, which are then used to create comprehensive reports.

## Metric Classes

### `Metric`

The `Metric` class is designed to store metrics for an individual component instance.

#### Attributes

- `metrics` (dict): Dictionary to store metrics.
- `name` (str): Name of the metric.

#### Methods

- `__init__(self, name)`: Initializes the metric with a name.
- `add(self, **component)`: Adds metrics to the metric object.
- `emit(self)`: Returns the metric content as a dictionary.
- `__repr__(self)`: Returns a string representation of the metric.
- `__str__(self)`: Returns the emitted metric content as a string.

### Example Usage

```python
metric = Metric("ExampleMetric")
metric.add(source="example.com", files_processed=10)
print(metric.emit())
```

### `ProcessMetric`

The `ProcessMetric` class stores the metrics of all instances of a particular ETL process (e.g., Extraction, Transformation, Loading).

#### Attributes

- `name` (str): Name of the process.
- `objects` (list): List to store metrics of each component instance.

#### Methods

- `__init__(self, process_name)`: Initializes the process metric with a process name.
- `add(self, obj_metric)`: Adds an object metric to the process metric.
- `emit(self)`: Returns the process metrics as a dictionary.

### Example Usage

```python
process_metric = ProcessMetric("Extraction")
process_metric.add(metric)
print(process_metric.emit())
```

### `Report`

The `Report` class collects `ProcessMetric` objects and generates a comprehensive report.

#### Attributes

- `processes` (list): List to store process metrics.

#### Methods

- `__init__(self)`: Initializes the report.
- `add_process_metric(self, process_metric)`: Adds a process metric to the report.
- `as_text(self)`: Returns the report as text using `TextParser`.
- `as_markdown(self)`: Returns the report as markdown using `MarkdownParser`.
- `as_json(self)`: Returns the report as JSON using `JsonParser`.
- `as_csv(self)`: Returns the report as CSV using `CSVParser`.
- `as_yaml(self)`: Returns the report as YAML using `YAMLParser`.
- `as_xml(self)`: Returns the report as XML using `XMLParser`.
- `report(self)`: Property that returns the report as a dictionary.

### Example Usage

```python
report = Report()
report.add_process_metric(process_metric)
report.as_text().display()
```

## Parser Classes

### `BaseParser`

The `BaseParser` class is an abstract base class for all parsers.

#### Methods

- `__init__(self, report)`: Initializes the parser with a report.
- `parse_report(self, report)`: Abstract method to parse the report.
- `display(self)`: Prints the parsed report.
- `emit(self)`: Returns the parsed report.
- `write(self, file, mode)`: Writes the parsed report to a file.

### `TextParser`

The `TextParser` class parses the report into a plain text format.

#### Methods

- `parse_report(self, report)`: Parses the report into plain text.

### Example Usage

```python
text_parser = TextParser(report.report)
text_parser.display()
```

### `MarkdownParser`

The `MarkdownParser` class parses the report into a markdown format.

#### Methods

- `parse_report(self, report)`: Parses the report into markdown.

### Example Usage

```python
markdown_parser = MarkdownParser(report.report)
markdown_parser.display()
```

## Example Report

Here is an example of what a raw report might look like:

```python
report = Report()
extraction_metric = Metric("vision_of_humanity")
extraction_metric.add(source="www.visionofhumanity.org", save_directory="data/gti/extracted", start_year=2011, end_year=2023, upload_year="2024", number_of_files_downloaded=12)
extraction_process = ProcessMetric("Extraction")
extraction_process.add(extraction_metric)

transformation_metric = Metric("GTITransformer")
transformation_metric.add(data_directory="data/gti/extracted", save_directory="data/gti/transformed", number_of_files_read=12, number_of_files_written=12)
transformation_process = ProcessMetric("Transformation")
transformation_process.add(transformation_metric)

loading_metric_1 = Metric("GTI Merge Loader")
loading_metric_1.add(save_directory="data/loaded", write_directory="data/gti/transformed", number_of_files_read=12, operations=["merge", "sorting"], number_of_files_written=1)
loading_metric_2 = Metric("Generic Merge Loader")
loading_metric_2.add(save_directory="data/loaded", write_directory="data/loaded", number_of_files_read=1, number_of_written_files=1, operations=["concatenation", "sorting"])
loading_process = ProcessMetric("Loading")
loading_process.add(loading_metric_1)
loading_process.add(loading_metric_2)

report.add_process_metric(extraction_process)
report.add_process_metric(transformation_process)
report.add_process_metric(loading_process)

print(report.as_text())
```

# The Pipeline 

## Overview

The `Pipeline` class is the central component of the ETL (Extract, Transform, Load) process. It orchestrates the sequence of operations by bringing together extractors, transformers, and loaders, managing their execution, and generating comprehensive reports on the process.

## Class Definition

### `Pipeline`

The `Pipeline` class coordinates the ETL workflow by managing the addition of components, running each phase of the process, and compiling metrics into a report.

#### Attributes

- `extractors` (list): A list to store extractor objects.
- `transformers` (list): A list to store transformer objects.
- `loaders` (list): A list to store loader objects.
- `report` (Report): An instance of the `Report` class to collect metrics.
- `process_metric_factory` (ProcessMetricFactory): A factory to create process metrics.
- `logger` (Logger): Logger for logging pipeline activities.

#### Methods

- `__init__(self)`: Initializes the pipeline, sets up logging, and prepares the report and process metric factory.
- `setup_logging(self)`: Configures logging based on a configuration file.
- `create_object(cls_)`: A helper method to create objects from class and parameters.
- `add(self, **kwargs)`: Adds extractors, transformers, or loaders to the pipeline.
- `add_extractor(self, extractor)`: Adds a single extractor to the pipeline.
- `add_transformer(self, transformer)`: Adds a single transformer to the pipeline.
- `add_loaders(self, loaders)`: Adds a single loader to the pipeline.
- `clear(self)`: Clears all components from the pipeline.
- `outline(self)`: Returns a string outlining the current configuration of the pipeline.
- `run_extractors(self)`: Runs all extractors and collects their metrics.
- `run_transformers(self)`: Runs all transformers and collects their metrics.
- `run_loaders(self)`: Runs all loaders and collects their metrics.
- `run(self)`: Runs the entire ETL process in sequence (extraction, transformation, loading) and returns the report.

### Example Usage

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
        GTIExtractor(start=2011, end=2015, upload="2024"),
        UnctadStatExtractor(variables=["US.PCI"])
    ],
    transformers=[
        GTITransformer(),
        UnctadStatTransformer()
    ],
    loaders=[
        UnctadStatLoader(),
        GTILoader(),
        GenericMergeLoader()
    ]
)

print(pipeline.outline())

report = pipeline.run()
```

### Result

```
ETL Pipeline
├── Extractors
│   ├── vision_of_humanity
│   ├── unctadstat
├── Transformers
│   ├── GTITransformer
│   ├── unctadstat
└── Loaders
    ├── UnctadStat Loader
    ├── GTI Merge Loader
    ├── Generic Merge Loader

Extraction: 7it [00:10,  1.55s/it]
Transformers:
    GTITransformer
DONE: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00,  4.89it/s]
    unctadstat
DONE: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:16<00:00, 16.23s/it]
Loaders
    ---> UnctadStat Loader (running...)
    ---> GTI Merge Loader (running...)
    ---> Generic Merge Loader (running...)

>>> report.as_yaml().display()
processes:
- objects:
  - metrics:
      end_year: 2015
      number_of_files_downloaded: 4
      save_directory: data/gti/extracted
      source: www.visionofhumanity.org
      start_year: 2011
      upload_year: '2024'
    name: vision_of_humanity
  - metrics:
      number_of_files_downloaded: 1
      save_directory: data/unctadstat/extracted
      source: https://unctadstat.unctad.org
    name: unctadstat
  process: Extraction
- objects:
  - metrics:
      data_directory: data/gti/extracted
      number_of_files_read: 4
      number_of_files_written: 4
      save_directory: data/gti/transformed
    name: GTITransformer
  - metrics:
      data_directory: data/unctadstat/extracted
      number_of_files_read: 1
      number_of_files_written: 1
      save_directory: data/unctadstat/transformed
    name: unctadstat
  process: Transformation
- objects:
  - metrics:
      number_of_files_read: 1
      number_of_written_files: 1
      operations:
      - concatenation
      - sorting
      save_directory: data/loaded
      write_directory: data/unctadstat/transformed
    name: UnctadStat Loader
  - metrics:
      number_of_files_read: 4
      number_of_files_written: 1
      operations:
      - merge
      - sorting
      save_directory: data/loaded
      write_directory: data/gti/transformed
    name: GTI Merge Loader
  - metrics:
      number_of_files_read: 1
      number_of_written_files: 1
      operations:
      - concatenation
      - sorting
      save_directory: data/loaded
      write_directory: data/loaded
    name: Generic Merge Loader
  process: Loading
```

The `Pipeline` class integrates the ETL components (extractors, transformers, loaders) and manages their execution in a coordinated manner. By collecting metrics at each stage and compiling them into a comprehensive report, the pipeline provides valuable insights into the ETL process, facilitating monitoring, analysis, and optimization.
