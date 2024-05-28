from report.parsers import *

import json


class Report:

    def __init__(self):
        self.processes = []

    def add_process_metric(self, process_metric):
        self.processes.append(process_metric)

    def as_text(self):
        return TextParser(self.report)

    def as_markdown(self):
        return MarkdownParser(self.report)

    def as_json(self):
        return JsonParser(self.report)

    def as_csv(self):
        return CSVParser(self.report)

    def as_yaml(self):
        return YAMLParser(self.report)

    def as_xml(self):
        return XMLParser(self.report)

    @property
    def report(self):
        return {"processes": self.processes}


class ProcessMetricFactory:

    def __call__(self, process_name):
        return ProcessMetric(process_name)


class ProcessMetric:

    def __init__(self, process_name):
        self.name = process_name
        self.objects = list()

    def add(self, obj_metric):
        self.objects.append(obj_metric)

    def emit(self):
        return {"process": self.name, "objects": self.objects}


# def __call__(self, process_name):
#     return ProcessMetric(process_name)


class Metric:

    def __init__(self, name):
        self.metrics = dict()
        self.name = name

    def add(self, **component):
        self.metrics.update(component)

    def emit(self):
        return {"name": self.name, "metrics": self.metrics}

    def __repr__(self):
        return "Metric <name=self.name>"

    def __str__(self):
        return self.emit()
