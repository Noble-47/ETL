from Report.parser import TextParser, MarkdownParser, JsonParser, ConsoleStreamParser

import json

class Report:

    def generate_text_report(report, textfile, mode='w'):
        report_obj = TextParser(report)
        with open(textfile, mode) as txt:
            txt.write(report_obj.emit())
        print(f"Generated Text Report: written to {textfile}")
    
    def generate_markdown_report(report, mdfile, mode='w'):
        report_obj = MarkdownParser(report)
        with open(mdfile, mode) as md:
            md.write(report_obj.emit())
        print(f"Generated Markdown Report: written to {mdfile}")
    
    def generate_json_report(report, jsonfile, mode='w'):
        report_obj = JsonParser(report)
        with open(jsonfile, mode):
            json.dump(report_obj.emit())
        print(f"Generated Json Report: written to {jsonfile}")
    
    def generate_csv_report(report, csvfile, mode='w'):
        report_obj = CSVParser(report)
        with open(csvfile, mode) as csv:
            csv.write(report_obj.emit())
        print(f"Generated CSV Report: written to {csvfile}")
    
    def generate_yaml_report(report, yamlfile, mode='w'):
        report_obj = YAMLParser(report)
        with open(yamlfile, mode) as yml:
            yml.write(report_obj.emit())
        print(f"Generated YAML Report: written to {yamlfile}")
    
    def generate_xml_report(report, xmlfile, mode='w'):
        report_obj = XMLParser(report)
        with open(xmlfile, mode) as xml:
            xml.write(report_obj.emit())
        print(f"Generated XML Report: written to {xmlfile}")
    
    def console_display_report(report):
        report_obj = ConsoleStreamParser(report)
        print(report_obj.emit())
    

class Metric:

    def __init__(self, name):
        self.metric = dict()
        self.name = name

    def add(self, **component):
        self.metric.update(component)
        

    def emit(self):
        return {
            "name" : self.name,
            "metrics" : self.metrics
        }
