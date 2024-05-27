from xml.etree.ElementTree import Element, SubElement, tostring, ElementTree
import json
import csv
import io

class BaseParser:
    def __init__(self, report):
        self.raw_report = report
    
    def parse_report(self, report):
        pass

    def emit(self):
        raw_report = self.raw_report
        self.report = self.parser_report(raw_report)
        return self.report


class TextParser(BaseParser):
    def parse_report(self, report):
        text_output = []
        processes = report.get('processes', [])

        for process in processes:
            process_name = process.get('process', 'Unknown Process')
            text_output.append(f"Process: {process_name}")
            for obj in process.get('objects', []):
                text_output.append(f"\tObject Name: {obj.get('name', 'Unknown Name')}")
                metrics = obj.get('metrics', {})
                for metric, value in metrics.items():
                    text_output.append(f"\t\t{metric}: {value}")

        return "\n".join(text_output)
    

class ConsoleStreamParser(TextParser):
    pass


class MarkdownParser(BaseParser):
    def parse_report(self, report):
        markdown_output = []
        processes = report.get('processes', [])
        
        for process in processes:
            process_name = process.get('process', 'Unknown Process')
            markdown_output.append(f"## Process: {process_name}")
            for obj in process.get('objects', []):
                markdown_output.append(f"### Object Name: {obj.get('name', 'Unknown Name')}")
                metrics = obj.get('metrics', {})
                for metric, value in metrics.items():
                    markdown_output.append(f"- **{metric}**: {value}")
                    
        return "\n".join(markdown_output)
          

class JsonParser(BaseParser):
    def parse_report(self, report, indent=4):
        return json.dumps(report, indent=indent)


class CSVParser(BaseParser):
    def parse_report(self, report):
        output = io.StringIO()
        csv_writer = csv.writer(output)

        # Write CSV header
        csv_writer.writerow(['process', 'object', 'metric', 'value'])

        processes = report.get('processes', [])
        for process in processes:
            process_name = process.get('process', 'Unknown Process')
            for obj in process.get('objects', []):
                object_name = obj.get('name', 'Unknown Name')
                metrics = obj.get('metrics', {})
                for metric, value in metrics.items():
                    csv_writer.writerow([process_name, object_name, metric, value])

        return output.getvalue()


class YAMLParser(BaseParser):
    def parse_report(self, report):
        return yaml.dump(report, default_flow_style=False)


class XMLParser(BaseParser):
    def parse_report(self, report):
        root = Element('report')
        processes = report.get('processes', [])

        for process in processes:
            process_element = SubElement(root, 'process')
            process_element.set('name', process.get('process', 'Unknown Process'))
            for obj in process.get('objects', []):
                object_element = SubElement(process_element, 'object')
                object_element.set('name', obj.get('name', 'Unknown Name'))
                metrics = obj.get('metrics', {})
                for metric, value in metrics.items():
                    metric_element = SubElement(object_element, 'metric')
                    metric_element.set('name', metric)
                    metric_element.text = str(value)

        return tostring(root, encoding='unicode')

