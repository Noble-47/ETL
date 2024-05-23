import datetime as dt
import logging
import json


############################################### LOGGING UTILS ################################################################
class JsonFormatter(logging.Formatter):

    def __init__(self, *, fmt_keys = None):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else dict()

    def format(self, record):
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record):
        fields = {
                "message" : record.getMessage(),
                "timestamp" : dt.datetime.fromtimestamp(
                        record.created,
                        tz = dt.timezone.utc
                    ).isoformat()
            }

        if record.exc_info is not None:
            fields['exc_info'] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            fields['stack_info'] = self.formatStack(record.stack_info)

        message = dict()

        for key, val in self.fmt_keys.items():
            msg_val = fields.pop(val, None)
            if msg_val is None:
                msg_val = getattr(record, val)
            message[key] = msg_val

        message.update(fields)

        return message

############################ Mixin Class #######################################
class IOMixin:

    def get_extension_and_writer(self):
        return {
                "excel" : ("Xlsx", pd.DataFrame.to_excel),
                "csv" : ("csv", pd.DataFrame.to_csv)
            }.get(self.save_file_type)

    def write(self, name, data):
        ext, writer = self.get_extension_and_writer()
        filename = self.save_dir / f"{name}.{ext}"
        writer(data, filename, index=False)

    def fetch(self):
        try:
            files = [f for f in self.data_dir.iterdir() if f.is_file()]
        except Exception as e:
            self.logger.exception(
                    "Encountered an error trying to read %s", self.data_dir
                )
            raise e
        else:
            self.logger.info("%d files found in %s filder", len(files), self.data_dir.name)

        datasets = [self.read(f) for f in files]
        return datasets

    def read(self, fn):
        ext = fn.suffix
        reader = self.extension.get(ext)
        return reader(fn)

