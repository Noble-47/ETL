import datetime as dt
import logging
import json


class JsonFormatter(logging.Formatter):

    def __init__(self, *, fmt_keys=None):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else dict()

    def format(self, record):
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record):
        fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }

        if record.exc_info is not None:
            fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            fields["stack_info"] = self.formatStack(record.stack_info)

        message = dict()

        for key, val in self.fmt_keys.items():
            msg_val = fields.pop(val, None)
            if msg_val is None:
                msg_val = getattr(record, val)
            message[key] = msg_val

        message.update(fields)

        return message
