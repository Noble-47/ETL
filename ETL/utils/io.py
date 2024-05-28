from pandas import read_csv, read_excel
from pandas import DataFrame
from pathlib import Path
import os


class IOMixin:

    extension_reader = {".xlsx": read_excel, ".csv": read_csv}
    extension_and_writer = {
        "excel": ("xlsx", DataFrame.to_excel),
        "csv": ("csv", DataFrame.to_csv),
    }

    def setup_data_dir(self, data_dir):
        data_dir = data_dir or self.default_data_dir
        self.data_dir = data_dir if isinstance(data_dir, Path) else Path(data_dir)
        # check if program have write permission

    #   if not os.access(data_dir, mode=os.W_OK):
    #       self.logger.critical(
    #           f"Permission Error: Do not have permission to write to {self.data_dir}"
    #       )
    #       # Raise permission error
    #       raise Exception

    def setup_save_dir(self, save_dir):
        save_dir = save_dir or self.default_save_dir
        self.save_dir = save_dir if isinstance(save_dir, Path) else Path(save_dir)
        if not self.save_dir.is_dir():
            self.save_dir.mkdir(parents=True, exist_ok=True)

    def setup_directories(self, data_dir, save_dir):
        self.setup_data_dir(data_dir)
        self.setup_save_dir(save_dir)

    def get_extension_and_writer(self):
        return self.extension_and_writer.get(self.save_file_type)

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
            self.logger.info(
                "%d files found in %s filder", len(files), self.data_dir.name
            )

        datasets = [self.read(f) for f in files]
        return datasets

    def read(self, fn):
        ext = fn.suffix
        reader = self.extension_reader.get(ext)
        return reader(fn)
