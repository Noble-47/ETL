from loaders.base import BaseLoaderClass


class GTILoader(BaseLoaderClass):

    name = "GTI Merge Loader"
    default_data_dir = "data/gti/transformed"
    default_save_dir = "data/loaded"

    def load(self):

        dataset = self.fetch()
        len_read_files = len(dataset)
        self.metric.add(number_of_files_read = len_read_files)
        if len_read_files > 1:
            merged_data = pd.concat(dataset)
        else:
            merged_data = dataset[0]

        merged_data = merged_data.sort(by = ["Country Name", "year"])

        self.write("gti", merged_data)
        self.metric.add(operations = ["merge", "sorting"])
        self.metric.add(number_of_files_written = 1)
