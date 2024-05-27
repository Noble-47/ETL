from loaders.base import BaseLoaderClass


class GTILoader(BaseLoaderClass):

    name = "GTI Merge Loader"
    default_data_dir = "data/gti/transformed"
    default_save_dir = "data/loaded"

    def load(self):

        dataset = self.fetch()

        if len(dataset) > 1:
            merged_data = pd.concat(dataset)
        else:
            merged_data = dataset[0]

        self.write("gti", merged_data)
