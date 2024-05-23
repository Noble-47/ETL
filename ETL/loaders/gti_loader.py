from loaders.base import BaseLoaderClass


class GTIMergeLoader(BaseLoaderClass):

    name = "GTI Merge Loader"
    default_data_dir = Path("data/gti/transformed")
    default_save_dir = default_data_dir.parent / "loaded"

    def load(self):

        dataset = self.fetch()

        if len(dataset) > 1:
            merged_data = pd.concat(dataset)
        else:
            merged_data = dataset[0]

        self.write("gti", merged_data)
