from transformers.base import BaseTransformClass

from pathlib import Path
import pandas as pd

countries_to_replace = {
    "Egypt": "Egypt, Arab Rep.",
    "Democratic Republic of the Congo": "Congo, Dem. Rep.",
    "Republic of the Congo": "Congo, Rep.",
    "Cote d' Ivoire": "Cote d'Ivoire",
}


class GTITransformer(BaseTransformClass):
    parent = Path("data/gti")
    default_data_dir = parent
    default_save_dir = parent / "extract"

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


# gti_transformer = GTITransformer('../../data/gti')
# gti_transformer.run_transformation()
