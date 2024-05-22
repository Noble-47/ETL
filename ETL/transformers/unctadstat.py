from transformers.base import BaseTransformClass

from functools import reduce


class UnctadStatTransformer(BaseTransformClass):

    name = "unctadstat"

    def rename_columns(self, data):
        columns = [col.lower().replace(" ", "_") for col in data.columns]
        data.columns = columns

        required_columns = ["economy_label", "year"]
        for col in data.columns:
            if col in required_columns:
                continue
            elif col.endswith("_footnote") or col.endswith("_missing_value"):
                continue
            elif col == "category" or col == "economy":
                continue
            else:
                required_columns.append(col)

        data = data[required_columns]
        return data.rename(
            columns={"economy_label": "Country Name", "category_label": "category"}
        )

    def rename_countries(self, data):
        # use gpt to fill this
        data["Country Name"] = data["Country Name"].replace({"Egypt": "Egypt"})

        return data

    def transform(self, data_dict):
        name = data_dict.get("name")
        data = data_dict.get("data")
        data = self.rename_columns(data)
        data = self.rename_countries(data)
        data = data.sort_values(by=["Country Name", "year"])
        self.write(name, data)

        return data

    def merge(self, transformed_dataset):
        if len(transformed_dataset) > 1:
            merged_data = reduce(
                lambda left, right: pd.merge(
                    left, right, on=["Country Name", "year"], how="left"
                ),
                transformed_dataset,
            ).sort_values(by=["Country Name", "year"])

        else:
            merged_data = transformed_dataset[0].sort_values(
                by=["Country Name", "year"]
            )

        self.write("unctadstat_report", merged_data)
        return merged_data