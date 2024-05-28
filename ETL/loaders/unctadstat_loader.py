from loaders.generic import GenericMergeLoader


class UnctadStatLoader(GenericMergeLoader):

    name = "UnctadStat Loader"
    default_data_dir = "data/unctadstat/transformed"
    default_save_dir = "data/loaded"
