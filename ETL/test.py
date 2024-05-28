from extractors.unctadstat import UnctadStatExtractor
from extractors.gti import GTIExtractor

from transformers.unctadstat import UnctadStatTransformer
from transformers.gti import GTITransformer

from loaders.unctadstat_loader import UnctadStatLoader
from loaders.generic import GenericMergeLoader
from loaders.gti_loader import GTILoader

from pipeline import Pipeline

pipeline = Pipeline()

pipeline.add(
    # extractors = [
    #     GTIExtractor(**{"start" : 2011, "end" : 2024, "upload" : 2024}),
    #     #GTIExtractor(),
    #     UnctadStatExtractor(**{"variables" : ["US.PCI", "US.TermsOfTrade", 	"US.GDPComponent"]})
    #     ],
    # transformers = [
    #     GTITransformer(),
    #     UnctadStatTransformer()
    # ],
    loaders=[UnctadStatLoader(), GTILoader(), GenericMergeLoader()]
)


print(pipeline.outline())

pipeline.run()
