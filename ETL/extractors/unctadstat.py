"""
Extractor for https://unctadstat.unctad.org
"""

from extractors.base import BaseExtractor
from extractors.base import Link
from pathlib import Path
import py7zr
import aiofiles

"""
The main idea is that the extractor takes in url(s) as input and downloads the data

- should be able to specify the desired years, countries, regions or so on
- default should be `world` and all the available years
"""
# https://unctadstat-api.unctad.org/bulkdownload/US.PCI/US_PCI

HEADERS = {
    "Host": "unctadstat-api.unctad.org",
    # "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


class UnctadStatExtractor(BaseExtractor):
    name = "unctadstat"
    domain = "https://unctadstat.unctad.org"
    default_save_dir = "data/unctadstat/extracted"

    def __init__(self, variables, save_dir=None):
        self.variables = variables
        super().__init__(save_dir)
        temp_dir = Path(f"{self.save_dir}/uncstat_temp/")
        if not temp_dir.is_dir():
            temp_dir.mkdir()
        self.temp_dir = temp_dir

    def construct_download_link(self, variable_name):
        base_url = "https://unctadstat-api.unctad.org/bulkdownload/"
        url = base_url + f"{variable_name}/" + variable_name.replace(".", "_", 1)
        link = Link(
            url=url,
            name=variable_name.replace(".", "_") + ".csv",
            is_json_content=False,
            headers=HEADERS,
            encoding=None,
        )
        yield link

    def get_links(self):
        for variable_name in self.variables:
            yield from self.construct_download_link(variable_name)

    def parse(self, content):
        return content

    async def write_download(self, download):
        path = self.save_dir
        self.logger.info(f"Initializing Write Operation : {download.name} to {path}")
        temp_path = self.temp_dir / download.name

        async with aiofiles.open(temp_path, "wb") as f:
            await f.write(download.content)
            # move unzipped file to path
            archive = py7zr.SevenZipFile(temp_path, mode="r")
            archive.extractall(path=path)
            archive.close()
            temp_path.unlink()
            self.logger.info(f"Write Operation Complete : {download.name} to {path}")

    def run(self):
        super().run()
        self.temp_dir.rmdir()

