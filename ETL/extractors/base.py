#from collections import namedtuple
import aiofiles
import logging
import asyncio
import aiohttp


class Link:

    def __init__(self, url, name, headers, encoding="utf-8"):
        self.url = url
        self.name = name
        self._headers = headers
        self.encoding = encoding

    @property
    def headers(self):
        return self._headers  # or default request headers

    def __repr__(self):
        return f"Link(name = {self.name}, url = {self.url})"


class Download:

    def __init__(self, name, content):
        self.name = name
        self.content = content

    def __repr__(self):
        return f"Download(name = {self.name})"


class BaseExtractorClass:

    name = ""

    def __init__(self, directory=None):
        self.name = self.__class__.name or self.__class__.__name__
        self.logger = logging.getLogger(f"ETL.Extractor.{self.name}")
        self.directory = directory

    async def download_file(self, session, link):
       # Download = namedtuple("Download", "content name")
        encoding = link.encoding
        headers = link.headers
        async with session.get(link.url, headers=headers) as resp:
            self.logger.info(f"Initializing Download : {link.name} from {link.url}")
            if not resp.ok:
                self.logger.error(
                    f"Download Error -  Received {resp.status} : {link.name} from {link.url}"
                )
                return None

            content = await resp.content.read()
            self.logger.info(f"Download Complete - {link.name} from {link.url}")
            self.logger.info(f"Decoding Download - {link.name}, format : {encoding}")
            content = content.decode(encoding)
            self.logger.info(f"Decoding Complete - {link.name}, format : {encoding}")
            return Download(content=content, name=link.name)

    async def initialize_download(self):
        download_tasks = set()
        async with aiohttp.ClientSession() as session:
            async with asyncio.TaskGroup() as tg:
                for link in self.make_requests():
                    self.logger.info(
                        f"Schedulling Download : {link.name} from {link.url}"
                    )
                    task = tg.create_task(self.download_file(session, link))
                    download_tasks.add(task)
        return download_tasks

    async def write(self, directory):
        async with asyncio.TaskGroup() as tg:
            for download in self.downloads:
                self.logger.info(
                    f"Schedulling Write Operation: {download.name} to folder {self.directory}"
                )
                tg.create_task(self.write_download(download))

    async def write_download(self, download):
        path = f"{self.directory}/{download.name}"
        self.logger.info(f"Initializing Write Operation : {download.name} to {path}")
        async with aiofiles.open(path, "w") as f:
            await f.write(download.content)
            self.logger.info(f"Write Operation Complete : {download.name} to {path}")

    def run(self, write=True):

        self.downloads = [
            task.result() for task in asyncio.run(self.initialize_download())
        ]

        if write:
            # check if directory is given
            if not self.directory:
                self.logger.debug(
                    f"Missing Directory: No directory was specified, using data/{self.name}"
                )
                self.directory = f"data/"

            # check if directory exists
            # if not debug
            # check if program have write permission
            # if not error
            asyncio.run(self.write(self.directory))
