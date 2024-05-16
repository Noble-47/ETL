import aiofiles
import pathlib
import logging
import asyncio
import aiohttp
import abc
import os

class Link:

    def __init__(self, url, name, headers, is_json_content=False, encoding="utf-8"):
        self.url = url
        self.name = name
        self._headers = headers
        self.encoding = encoding
        self.is_json_content = is_json_content

    @property
    def headers(self):
        return self._headers  # or default request headers


    def __repr__(self):
        return f"Link(name = {self.name}, url = {self.url})"


class Download:

    def __init__(self, name, content):
        self.name = name
        self.content = content
    
    def __repr__(self, write=True):
        return f"Download(name = {self.name})"


class BaseExtractor(abc.ABC):

    name = ""
    domain = ""

    def __init__(self, directory=None):
        self.name = self.__class__.name or self.__class__.__name__
        self.logger = logging.getLogger(f"ETL.Extractor.{self.name}")
        self.directory = directory
    
    @abc.abstractmethod
    def get_links(self):
        pass

    @abc.abstractmethod
    def parse(self, content):
        pass

    async def handle_request(self, session, link):
        encoding = link.encoding
        headers = link.headers
        self.logger.info(f"Sending Request: {link.url}")
        async with session.get(link.url, headers=headers) as resp:
            if not resp.ok:
                self.logger.error(
                    f"Request Error -  Received {resp.status} : {link.url}"
                )
                return None

            self.logger.info(f"Response Received ({resp.status}) - {link.name} from {link.url}")
            if link.is_json_content:
                content = await resp.content.json()
            else:
                content = await resp.content.read()
            self.logger.info(f"Decoding Download - {link.name}, format : {encoding}")
            parsed_content = self.parse(content.decode(encoding))
            self.logger.info(f"Decoding Complete - {link.name}, format : {encoding}")
            return Download(content=parsed_content, name=link.name)

    async def start_request(self):
        download_tasks = set()
        async with aiohttp.ClientSession() as session:
            async with asyncio.TaskGroup() as tg:
                for link in self.get_links():
                    self.logger.info(
                        f"Schedulling Request : {link.name} from {link.url}"
                    )
                    task = tg.create_task(self.handle_request(session, link))
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
            task.result() for task in asyncio.run(self.start_request())
        ]

        if write:
            # check if directory is given
            if not self.directory:
                self.logger.debug(
                    f"Missing Directory: No directory was specified, using data/{self.name}"
                )
                self.directory = pathlib.Path("data")

            # check if directory exists
            if not self.directory.is_dir():
                self.directory.mkdir()
                self.logger.info(f"Created Directory: {self.directory}")

            # check if program have write permission
            if not os.access(self.directory, mode=os.W_OK):
                self.logger.critical(
                    f"Permission Error: Do not have permission to write to {self.directory}"
                )
                raise PermissionError(f"Cannot read directory: {self.directory}")

            asyncio.run(self.write(self.directory))
 
    def __repr__(self):
        return f"{self.__class__.__name__}<{self.directory or self.domain}>"

    
