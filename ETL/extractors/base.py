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

    def __repr__(self):
        return f"Download(name = {self.name})"


class BaseExtractor(abc.ABC):

    name = ""
    domain = ""
    default_data_dir = ""

    def __init__(self, data_dir=None):
        self.name = self.__class__.name or self.__class__.__name__
        self.logger = logging.getLogger(f"ETL.Extractor.{self.name}")

        if not data_dir:
            self.logger.debug(
                f"Missing Directory: No data_dir was specified, using data/{self.name}"
            )
            data_dir = self.default_data_dir

        else:
            data_dir = pathlib.Path(data_dir)

        # check if data_dir exists
        if not data_dir.is_dir():
            data_dir.mkdir()
            self.logger.info(f"Created Directory: {data_dir}")

        self.data_dir = data_dir

        # check if program have write permission
        if not os.access(data_dir, mode=os.W_OK):
            self.logger.critical(
                f"Permission Error: Do not have permission to write to {self.data_dir}"
            )
            # Raise permission error
            raise Exception

    @abc.abstractmethod
    def get_links(self):
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

            self.logger.info(
                f"Response Received ({resp.status}) - {link.name} from {link.url}"
            )
            if link.is_json_content:
                content = await resp.content.json()
            else:
                content = await resp.content.read()
            self.logger.info(f"Decoding Download - {link.name}, format : {encoding}")
            if link.encoding:
                content = content.decode(encoding)
            self.logger.info(f"Decoding Complete - {link.name}, format : {encoding}")
            return Download(content=content, name=link.name)

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

    async def write(self, data_dir):
        async with asyncio.TaskGroup() as tg:
            for download in self.downloads:
                self.logger.info(
                    f"Schedulling Write Operation: {download.name} to folder {self.data_dir}"
                )
                tg.create_task(self.write_download(download))

    async def write_download(self, download):
        path = self.data_dir / download.name
        self.logger.info(f"Initializing Write Operation : {download.name} to {path}")
        async with aiofiles.open(path, "w") as f:
            await f.write(download.content)
            self.logger.info(f"Write Operation Complete : {download.name} to {path}")

    def extract(self):

        self.downloads = [task.result() for task in asyncio.run(self.start_request())]

        asyncio.run(self.write(self.data_dir))

    def __repr__(self):
        return f"{self.__class__.__name__}<{self.data_dir or self.domain}>"
