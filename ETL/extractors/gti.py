from extractors.base import BaseExtractor
from extractors.base import Link

ROOT_URL = f"https://www.visionofhumanity.org/wp-content/uploads"

UPLOAD_YEAR = "2024"
START_YEAR = 2011
END_YEAR = 2023

URL = f"{ROOT_URL}/{UPLOAD_YEAR}/02/"

HEADERS = {
    "Host": "www.visionofhumanity.org",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Alt-Used": "www.visionofhumanity.org",
    # "Connection": "keep-alive",
    # "Cookie": "_ga_WTLY51X2ST=GS1.1.1714390979.8.0.1714390979.60.0.0; _ga=GA1.1.1431123759.1714019059; _ga_ZDCGKWMJ4M=GS1.2.1714390979.7.0.1714390979.0.0.0; _fbp=fb.1.1714019068816.290261249; _clck=cvlw9l%7C2%7Cflc%7C0%7C1576; PHPSESSID=d77c890b9befbd85bad3670166081f1d; popupX=1; __cf_bm=dpMmlAZTTaN72TMvuQd4NSZCvHlPqpQ7dRjMleOt5IQ-1714390808-1.0.1.1-SElJsVy0dq.oyPMoQuPCkFnDhoCXvluSfNJMp6CqUL.wAua8xD3zrBMDtFyxjO.pR8UeD.g1S9Rgo04rkhF9jA; _gid=GA1.2.1475446649.1714390978; _gat=1; _clsk=autw3p%7C1714390982841%7C1%7C1%7Ct.clarity.ms%2Fcollect",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


class GTISpider(BaseExtractor):

    name = "vision_of_humanity"

    def __init__(self, upload, start, end, directory):
        self.upload = upload
        self.start = start
        self.end = end
        self._root_url = ROOT_URL
        self._base_url = f"{self.root_url}/{self.upload}/02/"
        super().__init__(directory)

    @property
    def root_url(self):
        return self._root_url

    @property
    def base_url(self):
        return self._base_url

    def get_links(self):
        for year in range(self.start, self.end + 1):
            yield Link(
                url=self.base_url + f"GTI_{year}_{self.upload[-2:]}.csv",
                name=f"GTI_{year}.csv",
                headers=HEADERS,
            )


    def parse(self, content):
        return content

import pathlib
data = pathlib.Path("data")
gti = GTISpider(upload=UPLOAD_YEAR, start=START_YEAR, end=END_YEAR, directory=data)
# gti.run()
