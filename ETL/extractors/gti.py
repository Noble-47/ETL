from extractors.base import BaseExtractor
from extractors.base import Link

ROOT_URL = f"https://www.visionofhumanity.org/wp-content/uploads"

UPLOAD_YEAR = "2024"
START_YEAR = 2011
END_YEAR = 2023

URL = f"{ROOT_URL}/{UPLOAD_YEAR}/02/"

HEADERS = {
    "Host": "www.visionofhumanity.org",
    #    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Alt-Used": "www.visionofhumanity.org",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


class GTIExtractor(BaseExtractor):

    name = "vision_of_humanity"
    default_save_dir = "data/gti/extracted"

    def __init__(self, upload=None, start=None, end=None, save_dir=None):
        self.upload = upload or UPLOAD_YEAR
        self.start = start or START_YEAR
        self.end = end or END_YEAR
        self._root_url = ROOT_URL
        self._base_url = f"{self.root_url}/{self.upload}/02/"
        super().__init__(save_dir)

    def setup_metric_componenet(self, metric_class):
        super().setup_metric_componenet(metric_class)
        self.metric.add(start_year = self.start)
        self.metric.add(end_year = self.end)
        self.metric.add(upload_year = self.upload)

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

