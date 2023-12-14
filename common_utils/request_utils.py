import time

import httpx
import requests


class CommonRequests:
    def request(self, method, url, headers=None, data=None, proxy_url=None, **kwargs):
        for attempt in range(4):  # 1 initial attempt + 3 retries
            try:
                with requests.Session() as session:
                    with session.request(method=method, url=url, headers=headers, data=data,
                                         proxy={"http": proxy_url, "https": proxy_url},
                                         **kwargs) as response:
                        return response.status_code, response.text
            except requests.HTTPError as e:
                if attempt < 3:  # if we are not on the last attempt
                    time.sleep(3)
                else:
                    raise  # raise the last exception

    def get(self, url, headers=None, proxy_url=None, **kwargs):
        return self.request("GET", url, headers=headers, proxy_url=proxy_url, **kwargs)

    def post(self, url, headers=None, data=None, proxy_url=None, **kwargs):
        return self.request("POST", url, headers=headers, data=data, proxy_url=proxy_url, **kwargs)


common_requests = Requests()


class CommonHttpx:

    def request(self, method, url, headers=None, data=None, proxy_url=None, **kwargs):
        with httpx.Client(proxies={"http": proxy_url, "https": proxy_url}) as client:
            response = client.request(method=method, url=url, headers=headers, data=data, **kwargs)
            return response.status_code, response.text

    def get(self, url, headers=None, proxy_url=None, **kwargs):
        return self.request(method="GET", url=url, headers=headers, proxy_url=proxy_url, **kwargs)

    def post(self, url, headers=None, data=None, proxy_url=None, **kwargs):
        return self.request(method="POST", url=url, headers=headers, data=data, proxy_url=proxy_url, **kwargs)


common_httpx = Httpx()
