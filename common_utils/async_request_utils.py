import asyncio
import json
import httpx
import aiohttp
import requests


async def trans_request_params_bytes_to_str(params):
    assert isinstance(params, dict)
    for key, data in params.items():
        if isinstance(data, bytes):
            params[key] = str(data, encoding='utf-8')
        if isinstance(data, dict):
            await trans_request_params_bytes_to_str(data)
        if isinstance(data, list):
            for idx, val in enumerate(data):
                if isinstance(val, bytes):
                    data[idx] = str(val, encoding='utf-8')
    return params


class AsyncAiohttp:

    async def request(self, method, url, headers=None, data=None, proxy_url=None, proxy_auth=None, **kwargs):
        for attempt in range(4):  # 1 initial attempt + 3 retries
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method=method, url=url, headers=headers, data=data, proxy=proxy_url,
                                               **kwargs) as response:
                        bytes_data = await response.read()
                        return response.status, bytes_data.decode()
            except Exception as e:
                if attempt < 3:
                    await asyncio.sleep(3)
                else:
                    raise

    async def get(self, url, headers=None, proxy_url=None, proxy_auth=None, **kwargs):
        return await self.request(method="GET", url=url, headers=headers, proxy_url=proxy_url, **kwargs)

    async def post(self, url, headers=None, data=None, proxy_url=None, proxy_auth=None, **kwargs):
        return await self.request(method="POST", url=url, headers=headers, data=data, proxy_url=proxy_url, **kwargs)


async_aiohttp = AsyncAiohttp()


class AsyncHttpx:

    async def request(self, method, url, headers=None, data=None, proxy_url=None, **kwargs):
        async with httpx.AsyncClient(proxies={"http": proxy_url, "https": proxy_url}) as client:
            response = await client.request(method=method, url=url, headers=headers, data=data, **kwargs)
            return response.status_code, response.text

    async def get(self, url, headers=None, proxy_url=None, **kwargs):
        async with httpx.AsyncClient(proxies={"http://": proxy_url, "https://": proxy_url}, verify=False,
                                     timeout=30) as client:
            response = await client.get(url=url, headers=headers, **kwargs)
            return response.status_code, response.text

    async def post(self, url, headers=None, data=None, proxy_url=None, **kwargs):
        async with httpx.AsyncClient(proxies={"http": proxy_url, "https": proxy_url}) as client:
            response = await client.post(url=url, headers=headers, data=data, **kwargs)
            return response.status_code, response.text


async_httpx = AsyncHttpx()


class AsyncRequests:
    async def request(self, method, url, headers=None, data=None, proxy_url=None, **kwargs):
        for attempt in range(4):  # 1 initial attempt + 3 retries
            try:
                async with requests.Session() as session:
                    async with session.request(method=method, url=url, headers=headers, data=data,
                                               proxy=proxy_url, **kwargs) as response:
                        return response.status, await response.text()
            except requests.HTTPError as e:
                if attempt < 3:  # if we are not on the last attempt
                    await asyncio.sleep(3)
                else:
                    raise  # raise the last exception

    async def get(self, url, headers=None, proxy_url=None, **kwargs):
        return await self.request("GET", url, headers=headers, proxy_url=proxy_url, **kwargs)

    async def post(self, url, headers=None, data=None, proxy_url=None, **kwargs):
        return await self.request("POST", url, headers=headers, data=data, proxy_url=proxy_url, **kwargs)


async_requests = AsyncRequests()
