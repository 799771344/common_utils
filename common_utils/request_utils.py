import re
import time
from urllib.parse import urlencode

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


common_requests = CommonRequests()


class CommonHttpx:

    def request(self, method, url, headers=None, data=None, proxy_url=None, **kwargs):
        with httpx.Client(proxies={"http": proxy_url, "https": proxy_url}) as client:
            response = client.request(method=method, url=url, headers=headers, data=data, **kwargs)
            return response.status_code, response.text

    def get(self, url, headers=None, proxy_url=None, **kwargs):
        return self.request(method="GET", url=url, headers=headers, proxy_url=proxy_url, **kwargs)

    def post(self, url, headers=None, data=None, proxy_url=None, **kwargs):
        return self.request(method="POST", url=url, headers=headers, data=data, proxy_url=proxy_url, **kwargs)


common_httpx = CommonHttpx()


class CommonUtils:

    def get_url_and_params(self, url):
        """
        将url中的地ip和参数分开
        :param url:
        :return:
        """
        url_list = url.split("?")
        base_url = url_list[0]
        param = url_list[1]
        params = param.split("&")
        query_dic = {}
        for i in params:
            k, v = i.split("=")
            query_dic[k] = v
        return base_url, query_dic

    def convert_curl_to_python(self, curl_command):
        """
        将curl命令转换为python请求。
        :param curl_command:
        :return:
        """
        # 提取URL
        url_match = re.search(r"curl '(.*?)'", curl_command)
        if url_match:
            url = url_match.group(1)
        else:
            raise ValueError("无法提取URL")
        url, params = self.get_url_and_params(url)
        # 提取请求方法
        method_match = re.search(r"-X ([A-Z]+)", curl_command)
        if method_match:
            method = method_match.group(1)
        else:
            method = "GET"  # 默认为GET请求

        # 提取请求头
        headers_match = re.findall(r"-H '(.*?)'", curl_command)
        headers = {}
        for header in headers_match:
            key, value = header.split(": ")
            headers[key] = value

        # 提取请求体数据
        data_match = re.search(r"-d '(.*?)'", curl_command)
        if data_match:
            data = data_match.group(1)
        else:
            data = None

        # 提取代理
        proxy_match = re.search(r"-x\s+'([^']+)'proxy\s+'([^']+)'", curl_command)
        proxies = None
        if proxy_match:
            proxy_url = proxy_match.group(1) or proxy_match.group(2)
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }

        req = "params = {}\nencoded_params = urlencode(params)\nurl = '{}' + '?' + encoded_params\nheaders = {}\ndata = {}\nproxies = {}\nresp = requests.request(methhod='{}', url=url, headers=headers, data=data, proxies=proxies)".format(
            params, url, headers, data, proxies, method)
        return req


if __name__ == '__main__':
    common_utils = CommonUtils()
    curl = "curl 'https://www.qidian.com/ajax/book/category?_csrfToken=ad1df9f0-a2a2-4de0-bfce-3fd0e2ecb9d6&bookId=3690449' \
      -H 'Accept: application/json, text/javascript, */*; q=0.01' \
      -H 'Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6' \
      -H 'Cache-Control: no-cache' \
      -H 'Connection: keep-alive' \
      -H 'Cookie: newstatisticUUID=1700186294_1254531469; fu=768814420; supportwebp=true; e2=; e1=%7B%22l6%22%3A%22%22%2C%22l1%22%3A3%2C%22pid%22%3A%22qd_p_qidian%22%2C%22eid%22%3A%22qd_A18%22%7D; supportWebp=true; _csrfToken=ad1df9f0-a2a2-4de0-bfce-3fd0e2ecb9d6; Hm_lvt_f00f67093ce2f38f215010b699629083=1700186295,1700556185,1700569888,1701053948; _gid=GA1.2.1026188241.1701053949; traffic_utm_referer=; ywguid=855084233966; ywkey=ywnwspZSAAEc; ywopenid=8C49ADA6F7278755417FAA177357BA62; Hm_lpvt_f00f67093ce2f38f215010b699629083=1701054127; _ga=GA1.1.1552057303.1700186295; _ga_FZMMH98S83=GS1.1.1701053948.6.1.1701054696.0.0.0; _ga_PFYW0QLV3P=GS1.1.1701053948.6.1.1701054697.0.0.0' \
      -H 'Pragma: no-cache' \
      -H 'Referer: https://www.qidian.com/book/3690449/' \
      -H 'Sec-Fetch-Dest: empty' \
      -H 'Sec-Fetch-Mode: cors' \
      -H 'Sec-Fetch-Site: same-origin' \
      -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0' \
      -H 'X-Requested-With: XMLHttpRequest' \
      -H 'X-Yuew-sign: 59e8a1d8e57fef2149c4ce1a6c0d51ab' \
      -H 'X-Yuew-time: 1701054698' \
      -H 'baggage: sentry-environment=production,sentry-public_key=791c491729da4642903a67379a813d3a,sentry-trace_id=27e3bd40a44f461a8ab49a95498f6eaa,sentry-sample_rate=0.01,sentry-sampled=false' \
      -H 'sec-ch-ua-mobile: ?0' \
      -H 'sentry-trace: 27e3bd40a44f461a8ab49a95498f6eaa-80ede88f2f797d4c-0' \
      --compressed"
    req = common_utils.convert_curl_to_python(curl)
    print(req)
