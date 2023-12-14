import asyncio
from concurrent.futures import ThreadPoolExecutor

import aiofiles
import pandas as pd


class AsyncCsvFileUtils:
    def __init__(self, file_path, delimiter=',', encoding='utf-8'):
        self.file_path = file_path
        self.delimiter = delimiter
        self.encoding = encoding

    async def read_csv(self, header=False):
        """
        异步读取csv文件

        :param header: 如果为True，将跳过第一行
        :return: 数据列表，每个元素是一个代表行的列表
        """
        data = []
        async with aiofiles.open(self.file_path, 'r', encoding=self.encoding) as file:
            if header:
                # 跳过表头行
                await file.readline()
            async for line in file:
                data.append(line.strip().split(self.delimiter))
        return data

    async def write_csv(self, data, header=None):
        """
        异步写入csv文件

        :param data: 写入的数据，应为行列表
        :param header: 可选的头部行，是字符串的列表
        """
        # 使用 StringIO 来构建 CSV 内容，然后一次性写入文件
        from io import StringIO
        import csv

        sio = StringIO()
        csv_writer = csv.writer(sio, delimiter=self.delimiter)
        if header:
            csv_writer.writerow(header)
        csv_writer.writerows(data)

        # 移动到字符串的开始位置
        sio.seek(0)
        async with aiofiles.open(self.file_path, 'w', encoding=self.encoding) as file:
            await file.write(sio.getvalue())

    async def append_to_csv(self, data):
        """
        异步追加到csv文件

        :param data: 单行数据作为列表
        """
        # 类似 write_csv，使用 StringIO 来构建 CSV 内容
        from io import StringIO
        import csv

        sio = StringIO()
        csv_writer = csv.writer(sio, delimiter=self.delimiter)
        csv_writer.writerow(data)

        # 移动到字符串的开始位置
        sio.seek(0)
        async with aiofiles.open(self.file_path, 'a', encoding=self.encoding) as file:
            await file.write(sio.getvalue())

    async def read_csv_generator(self, header=True):
        """
        异步使用生成器逐行读取csv文件

        :param header: 如果为True，将返回包含表头的第一行。
        :yield: 下一行数据的列表。
        """
        async with aiofiles.open(self.file_path, 'r', newline='', encoding=self.encoding) as file:
            if header:
                yield (await file.readline()).strip().split(self.delimiter)
            async for line in file:
                yield line.strip().split(self.delimiter)


class AsyncExcelUtils:
    """ 用于处理Excel文件的异步工具类 """

    def __init__(self, file_path):
        """ 初始化AsyncExcelUtils对象。 """
        self.file_path = file_path
        self.executor = ThreadPoolExecutor()

    async def run_in_executor(self, func, *args):
        """ 在线程池中异步运行函数。 """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, func, *args)

    async def read_sheet(self, sheet_name=""):
        """ 异步读取指定sheet """
        func = pd.read_excel
        return await self.run_in_executor(func, self.file_path, sheet_name)

    async def write_sheet(self, data, sheet_name='Sheet1'):
        """ 异步写入指定sheet """
        func = data.to_excel
        await self.run_in_executor(func, self.file_path, sheet_name, False)
        return

    async def append_data(self, data, sheet_name='Sheet1'):
        """ 异步追加数据到指定sheet """
        orig_data = await self.read_sheet(sheet_name=sheet_name)
        updated_data = pd.concat([orig_data, data], ignore_index=True)
        await self.write_sheet(updated_data, sheet_name)

    async def get_sheet_names(self):
        """ 异步获取所有sheet的名称 """
        excel_file = await self.run_in_executor(pd.ExcelFile, self.file_path)
        return excel_file.sheet_names

    async def read_sheet_by_chunk(self, sheet_name=0, chunk_size=1000):
        """ 异步生成器逐块读取sheet """
        func = pd.read_excel
        iterator = await self.run_in_executor(func, self.file_path, sheet_name, chunk_size)

        async for chunk in self.yield_chunks(iterator):
            yield chunk

    async def yield_chunks(self, iterator):
        """ 异步输出chunk数据 """
        for chunk in iterator:
            await asyncio.sleep(0)  # 让出控制权
            yield chunk
