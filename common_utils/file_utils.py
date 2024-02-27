import csv
import os
import shutil

import pandas as pd


class FileUtils:
    """ 文件工具类，提供基础的文件操作功能。 """

    def __init__(self, path):
        """
        初始化FileUtils对象。

        :param path: 需要操作的文件路径。
        """
        self.path = path

    def read_file(self, encoding='utf-8'):
        """
        读取文件的内容并返回。

        :param encoding: 文件编码，默认为'utf-8'。
        :return: 文件的内容。
        :raises FileNotFoundError: 如果文件不存在。
        """
        with open(self.path, 'r', encoding=encoding) as f:
            return f.read()

    def read_file_line_by_line(self, encoding='utf-8'):
        """
        使用生成器逐行读取文件内容，适用于读取大文件以节约内存。

        :param encoding: 文件编码，默认为'utf-8'。
        :yield: 文件的下一行。
        :raises FileNotFoundError: 如果文件不存在。
        """
        with open(self.path, 'r', encoding=encoding) as f:
            for line in f:
                yield line.rstrip('\n')  # 使用 rstrip 删除行尾的换行符

    def write_file(self, content, encoding='utf-8'):
        """
        将内容写入文件，如果文件存在，则覆盖。

        :param content: 要写入的内容。
        :param encoding: 文件编码，默认为'utf-8'。
        :raises IOError: 写入文件时发生I/O错误。
        """
        with open(self.path, 'w', encoding=encoding) as f:
            f.write(content)

    def append_to_file(self, content, encoding='utf-8'):
        """
        向文件追加内容，如果文件不存在，则创建。

        :param content: 要追加的内容。
        :param encoding: 文件编码，默认为'utf-8'。
        :raises IOError: 写入文件时发生I/O错误。
        """
        with open(self.path, 'a', encoding=encoding) as f:
            f.write(content)

    def copy_file(self, new_path):
        """
        将文件复制到新的路径。

        :param new_path: 目标文件路径。
        :raises FileNotFoundError: 源文件不存在。
        :raises IOError: 复制文件时发生I/O错误。
        """
        if not os.path.isfile(self.path):
            raise FileNotFoundError("源文件不存在")
        shutil.copy2(self.path, new_path)

    def rename_file(self, new_name):
        """
        将文件重命名为同一目录下的新名称。

        :param new_name: 新的文件名。
        :raises FileNotFoundError: 文件不存在。
        :raises OSError: 重命名文件时发生系统错误。
        """
        new_path = os.path.join(os.path.dirname(self.path), new_name)
        os.rename(self.path, new_path)
        self.path = new_path  # 更新文件路径属性

    def delete_file(self):
        """
        删除文件。

        :raises FileNotFoundError: 文件不存在。
        :raises OSError: 删除文件时发生系统错误。
        """
        if os.path.isfile(self.path):
            os.remove(self.path)
        else:
            raise FileNotFoundError("文件不存在")

    def get_file_info(self):
        """
        返回文件的信息。

        :return: 包含文件信息的字典。
        :raises FileNotFoundError: 文件不存在。
        """
        if not os.path.isfile(self.path):
            raise FileNotFoundError("文件不存在")

        file_stat = os.stat(self.path)
        return {
            'name': os.path.basename(self.path),  # 文件名
            'path': os.path.abspath(self.path),  # 文件绝对路径
            'size': file_stat.st_size,  # 文件大小（字节）
            'created_time': file_stat.st_ctime,  # 文件创建时间
            'modified_time': file_stat.st_mtime  # 文件最后修改时间
        }


class CsvFileUtils(object):
    def __init__(self, file_path, delimiter=',', encoding='utf-8'):
        self.file_path = file_path
        self.delimiter = delimiter
        self.encoding = encoding

    def read_csv(self, header=False):
        """
        读取csv文件

        :param header: 如果为True，将跳过第一行
        :return: 数据列表，每个元素是一个代表行的列表
        :raises IOError: 如果文件读取过程出错
        """
        data = []
        with open(self.file_path, 'r', newline='', encoding=self.encoding) as file:
            reader = csv.reader(file, delimiter=self.delimiter)
            if header:
                next(reader, None)  # 跳过头部
            for row in reader:
                data.append(row)
        return data

    def write_csv(self, data, header=None):
        """
        写入csv文件

        :param data: 写入的数据，应为行列表
        :param header: 可选的头部行
        :raises IOError: 如果文件写入过程出错
        """
        with open(self.file_path, 'w', newline='', encoding=self.encoding) as file:
            writer = csv.writer(file, delimiter=self.delimiter)
            if header is not None:
                writer.writerow(header)
            writer.writerows(data)

    def append_to_csv(self, data):
        """
        追加到csv文件

        :param data: 单行数据作为列表
        :raises IOError: 如果文件追加过程出错
        """
        with open(self.file_path, 'a', newline='', encoding=self.encoding) as file:
            writer = csv.writer(file, delimiter=self.delimiter)
            writer.writerow(data)

    def read_csv_generator(self, header=True):
        """
        使用生成器逐行读取csv文件。

        :param header: 如果为True，将返回包含表头的迭代器，否则将返回无表头的迭代器。
        :yield: 文件的下一行作为列表。
        :raises IOError: 如果文件读取过程出错。
        """
        with open(self.file_path, 'r', newline='', encoding=self.encoding) as file:
            reader = csv.reader(file, delimiter=self.delimiter)
            if header:
                yield next(reader)  # 返回表头
            for row in reader:
                yield row

    # 上下文管理器支持 (__enter__ 和 __exit__ 方法)
    def __enter__(self):
        try:
            self.file = open(self.file_path, 'a', newline='', encoding=self.encoding)
            self.writer = csv.writer(self.file, delimiter=self.delimiter)
            return self
        except IOError as e:
            raise e

    def __exit__(self, exc_type, exc_value, traceback):
        if self.file:
            self.file.close()


class ExcelUtils:
    """ 用于处理Excel文件的工具类 """

    def __init__(self, file_path):
        """
        初始化ExcelUtils对象。

        :param file_path: Excel文件的路径。
        """
        self.file_path = file_path
        # 确保文件存在，如果不存在则创建一个新的Excel文件
        if not os.path.exists(self.file_path):
            with pd.ExcelWriter(self.file_path, engine='openpyxl') as writer:
                writer.save()

    def read_sheet(self, sheet_name=0):
        """
        读取Excel文件中指定的sheet。

        :param sheet_name: 要读取的sheet名，如果是integer，0表示第一个sheet，也是默认值。
        :return: 指定sheet的数据，作为pandas的DataFrame对象。
        """
        return pd.read_excel(self.file_path, sheet_name=sheet_name)

    def write_sheet(self, data, sheet_name='Sheet1'):
        """
        将DataFrame写入Excel文件的指定sheet。

        :param data: 要写入的DataFrame数据。
        :param sheet_name: 要写入的sheet名称，默认为'Sheet1'。
        """
        # 首先检查sheet是否存在，如果不存在则创建
        sheet_exists = any(sheet_name == name for name in self.get_sheet_names())
        if not sheet_exists:
            # 创建新sheet并写入数据
            data.to_excel(self.file_path, sheet_name=sheet_name, index=False)
        else:
            # 如果sheet已存在，使用append模式
            with pd.ExcelWriter(self.file_path, engine='openpyxl', mode='a', if_sheet_exists='append') as writer:
                data.to_excel(writer, sheet_name=sheet_name, index=False)

    def append_data(self, data, sheet_name='Sheet1'):
        """
        向Excel文件的指定sheet追加数据。

        :param data: 要追加的DataFrame数据。
        :param sheet_name: 要追加数据的sheet名称，默认为'Sheet1'。
        """
        # 检查sheet是否存在，如果不存在则创建
        sheet_exists = any(sheet_name == name for name in self.get_sheet_names())
        if not sheet_exists:
            # 创建新sheet并写入数据
            data.to_excel(self.file_path, sheet_name=sheet_name, index=False)
        else:
            # 如果sheet已存在，追加数据
            with pd.ExcelWriter(self.file_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                # 读取现有sheet的数据
                orig_data = pd.read_excel(self.file_path, sheet_name=sheet_name)
                # 将新数据与原有数据合并
                updated_data = pd.concat([orig_data, data], ignore_index=True)
                # 将合并后的数据写回Excel
                updated_data.to_excel(writer, sheet_name=sheet_name, index=False)

    def get_sheet_names(self):
        """
        获取Excel文件中所有sheet的名称。

        :return: 包含所有sheet名称的列表。
        """
        with pd.ExcelFile(self.file_path) as xls:
            return xls.sheet_names

    def read_sheet_by_chunk(self, sheet_name=0, chunk_size=1000):
        """
        使用生成器逐块读取Excel文件中指定的sheet。

        :param sheet_name: 要读取的sheet名，可以是一个索引，也可以是一个字符串，默认为0，表示第一个sheet。
        :param chunk_size: 每一块的行数。
        :yield: 生成器，每次返回一个包含数据块的DataFrame。
        """
        with pd.ExcelFile(self.file_path) as xls:
            iterator = pd.read_excel(xls, sheet_name=sheet_name, chunksize=chunk_size)
            for chunk in iterator:
                yield chunk
