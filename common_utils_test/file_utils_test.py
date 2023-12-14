import os

import pandas as pd

from common_utils.file_utils import ExcelUtils, CsvFileUtils, FileUtils


def test_file_utils():
    # 假设我们有一个叫做 'example.txt' 的文件，并且这个脚本与文件在同一目录下
    file_path = 'example.txt'

    # 初始化 FileUtils 对象
    file_utils = FileUtils(file_path)

    # 读取整个文件内容
    try:
        content = file_utils.read_file()
        print("文件内容:")
        print(content)
    except FileNotFoundError:
        print("文件不存在。")

    # 逐行读取文件内容并打印
    print("\n文件逐行内容:")
    try:
        for line in file_utils.read_file_line_by_line():
            print(line)
    except FileNotFoundError:
        print("文件不存在。")

    # 写入新内容到文件（覆盖）
    new_content = "这是写入的新内容。"
    file_utils.write_file(new_content)

    # 追加内容到文件
    append_content = "\n这是追加的内容。"
    file_utils.append_to_file(append_content)

    # 复制文件
    new_file_path = 'example_copy.txt'
    file_utils.copy_file(new_file_path)
    print(f"\n文件已复制到: {new_file_path}")

    # 重命名文件
    new_file_name = 'renamed_example.txt'
    file_utils.rename_file(new_file_name)
    print(f"\n文件已重命名为: {new_file_name}")

    # 获取并打印文件信息
    file_info = file_utils.get_file_info()
    print("\n文件信息:")
    for key, value in file_info.items():
        print(f"{key}: {value}")

    # 删除文件
    try:
        file_utils.delete_file()
        print(f"\n文件 {new_file_name} 已被删除")
    except FileNotFoundError as e:
        print(e)

    # 检查文件是否已被删除
    if not os.path.exists(file_path):
        print(f"确认文件 {new_file_name} 已不存在")

def test_csv_file_utils():
    # 假定data.csv文件已经存在

    # 初始化CsvFileUtils对象
    csv_utils = CsvFileUtils('data.csv')

    # 读取CSV文件，假设文件中有头部行
    data_with_header = csv_utils.read_csv(header=True)
    print("Data with Header:")
    print(data_with_header)

    # 读取CSV文件，假设文件中没有头部行
    data_without_header = csv_utils.read_csv(header=False)
    print("Data without Header:")
    print(data_without_header)

    # 写入数据到CSV文件
    # 定义了一个新的数据集和头部
    header = ['Name', 'Age', 'City']
    data_to_write = [
        ['Alice', 24, 'New York'],
        ['Bob', 30, 'Los Angeles'],
        ['Charlie', 28, 'Boston']
    ]
    csv_utils.write_csv(data_to_write, header=header)

    # 向CSV文件追加一行数据
    new_data_row = ['David', 32, 'San Francisco']
    csv_utils.append_to_csv(new_data_row)

    # 读取大文件
    for row in csv_utils.read_csv_generator(header=True):
        print("迭代式读取：", row)

    # 或者，使用上下文管理器安全地追加数据
    with csv_utils as csv_file:
        csv_file.append_to_csv(['Emma', 29, 'Seattle'])

    # 再次读取数据以验证它是否已被写入/追加
    updated_data = csv_utils.read_csv(header=True)
    print("Updated Data:")
    print(updated_data)


def test_excel_utils():
    # your Excel file path here
    file_path = 'example.xlsx'

    # 创建ExcelUtils实例
    excel = ExcelUtils(file_path)

    # 读取第一个sheet的数据
    df = excel.read_sheet()
    print(df)

    # 写入一个新的sheet
    df_new = pd.DataFrame({
        'Column1': [1, 2, 3],
        'Column2': ['A', 'B', 'C']
    })
    excel.write_sheet(df_new, sheet_name='NewSheet')

    # 向现有sheet追加数据
    df_append = pd.DataFrame({
        'Column1': [4, 5, 6],
        'Column2': ['D', 'E', 'F']
    })
    excel.append_data(df_append, sheet_name='NewSheet')

    for chunk in excel.read_sheet_by_chunk(sheet_name='MySheet', chunk_size=5000):
        print(chunk)

    # 获取Excel文件的所有sheet名称
    sheet_names = excel.get_sheet_names()
    print(sheet_names)
