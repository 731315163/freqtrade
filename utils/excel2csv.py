from pathlib import Path

import pandas as pd

from . import pathutil


def convert_excel_to_csv(excel_folder, csv_folder):
    """
    递归地将指定文件夹中的所有Excel文件转换为CSV文件，并确保CSV文件名唯一。

    :param excel_folder: 包含Excel文件的文件夹路径（Path对象或字符串）
    :param csv_folder: 存储CSV文件的文件夹路径（Path对象或字符串）
    """
    # 将输入转换为Path对象
    excel_folder = Path(excel_folder)
    csv_folder = Path(csv_folder)

    # 确保csv文件夹存在
    csv_folder.mkdir(parents=True, exist_ok=True)

    # 遍历excel_folder中的所有文件和子文件夹
    for item in excel_folder.rglob("*.xlsx"):
        # 读取Excel文件
        df = pd.read_excel(item, engine="openpyxl")

        # 构造CSV文件的唯一路径和名称
        # 使用item.parent.stem来获取父文件夹的最后一个组成部分（即名称）
        # 将其与文件名和.csv扩展名组合起来
        csv_path = csv_folder / item.parent.stem / f"{item.stem}.csv"
        pathutil.create_dir(csv_path)
        # 将DataFrame保存到CSV文件
        df.to_csv(csv_path, index=False, encoding="utf_8_sig")
        print(f"已将 {item} 转换为 {csv_path}")
