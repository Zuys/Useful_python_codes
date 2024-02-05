import pandas as pd
import requests
from lxml import html
from lxml import etree
from concurrent.futures import ThreadPoolExecutor

# 处理表格，并分开保存为csv


# 处理四个标签的列表

# 定义标签列表
tags = ["听过", "看过", "玩过", "读过"]

# 循环处理每个标签的表格
for tag in tags:
    # 读取Excel文件
    file_name = "mark.xlsx"
    xlsx = pd.ExcelFile(file_name)

    # 尝试读取标签对应的表格
    try:
        df = pd.read_excel(xlsx, tag)
    except KeyError:
        print(f"工作表{tag}不存在，请检查表格名称是否正确。")
        continue

    # 检查'豆瓣评分'列是否存在
    if "豆瓣评分" in df.columns:
        # 将'豆瓣评分'列的内容乘以2
        df["豆瓣评分"] *= 2
    elif "评分" in df.columns:
        # 将'评分'列的内容乘以2
        df["评分"] *= 2
    else:
        print(f"列'豆瓣评分'在{tag}工作表中不存在，请检查列名称是否正确。")
        continue

    # 将修改后的数据保存为CSV文件
    output_file_name = f"zout1_{tag}.csv"
    with open(output_file_name, "w", newline="") as csvfile:
        df.to_csv(csvfile, index=False)

    print(f"修改后的数据已保存到'{output_file_name}'")


# 处理读过的书
# 处理 读过 csv

# 读取原始的CSV文件
file_name = "zout1_读过.csv"
df = pd.read_csv(file_name)


# 拆分“简介”列的函数
def split_intro(intro):
    # 使用 / 分隔简介字符串，并确保至少有三个部分（用None填充缺失的部分）
    parts = intro.split(" / ")
    return parts + [None] * (3 - len(parts)) if len(parts) < 3 else parts[:3]


# 应用拆分函数到“简介”列，并创建新列
df[["作者", "出版日期", "出版社"]] = pd.DataFrame(
    df["简介"].apply(split_intro).tolist(), index=df.index
)

# 删除原始的“简介”列
df.drop(columns=["简介"], inplace=True)

# 将修改后的数据保存为新的CSV文件
output_file_name = "zout2_读过.csv"
df.to_csv(
    output_file_name, index=False, encoding="utf-8-sig"
)  # 使用utf-8-sig编码确保中文正常显示

print(f"数据已保存到'{output_file_name}'")

# 获取封面， 获取最终csv文件

# 读取CSV文件
file_name = "zout2_读过.csv"
df = pd.read_csv(file_name)

# 定义一个常见的浏览器User-Agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"


# 函数：从网页获取HTML并提取封面链接
def get_cover_link_from_html(url):
    headers = {"User-Agent": USER_AGENT}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tree = etree.HTML(response.text)
        cover_element = tree.xpath('//*[@id="item-cover"]/img')
        if cover_element:
            print("获取封面地址： ", cover_element[0].get("src"))
            return cover_element[0].get("src")
        else:
            return None
    except requests.RequestException:
        return None


# 使用多线程处理数据
def process_urls_multithreaded(urls):
    with ThreadPoolExecutor(max_workers=10) as executor:  # 设置线程池大小
        futures = [executor.submit(get_cover_link_from_html, url) for url in urls]
        cover_links = [future.result() for future in futures]  # 获取所有线程的结果
    return cover_links


# 遍历NeoDB链接列，获取封面链接并保存到DataFrame中
df["封面"] = process_urls_multithreaded(df["NeoDB链接"])

# 将更新后的DataFrame输出到新的CSV文件
output_file_name = "zout_final_读过.csv"
df.to_csv(output_file_name, index=False, encoding="utf-8-sig")

print(f"更新后的数据已保存到'{output_file_name}'文件中。")


# 处理 玩过的游戏 csv

# 读取原始的CSV文件
file_name = "zout1_玩过.csv"
df = pd.read_csv(file_name)


# 拆分“简介”列的函数
def split_intro(intro):
    # 使用 / 分隔简介字符串，并确保至少有三个部分（用None填充缺失的部分）
    parts = intro.split(" / ")
    return parts + [None] * (3 - len(parts)) if len(parts) < 3 else parts[:3]


# 应用拆分函数到“简介”列，并创建新列
df[["类型", "平台", "发行时间"]] = pd.DataFrame(
    df["简介"].apply(split_intro).tolist(), index=df.index
)

# 删除原始的“简介”列
df.drop(columns=["简介"], inplace=True)

# 将修改后的数据保存为新的CSV文件
output_file_name = "zout2_玩过.csv"
df.to_csv(
    output_file_name, index=False, encoding="utf-8-sig"
)  # 使用utf-8-sig编码确保中文正常显示

print(f"数据已保存到'{output_file_name}'")

# 获取封面， 获取最终csv文件

# 读取CSV文件
file_name = "zout2_玩过.csv"
df = pd.read_csv(file_name)

# 定义一个常见的浏览器User-Agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"


# 函数：从网页获取HTML并提取封面链接
def get_cover_link_from_html(url):
    headers = {"User-Agent": USER_AGENT}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tree = etree.HTML(response.text)
        cover_element = tree.xpath('//*[@id="item-cover"]/img')
        if cover_element:
            print("获取封面地址： ", cover_element[0].get("src"))
            return cover_element[0].get("src")
        else:
            return None
    except requests.RequestException:
        return None


# 使用多线程处理数据
def process_urls_multithreaded(urls):
    with ThreadPoolExecutor(max_workers=10) as executor:  # 设置线程池大小
        futures = [executor.submit(get_cover_link_from_html, url) for url in urls]
        cover_links = [future.result() for future in futures]  # 获取所有线程的结果
    return cover_links


# 遍历NeoDB链接列，获取封面链接并保存到DataFrame中
df["封面"] = process_urls_multithreaded(df["NeoDB链接"])

# 将更新后的DataFrame输出到新的CSV文件
output_file_name = "zout_final_玩过.csv"
df.to_csv(output_file_name, index=False, encoding="utf-8-sig")

print(f"更新后的数据已保存到'{output_file_name}'文件中。")


# 处理 看过的电影 csv

# 读取原始的CSV文件
file_name = "zout1_看过.csv"
df = pd.read_csv(file_name)


# 拆分“简介”列的函数
def split_intro(intro):
    # 使用 / 分隔简介字符串，并确保至少有三个部分（用None填充缺失的部分）
    parts = intro.split(" / ")
    return parts + [None] * (5 - len(parts)) if len(parts) < 5 else parts[:5]


# 应用拆分函数到“简介”列，并创建新列
df[["年代", "纸片国家\/地区", "类型", "导演", "演员"]] = pd.DataFrame(
    df["简介"].apply(split_intro).tolist(), index=df.index
)

# 删除原始的“简介”列
df.drop(columns=["简介"], inplace=True)

# 将修改后的数据保存为新的CSV文件
output_file_name = "zout2_看过.csv"
df.to_csv(
    output_file_name, index=False, encoding="utf-8-sig"
)  # 使用utf-8-sig编码确保中文正常显示

print(f"数据已保存到'{output_file_name}'")

# 获取封面， 获取最终csv文件

# 读取CSV文件
file_name = "zout2_看过.csv"
df = pd.read_csv(file_name)

# 定义一个常见的浏览器User-Agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"


# 函数：从网页获取HTML并提取封面链接
def get_cover_link_from_html(url):
    headers = {"User-Agent": USER_AGENT}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tree = etree.HTML(response.text)
        cover_element = tree.xpath('//*[@id="item-cover"]/img')
        if cover_element:
            print("获取封面地址： ", cover_element[0].get("src"))
            return cover_element[0].get("src")
        else:
            return None
    except requests.RequestException:
        return None


# 使用多线程处理数据
def process_urls_multithreaded(urls):
    with ThreadPoolExecutor(max_workers=10) as executor:  # 设置线程池大小
        futures = [executor.submit(get_cover_link_from_html, url) for url in urls]
        cover_links = [future.result() for future in futures]  # 获取所有线程的结果
    return cover_links


# 遍历NeoDB链接列，获取封面链接并保存到DataFrame中
df["封面"] = process_urls_multithreaded(df["NeoDB链接"])

# 将更新后的DataFrame输出到新的CSV文件
output_file_name = "zout_final_看过.csv"
df.to_csv(output_file_name, index=False, encoding="utf-8-sig")

print(f"更新后的数据已保存到'{output_file_name}'文件中。")
