import pandas as pd
from sqlalchemy import create_engine
import pymysql

# 获取用户输入
print('连接MySQL')
print('端口默认为3306')
while True:
    try:
        db_password = input('请输入密码：')
        db_username = input('请输入用户名：')
        db_host = input('请输入IP(如localhost)：')
        db_name = input('请输入数据库名：')
        db_port = '3306'

        # 连接到 MySQL 数据库，定义 全局 连接
        connection = pymysql.connect(host=db_host, user=db_username, password=db_password, database=db_name)
        # 创建 MySQL 连接引擎
        engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")
    except pymysql.err.OperationalError:
        print('!!!IP地址或数据库名输入错误，请重试\n')
    except RuntimeError:
        print('!!!密码或用户名输入错误，请重试\n')
    else:
        print('连接成功')
        break

# 获取数据库的表
show_tables = f'SHOW TABLES FROM {db_name}'
tables = pd.read_sql(show_tables, con=engine)
print(tables)
print('\n支持多表查询\n')

# 获取用户输入的 SELECT 语句,从数据库读取数据到 DataFrame
query = input('输入执行的 SELECT 语句：')
while True:
    if 'select' in query.lower() and 'from' in query.lower():
        df = pd.read_sql(query, con=engine)
        break
    else:
        query = input('输入错误，请重新输入：')
print('\n仅展示部分数据：\n')
print(df.head())

# 获取用户输入的保存的地址
path = input('输入保存地址(如d:\desktop\mmm)：')
print('可导出多个类型，输入0终止：\n')
while True:
    index = eval(input('选择文件保存类型（0：终止,1：csv,2:excel）:'))
    if index == 1:
        csv_path = path + str('.csv')
        # 将 DataFrame 保存为 csv 文件，并使用 UTF-8 字符集和避免BOM(Byte Order Mark)
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print('导出成功，请勿重复输入\n')
    if index == 2:
        excel_path = path + str('.xlsx')
        # 将 DataFrame 保存为 excel 文件
        df.to_excel(excel_path, index=False)
        print('导出成功，请勿重复输入\n')
    if index == 0:
        break

print('\n')

print('运行完成！')
print('注意：数据过长时需调整 自适应列宽')
input("please input any key to exit!")
