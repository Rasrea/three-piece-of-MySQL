import pandas as pd
from sqlalchemy import create_engine

# 获取用户输入
print('默认端口3306')
password = input('请输入MySQL密码：')
host = input('请输入IP地址(如localhost)：')
user = input('请输入用户名：')
database = input('请输入数据库名：')

# 建立 SQLAlchemy 引擎
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

# 获取数据库的表
show_tables = f'SHOW TABLES FROM {database}'
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
