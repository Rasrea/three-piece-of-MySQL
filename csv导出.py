import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

# 获取用户输入
print('默认端口3306')
password = input('请输入MySQL密码：')
host = input('请输入IP地址(如localhost)：')
user = input('请输入用户名：')
database = input('请输入数据库名：')
# 建立数据库连接
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
)
# 转换 MySQL 连接为 SQLAlchemy 可连接对象
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# 获取数据库的表
show_tables = f'show tables from {database}'
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

# 获取用户输入的保存 CSV 的地址
csv_path = input('保存 CSV 地址(如d:\desktop\mmm)：') + str('.csv')
# 将 DataFrame 保存为 CSV 文件，并使用 UTF-8 字符集和避免BOM(Byte Order Mark)
df.to_csv(csv_path, index=False, encoding='utf-8-sig')

# 关闭数据库连接
if connection.is_connected():
    connection.close()
print('运行成功！')
print('注意：数据过长时需调整 自适应列宽')
input("please input any key to exit!")
