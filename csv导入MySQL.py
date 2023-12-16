from sqlalchemy import create_engine
import pandas as pd
import pymysql

# MySQL连接信息
print('端口默认为3306')
db_password = input('请输入密码：')
db_username = input('请输入用户名：')
db_host = input('请输入IP(如localhost)：')
db_name = input('请输入数据库名：')
name = input('表的名字：')
db_port = '3306'
csv_file_path = input('请输入csv地址（如D:/Desktop/data.csv）：')

# 读取 CSV 文件
df = pd.read_csv(csv_file_path)

# 获取列名
column_names = df.columns

# 输出每个列名,并更改列名
print('csv表中列名：')
for i, column in enumerate(column_names):
    print("{}. {}".format(i + 1, column))

while True:
    index = int(input('输入要更改列名的序号（输入0终止）：'))
    if index == 0:
        break
    elif 1 <= index <= len(column_names):
        new_column_name = input(f"请输入新的列名 for {df.columns[index - 1]}: ")
        df.rename(columns={df.columns[index - 1]: new_column_name}, inplace=True)
    else:
        print("Invalid index. Please enter a valid index.")

print('修改后列名：')
for i, column in enumerate(df.columns):
    print("{}. {}".format(i + 1, column))

# 创建 MySQL 连接引擎
engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")

# 将 DataFrame 写入 MySQL 数据库中的表
df.to_sql(name, con=engine, if_exists='replace', index=False)

# 连接到 MySQL 数据库
connection = pymysql.connect(host=db_host, user=db_username, password=db_password, database=db_name)

# 创建一个游标对象
cursor = connection.cursor()
table_name = name

# 获取表的列信息
cursor.execute(f"DESCRIBE {table_name}")
columns = cursor.fetchall()

for column in columns:
    column_name = column[0]
    current_data_type = column[1]
    new_data_type = input(f"请输入新的数据类型 for {column_name} (当前类型: {current_data_type}): ")
    
    # 构建 ALTER TABLE 语句
    alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} {new_data_type}"
    
    # 执行 ALTER TABLE 语句
    cursor.execute(alter_query)

# 提交修改
connection.commit()

# 添加主键
primary_key_column = input("请输入要作为主键的列名: ")
alter_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key_column})"
cursor.execute(alter_query)

# 提交修改
connection.commit()

# 关闭游标和连接
cursor.close()
connection.close()
print('运行完成')
input("please input any key to exit!")
