from sqlalchemy import create_engine
import pandas as pd
import pymysql

# MySQL连接信息
print('端口默认为3306')
db_password = input('请输入密码：')
db_username = input('请输入用户名：')
db_host = input('请输入IP(如localhost)：')
db_name = input('请输入数据库名：')
name = input('自定义表的名字：')
db_port = '3306'

# 连接数据文件
i = 1 #为后续更新提供空间
while True:
    if i == 1:
        file_path = input('请输入地址如d:/desktop/data.xlsx ：')
        if file_path.endswith(('.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt', '.csv')):
            # 读取xls文件到DataFrame
            df = pd.read_excel(file_path)
        break
    else:
        i = eval(input('索引错误，请重新输入：'))

# 获取列名
column_names = df.columns

# 输出每个列名,并更改列名
print('\n表中列名：')
for i, column in enumerate(column_names):
    print("{}. {}".format(i + 1, column))
i = eval(input('\n是否有需要删除的列名（是=-1，否=-2）：'))

# 使用drop方法删除指定列
if i == -1:
    print('\n进入删除阶段：\n')
    while True:
        index = int(input('输入要删除的列索引（输入0终止）：'))
        if index == 0:
            break
        elif 1 <= index <= len(column_names):
            print('({})删除完成'.format(column_names[index - 1]))
            df = df.drop(columns=column_names[index - 1])
        else:
            print("无用索引，请重试")
    print('\n删除后的列名:')
    for i, column in enumerate(df):
        print("{}. {}".format(i + 1, column))

# 输出每个列名,并更改列名
print('\n进入更改阶段：\n')
while True:
    index = int(input('输入要更改的列索引（输入0终止）：'))
    if index == 0:
        break
    elif 1 <= index <= len(df):
        new_column_name = input(f"请输入新的列名 for {df.columns[index - 1]}: ")
        df.rename(columns={df.columns[index - 1]: new_column_name}, inplace=True)
        print('更改完成\n')
    else:
        print("无用索引，请重试")

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

# 数据类型转换
print('\n更改后的列名：')
for i, column in enumerate(columns):
    column_name = column[0]
    current_data_type = column[1]
    print(f"{i + 1}.{column_name} (当前类型: {current_data_type})")

index = eval(input('\n是否更改数据类型（0：否， 1：是）：'))
while True:
    if index == 1:
        type_index = eval(input('输入要修改的列索引(输入0返回):')) - 1
        if len(columns) > type_index >= 0:
            new_data_type = input(
                f"请输入新的数据类型 for {columns[type_index][0]} (当前类型: {columns[type_index][1]}): ")
            # 构建 ALTER TABLE 语句
            alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {columns[type_index][0]} {new_data_type}"
            # 执行 ALTER TABLE 语句
            cursor.execute(alter_query)
            print('修改成功\n')
            continue
        if type_index == -1:
            break
        else:
            print('无效索引，请重新输入\n')
            continue
    if index == 0:
        break
    else:
        index = eval(input('无效索引，请重新输入:'))
# 提交修改
connection.commit()

# 添加主键
index = eval(input('是否定义主键（0：否， 1：是）：'))
while True:
    if index == 1:
        primary_index = eval(input("请输入作为主键的列名(序号): ")) - 1
        if primary_index <= len(columns) and primary_index >= 0:
            alter_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({columns[primary_index][0]})"
            cursor.execute(alter_query)
            break
        else:
            primary_index = eval(input('无效索引，请重新输入:'))
    if index == 0:
        break
    else:
        index = eval(input('无效索引，请重新输入:'))
# 提交修改
connection.commit()

# 关闭游标和连接
cursor.close()
connection.close()
print('运行完成')
input("please input any key to exit!")
