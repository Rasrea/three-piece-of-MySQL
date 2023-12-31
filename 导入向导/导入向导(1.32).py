from sqlalchemy import create_engine
import pandas as pd
import pymysql

# 连接数据文件
while True:
    file_path = input('请输入文件地址如d:/desktop/data.xlsx ：')
    try:
        if file_path.endswith(('.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt', '.csv')):
            if file_path.endswith('.csv'):
                # 读取 CSV 文件到 DataFrame
                df = pd.read_csv(file_path)
                if 'Unnamed' in df.columns.values[0]:  # 排除不符合关系型数据要求的文件
                    print('!!!文件内容不符合关系型数据,请修改后重试\n')
                    continue

                else:
                    break

            else:
                # 读取 excel 文件到 DataFrame
                df = pd.read_excel(file_path)
                if 'Unnamed' in df.columns.values[0]:  # 排除不符合关系型数据要求的文件
                    print('!!!文件内容不符合关系型数据,请修改后重试\n')
                    continue

                else:
                    break

        else:  # 补充except
            print('!!!文件不存在或不要带引号，请重试\n')
    except FileNotFoundError:
        print('!!!文件不存在或不要带引号，请重试\n')
    except OSError:
        print('!!!文件不存在或不要带引号，请重试\n')

# 获取列名
column_names = df.columns

# 输出每个列名，包括两个索引
print('\n列出表的列名及索引:')
for i, column in enumerate(column_names):
    if i+1 == len(column_names):  # 0被用于返，所以倒数时不要给最后一个配0索引
        print("{}. {}".format(i + 1, column))
        break

    # 配两个索引，用于应对index<0的情况
    print("{} or {}. {}".format(i + 1, -len(column_names)+i+1, column))

# 使用drop方法删除指定列
print('\n进入删除阶段：\n')
while True:
    try:
        index = eval(input('输入要删除的列索引（输入0终止）：'))
        if index == 0:
            break

        print('({})删除完成'.format(column_names[index - 1]))
        df = df.drop(columns=column_names[index - 1])
    except KeyError:  # 返回重复输入
        print('!!!不要重复删除，请重试\n')
    except IndexError:  # 返回越界错误
        print(f'!!!范围:1——{len(column_names)} or {-len(column_names) + 1}——-1, 请重试\n')
    except NameError:  # 返回错误索引
        print('!!!索引不符合要求，请重写\n')
    except TypeError:  # 补充NameError
        print('!!!索引不符合要求，请重写\n')
    except SyntaxError:  # 补充NameError
        print('!!!索引不符合要求，请重写\n')

# 检查删除后的列
print('\n删除后的列名:')
for i, column in enumerate(df.columns):
    if i+1 == len(df.columns):  # 0被用于返，所以倒数时不要给最后一个配0索引
        print("{}. {}".format(i + 1, column))
        break

    # 配两个索引，用于应对index<0的情况
    print("{} or {}. {}".format(i + 1, -len(df.columns)+i+1, column))

# 输出每个列名,并更改列名
print('\n进入更改阶段：')
print('!!!注意：列名的首位只能是字母或下划线，否则程序会崩溃\n')
while True:
    try:
        index = int(input('输入要更改的列索引（输入0终止）：'))
        if index == 0:
            break

        new_column_name = input(f"请输入新的列名 for {df.columns[index - 1]}: ")
        df.rename(columns={df.columns[index - 1]: new_column_name}, inplace=True)
        print('更改完成\n')
    except IndexError:  # 返回越界错误
        print(f'!!!范围:1——{len(column_names)} or {-len(column_names) + 1}——-1, 请重试\n')
    except ValueError:  # 返回错误索引
        print('!!!索引不符合要求，请重写\n')
    except TypeError:  # 补充ValueError
        print('!!!索引不符合要求，请重写\n')
    except SyntaxError:  # 补充ValueError
        print('!!!索引不符合要求，请重写\n')

# 检查更改后的列名
print('\n更改后的列名:')
for i, column in enumerate(df.columns):
    if i+1 == len(df.columns):  # 0被用于返，所以倒数时不要给最后一个配0索引
        print("{}. {}".format(i + 1, column))
        break

    # 配两个索引，用于应对index<0的情况
    print("{} or {}. {}".format(i + 1, -len(df.columns)+i+1, column))

# MySQL连接信息
print('\n进入MySQL连接阶段：\n')
print('端口默认为3306')
while True:
    try:
        db_password = input('请输入密码：')
        db_username = input('请输入用户名：')
        db_host = input('请输入IP(如localhost)：')
        db_name = input('请输入数据库名：')
        name = input('自定义表的名字：')
        db_port = '3306'

        # 连接到 MySQL 数据库
        connection = pymysql.connect(host=db_host, user=db_username, password=db_password, database=db_name)
    except pymysql.err.OperationalError:
        print('!!!IP地址或数据库名输入错误，请重试\n')
    except RuntimeError:
        print('!!!密码或用户名输入错误，请重试\n')
    else:
        print('连接成功')
        break

# 导入数据
# 创建 MySQL 连接引擎
engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")
# 将 DataFrame 写入 MySQL 数据库中的表
df.to_sql(name, con=engine, if_exists='replace', index=False)

# 创建一个游标对象
cursor = connection.cursor()
table_name = name

# 获取表的列信息
cursor.execute(f"DESCRIBE {table_name}")
columns = cursor.fetchall()

# 数据类型转换
print('\n进入数据类型转换阶段：\n')
for i, column in enumerate(columns):
    if i+1 == len(columns):  # 0被用于返，所以倒数时不要给最后一个配0索引
        print(f"{i + 1}.{column[0]} (当前类型: {column[1]})")
        break

    # 配两个索引，用于应对index<0的情况
    print(f"{i + 1} or {-len(columns)+i+1}. {column[0]}(当前类型: {column[1]})" )

while True:
    try:
        type_index = eval(input('输入要修改的列索引(输入0返回):')) - 1
        if type_index == - 1:
            break

        new_data_type = input(
            f"请输入新的数据类型 for {columns[type_index][0]} (当前类型: {columns[type_index][1]}): ")
        # 构建 ALTER TABLE 语句
        alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {columns[type_index][0]} {new_data_type}"
        # 执行 ALTER TABLE 语句
        cursor.execute(alter_query)
    except IndexError:  # 返回越界错误
        print(f'!!!范围:1——{len(columns)} or {-len(columns) + 1}——-1, 请重试\n')
    except NameError:  # 返回错误索引
        print('!!!索引不符合要求，请重写\n')
    except TypeError:  # 补充NameError
        print('!!!索引不符合要求，请重写\n')
    except SyntaxError:  # 补充NameError
        print('!!!索引不符合要求，请重写\n')
    except pymysql.err.ProgrammingError:
        print('!!!非法数据类型，请重写\n')
    except pymysql.err.DataError:  # 补充pymysql.err.ProgrammingError
        print('!!!非法数据类型，请重写\n')
    else:
        print('修改成功\n')
# 提交修改
connection.commit()

# 自定义主键
while True:
    try:
        primary_index = eval(input("请输入作为主键的列索引(0：不定义主键): ")) - 1
        if primary_index == -1:
            break

        alter_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({columns[primary_index][0]})"
        cursor.execute(alter_query)
    except IndexError:  # 返回越界错误
        print(f'!!!范围:1——{len(columns)} or {-len(columns) + 1}——-1, 请重试\n')
    except NameError:  # 返回错误索引
        print('!!!索引不符合要求，请重写\n')
    except TypeError:  # 补充NameError
        print('!!!索引不符合要求，请重写\n')
    except SyntaxError:  # 补充NameError
        print('!!!索引不符合要求，请重写\n')
    except pymysql.err.OperationalError:  # 数据类型不符合做主键的条件
        print('!!!数据类型不符合做主键的条件，请重试\n')
    except pymysql.err.IntegrityError:  # 补充pymysql.err.OperationalError
        print('!!!数据类型不符合做主键的条件，请重试\n')
    else:
        break
# 提交修改
connection.commit()

# 关闭游标和连接
cursor.close()
connection.close()
print('运行完成')
input("please input any key to exit!")
