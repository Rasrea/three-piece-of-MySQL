from sqlalchemy import create_engine
import pandas as pd
import pymysql

# 定义全局变量
global_table = None  # 定义数据表的全局变量
connection = None  # 定义MySQL连接系数的全局变量
cursor = None  # 定义游标的全局变量


# 文件连接阶段
def link_data():
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

    # 打印原数据或更改后的数据
    print_data(df)
    # 自定义删除列函数，并接收修改后数据
    delete_data(df)


# 列删除阶段，使用drop方法删除指定列
def delete_data(df):
    column_names = get_columns(df)  # 从df中获取列名

    print('\n进入删除阶段：\n')
    while True:
        try:
            index = eval(input('输入要删除的列索引（输入0终止）：'))
            if index == 0:
                break

            print('({})删除完成\n'.format(column_names[index - 1]))
            df = df.drop(columns=column_names[index - 1])
        except KeyError:  # 返回重复输入
            print('!!!不要重复删除，请重试\n')
        except IndexError:  # 返回越界错误
            print(f'!!!范围(整数):1——{len(column_names)} or {-len(column_names) + 1}——-1, 请重试\n')
        except NameError:  # 返回错误索引
            print('!!!索引不符合要求，请重写\n')
        except SyntaxError:  # 补充NameError
            print('!!!索引不符合要求，请重写\n')

    # 打印原数据或更改后的数据
    print_data(df)
    # 更改列名函数，并接收修改后的数据
    change_data(df)


# 列名修改阶段，输出需要更改列名,并更改列名
def change_data(df):
    column_names = get_columns(df)  # 从df中获取列名

    print('\n进入更改阶段：')
    print('!!!注意：命名需要符合MySQL的要求，否则数据将无法识别\n')
    while True:
        try:
            index = int(input('输入要更改的列索引（输入0终止）：'))
            if index == 0:
                break

            new_column_name = input(f"请输入新的列名 for {df.columns[index - 1]}: ")
            print(f'更改完成,原({df.columns[index - 1]}) => 新({new_column_name})\n')
            df.rename(columns={df.columns[index - 1]: new_column_name}, inplace=True)
        except IndexError:  # 返回越界错误
            print(f'!!!范围:1——{len(column_names)} or {-len(column_names) + 1}——-1, 请重试\n')
        except ValueError:  # 返回错误索引
            print('!!!索引不符合要求，请重写\n')
        except TypeError:  # 补充ValueError
            print('!!!索引不符合要求，请重写\n')
        except SyntaxError:  # 补充ValueError
            print('!!!索引不符合要求，请重写\n')

    # 打印原数据或更改后的数据
    print_data(df)
    # 连接MySQL函数，并将修改后的数据导入到MySQL
    mysql_link(df)


# MySQL连接阶段
def mysql_link(df):
    global global_table  # 使用 global 关键字声明全局变量
    global connection  # 使用 global 关键字声明全局变量
    global cursor  # 使用 global 关键字声明全局变量

    print('\n进入MySQL连接阶段：\n')
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
            print('\n!!!注意命名请符合MySQL的命名规则!!!\n')
            global_table = input('自定义表的名字：')  # 定义 全局 表名
            # 将 DataFrame 写入 MySQL 数据库中的表
            df.to_sql(global_table, con=engine, if_exists='replace', index=False)
            break

    cursor = connection.cursor()  # 创建一个 全局 游标
    # 获取表的列信息
    cursor.execute(f"DESCRIBE {global_table}")
    columns = cursor.fetchall()

    # 打印数据类型
    show_type(columns)
    # 更改数据类型
    type_change(columns)


# 数据类型更改阶段
def type_change(columns):
    global global_table  # 使用 global 关键字声明全局变量
    global connection  # 使用 global 关键字声明全局变量
    global cursor  # 使用 global 关键字声明全局变量

    while True:
        try:
            type_index = eval(input('输入要修改的列索引(输入0返回):')) - 1
            if type_index == -1:
                # 提交修改
                connection.commit()
                break

            new_data_type = input(
                f"请输入新的数据类型 for {columns[type_index][0]} (当前类型: {columns[type_index][1]}): ")
            # 构建 ALTER TABLE 语句
            alter_query = f"ALTER TABLE {global_table} MODIFY COLUMN {columns[type_index][0]} {new_data_type}"
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
            print(f'修改成功,原({columns[type_index][1]}) => 新({new_data_type})\n')

    # 获取表的列信息
    cursor.execute(f"DESCRIBE {global_table}")
    columns = cursor.fetchall()
    # 接收新数据，自定义主键函数
    show_type(columns)
    primary_key(columns)


# 自定义主键
def primary_key(columns):
    while True:
        global global_table  # 使用 global 关键字声明全局变量
        global connection  # 使用 global 关键字声明全局变量
        global cursor  # 使用 global 关键字声明全局变量

        try:
            primary_index = eval(input("请输入作为主键的列索引(0：不定义主键): ")) - 1
            if primary_index == -1:
                # 提交修改
                connection.commit()
                print('\n未定义主键\n')
                break

            alter_query = f"ALTER TABLE {global_table} ADD PRIMARY KEY ({columns[primary_index][0]})"
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
        except pymysql.err.ProgrammingError:  # 补充pymysql.err.OperationalError
            print('!!!索引不符合要求，请重写\n')
        else:
            print(f'主键 => ({columns[primary_index][0]})')
            break


# 打印阶段
# 从df中获取列名，会重复调用
def get_columns(df):
    names = df.columns
    return names


# 打印列名及索引，接受的是df，不是columns
def print_data(df):
    column_names = get_columns(df)  # 从df中获取列名

    print('\n现在表的列名及索引:')
    for i, column in enumerate(column_names):
        if i + 1 == len(column_names):  # 0被用于返，所以倒数时不要给最后一个配0索引
            print("{}. {}".format(i + 1, column))
            break

        # 配两个索引，用于应对index<0的情况
        print("{} or {}. {}".format(i + 1, -len(column_names) + i + 1, column))


# 打印数据类型
def show_type(columns):
    print('\n进入数据类型转换阶段：\n')
    for i, column in enumerate(columns):
        if i + 1 == len(columns):  # 0被用于返，所以倒数时不要给最后一个配0索引
            print(f"{i + 1}.{column[0]} (当前类型: {column[1]})\n")
            return

        # 配两个索引，用于应对index<0的情况
        print(f"{i + 1} or {-len(columns) + i + 1}. {column[0]}(当前类型: {column[1]})")


# 主函数
if __name__ == "__main__":
    link_data()  # 开始运行
