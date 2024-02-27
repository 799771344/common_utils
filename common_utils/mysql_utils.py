import asyncio
import time
import traceback

import aiomysql

# mysql_pool = None


# async def create_pool():
#     config = monitor_db
#     loop = asyncio.get_event_loop()
#     try:
#         from sshtunnel import SSHTunnelForwarder
#         # 本地ssh链接
#         services = SSHTunnelForwarder(
#             ('9.134.84.76', 36000),  # B机器的配置--跳板机
#             ssh_password="Music@2017",  # B机器的配置--跳板机账号
#             ssh_username="user_01",  # B机器的配置--跳板机账户密码
#             remote_bind_address=(config["host"], config["port"]))  # A机器的配置-MySQL服务器
#
#         services.start()
#
#         pool = await aiomysql.create_pool(
#             host="127.0.0.1",  # 指定数据库连接的主机地址
#             port=services.local_bind_port,  # 指定数据库连接的端口号
#             user=config["user"],  # 指定数据库连接的用户名
#             password=config["password"],  # 指定数据库连接的密码
#             db=config["database"],  # 指定要连接的数据库名称
#             charset="utf8mb4",  # 指定字符集
#             minsize=0,  # 指定连接池的最小连接数
#             maxsize=20,  # 指定连接池的最大连接数
#             loop=loop,
#             autocommit=True,
#             connect_timeout=120.0,
#             echo=False
#         )
#
#     except:
#         pool = await aiomysql.create_pool(
#             host=config["host"],  # 指定数据库连接的主机地址
#             port=config["port"],  # 指定数据库连接的端口号
#             user=config["user"],  # 指定数据库连接的用户名
#             password=config["password"],  # 指定数据库连接的密码
#             db=config["database"],  # 指定要连接的数据库名称
#             charset="utf8mb4",  # 指定字符集
#             minsize=0,  # 指定连接池的最小连接数
#             maxsize=20,  # 指定连接池的最大连接数
#             loop=loop,  # 指定事件循环
#             autocommit=True,
#             connect_timeout=120.0,
#             echo=False
#         )
#     return pool


async def get_mysql_connection(config):
    loop = asyncio.get_event_loop()
    try:
        from sshtunnel import SSHTunnelForwarder
        # 在单独的线程里建立SSH隧道
        services = SSHTunnelForwarder(
            ('9.134.84.76', 36000),  # B机器的配置--跳板机
            ssh_password="Music@2017",  # B机器的配置--跳板机账号
            ssh_username="user_01",  # B机器的配置--跳板机账户密码
            remote_bind_address=(config["host"], config["port"]))  # A机器的配置-MySQL服务器

        services.start()

        # 用新的host和port参数建立异步MySQL连接
        conn = await aiomysql.connect(
            host="127.0.0.1",  # 指定数据库连接的主机地址
            port=services.local_bind_port,  # 指定数据库连接的端口号
            user=config["user"],  # 指定数据库连接的用户名
            password=config["password"],  # 指定数据库连接的密码
            db=config["database"],  # 指定要连接的数据库名称
            charset="utf8mb4",  # 指定字符集
            loop=loop,
            autocommit=True,
        )
    except:
        conn = await aiomysql.connect(
            host=config["host"],  # 指定数据库连接的主机地址
            port=config["port"],  # 指定数据库连接的端口号
            user=config["user"],  # 指定数据库连接的用户名
            password=config["password"],  # 指定数据库连接的密码
            db=config["database"],  # 指定要连接的数据库名称
            charset="utf8mb4",  # 指定字符集
            loop=loop,
            autocommit=True,
        )

    return conn


class MysqlPool(object):
    def __init__(self, cursorclass="dict"):
        self.config = monitor_db
        if cursorclass == "dict":
            self.cursorclass = aiomysql.DictCursor
        elif cursorclass == "tuple":
            self.cursorclass = aiomysql.Cursor
        # asyncio.ensure_future(self.create_mysql_pool())

    async def save_mysql(self, sql, args=[], is_get_rowcount=False):
        """
        保存数据库
        :param sql: 执行sql语句
        :param args: 添加的sql语句的参数 list[tuple]
        """
        conn = await get_mysql_connection()
        async with conn.cursor(self.cursorclass) as cursor:  # 指定游标类为aiomysql.DictCursor
            new_id = None
            if len(args) > 0:
                await asyncio.wait_for(cursor.execute(sql, args), timeout=3600.0)  # 设置SQL执行超时为5秒
            else:
                await asyncio.wait_for(cursor.execute(sql), timeout=3600.0)  # 设置SQL执行超时为5秒
            rowcount = cursor.rowcount
            if rowcount > 0:
                new_id = cursor.lastrowid  # 获取新插入行的ID
            if is_get_rowcount:
                return rowcount
            conn.close()
            return new_id

    async def select_mysql(self, sql, args=[]):
        conn = await get_mysql_connection()
        async with conn.cursor(self.cursorclass) as cursor:  # 指定游标类为aiomysql.DictCursor
            if len(args) > 0:
                await asyncio.wait_for(cursor.execute(sql, args), timeout=3600.0)  # 设置SQL执行超时为5秒
            else:
                await asyncio.wait_for(cursor.execute(sql), timeout=3600.0)  # 设置SQL执行超时为5秒
            result = await cursor.fetchone()
            conn.close()
            return result

    async def select_mysql_all(self, sql, args=[]):
        conn = await get_mysql_connection()
        async with conn.cursor(self.cursorclass) as cursor:  # 指定游标类为aiomysql.DictCursor
            if len(args) > 0:
                await asyncio.wait_for(cursor.execute(sql, args), timeout=3600.0)  # 设置SQL执行超时为5秒
            else:
                await asyncio.wait_for(cursor.execute(sql), timeout=3600.0)  # 设置SQL执行超时为5秒
            result = await cursor.fetchall()
            conn.close()
            return result

    async def select_mysql_all_yield(self, sql, args=[], batch_size=10):
        conn = await get_mysql_connection()
        async with conn.cursor(self.cursorclass) as cursor:  # 指定游标类为aiomysql.DictCursor
            if len(args) > 0:
                await asyncio.wait_for(cursor.execute(sql, args), timeout=3600.0)  # 设置SQL执行超时为5秒
            else:
                await asyncio.wait_for(cursor.execute(sql), timeout=3600.0)  # 设置SQL执行超时为5秒
            while True:
                result = await cursor.fetchmany(batch_size)
                if not result or len(result) < 1:
                    conn.close()
                    raise StopAsyncIteration
                yield result
