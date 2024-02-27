import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


class ProductionMongoClient:
    def __init__(self, db_name, uri="mongodb://localhost:27017/"):
        try:
            self.client = MongoClient(uri)
            # 运行一个服务器状态的命令，测试连接是否成功
            self.client.admin.command('ping')
        except ConnectionFailure:
            raise ConnectionFailure("Server not available")
        self.db = self.client[db_name]

    def insert_one(self, collection_name, data):
        try:
            collection = self.db[collection_name]
            return collection.insert_one(data).inserted_id
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def find_one(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            return collection.find_one(query)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def find(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            return collection.find(query)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def update_one(self, collection_name, query, data):
        try:
            collection = self.db[collection_name]
            return collection.update_one(query, data)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def delete_one(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            return collection.delete_one(query)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def delete_many(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            return collection.delete_many(query)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def update_many(self, collection_name, query, data):
        try:
            collection = self.db[collection_name]
            return collection.update_many(query, data)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def insert_many(self, collection_name, data):
        try:
            collection = self.db[collection_name]
            return collection.insert_many(data)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def find_one_and_delete(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            return collection.find_one_and_delete(query)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def find_one_and_replace(self, collection_name, query, data):
        try:
            collection = self.db[collection_name]
            return collection.find_one_and_replace(query, data)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def find_one_and_update(self, collection_name, query, data):
        try:
            collection = self.db[collection_name]
            return collection.find_one_and_update(query, data)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def replace_one(self, collection_name, query, data):
        try:
            collection = self.db[collection_name]
            return collection.replace_one(query, data)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def count_documents(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            return collection.count_documents(query)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")

    def aggregate(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            return collection.aggregate(query)
        except OperationFailure as e:
            raise OperationFailure(f"An error occurred: {e}")


# 使用示例
if __name__ == '__main__':
    # 替换为你的数据库和连接字符串
    db_name = 'your_production_db'
    uri = "your_production_db_uri"

    client = ProductionMongoClient(db_name, uri)

    # 插入数据
    try:
        inserted_id = client.insert_one('your_collection', {'key': 'value'})
        print(f"Inserted document with ID: {inserted_id}")
    except Exception as e:
        print(e)

    # 查询数据
    try:
        document = client.find_one('your_collection', {'key': 'value'})
        print(document)
    except Exception as e:
        print(e)

    # 更多操作...