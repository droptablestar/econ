from pymongo import MongoClient

class DB:
    def __init__(self, db_name):
        client = MongoClient()
        # print(client)
        self.db = client[db_name]

    def get_collection(self, collection):
        return self.db[collection]

    def count_all(self):
        collections = self.db.collection_names()

        print('Collections inside the db:')
        total = 0
        for c in collections:
            if 'system' not in c:
                print(c, '-', self.db[c].count(), 'records')
                total += self.db[c].count()
        print('Total: ', total)
