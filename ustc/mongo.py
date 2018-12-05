from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import pprint

class DB:
    def __init__(self, db_name):
        with open('credentials.txt') as f:
            lines = [c.strip('\n') for c in f.readlines()]

        client = MongoClient('127.0.0.1',
                             username=lines[0],
                             password=lines[1],
                             authSource=lines[2],
                             authMechanism='SCRAM-SHA-1')
        try:
            client.admin.command('ismaster')
        except ConnectionFailure:
            print("Server not available")
        self.db = client[db_name]

    def get_collection(self, collection):
        return self.db[collection]

    def get_collections(self, exclude=None, system=False):
        return list(filter(lambda c: c != exclude, self.db.collection_names())) if system \
            else list(filter(lambda c: c != exclude, self.db.collection_names(include_system_collections=False)))

    def count_all(self, exclude=None):
        collections = self.get_collections(exclude)

        print('Collections inside the db:')
        total = 0
        for c in collections:
            if 'system' not in c:
                print(c, '-', self.db[c].count(), 'records')
                total += self.db[c].count()
        print('Total: ', total)

    def remove_from_all(self, coll, field, char):
        f = self.db[coll].find()
        for r in f:
            print(r['url'])
            r[field] = r[field].replace(char, '')
            return

    # looking for a list of collection names and a single collection to compare
    # and the unique column name to check. e.g., find_differences([a,b], d, 'key')
    # will find all the records in collection d that don't exist in collections a and b
    # and have the same value for the 'key' property
    def find_differences(self, coll_list, coll, uid):
        coll_list_uids = []
        for c in coll_list:
            pprint.pprint(c)
            # coll_list_uids.extend(list(self.db[c].find({}, {'_id': 0, c: 1}, limit=10)))
            docs = self.db[c].find({}, projection={uid: 1, '_id': 0})
            coll_list_uids.extend([d[uid] for d in docs])
        docs = self.db[coll].find({}, projection={uid: 1, '_id': 0})
        coll_uids = ([d[uid] for d in docs])
        print(len(coll_list_uids))
        print(len(coll_uids))

        coll_list_set = set(coll_list_uids)
        print(len(coll_list_set))
        diff = set(coll_uids) - coll_list_set
        return diff

    # field to modify, characters to remove.
    # ex. clean('html', '\t','\n')
    def clean(self):
        collections = self.get_collections()
        for c in collections:
            print(c)
            self.db[c].forEach(bson.Code('''
                function(e,i) {
                    e.html = e.html.replace(/[\t\n\r]/g, '');
                    db.{}.save(e);
                }'''.format(c)))
            return

'''
query to replace:
db.Book_Trade.find().forEach(function(e,i) { e.html = e.html.replace(/[\n\r]/g, ''); db.Book_Trade.save(e);});
'''
