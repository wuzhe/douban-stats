#
# Crawler to collect data from douban via its API
# author: Wu Zhe <wu@madk.org>
#

import douban.service
import os, sys, sqlite3, atexit, pickle, datetime
from collections import deque

def open_and_read(path):
    f = open(path)
    text = f.read()
    f.close()
    return text

# API key
APIKEY = open_and_read(os.path.normpath('../API_KEY')).split('\n')[0]
SECRET = ''

# Path settings
SQL_PATH = os.path.normpath('db.sql')
DB_PATH = os.path.normpath('../data.db') # sqlite3 database
USER_PATH = os.path.normpath('../user_queue.pkl') # pickle

# Seed users
#SEED_USERS = [1000001, 1021991]
SEED_USERS = [1584719, 1021991]

# max-results per page in douban API
MAX_RESULTS = sys.maxint

class User:

    # Map fields in `users' table to douban_python gdata api
    Mapper = (('uid', lambda x: int(x.GetSelfLink().href.split('/')[-1])),
              ('uid_text', lambda x: x.uid.text),
              ('location', lambda x: x.location.text),
              ('nickname', lambda x: x.title.text),
              ('icon_url', lambda x: x.link[2].href),
              ('homepage', lambda x: x.link[3].href),
              ('description', lambda x: x.content.text))


    def __init__(self, db_cursor, client, uri_id):
        self.db_cursor = db_cursor
        self.client = client
        self.uri_id = uri_id # uri_id is either uid or uid_text
        self.data = []
        self.rows_store = {}
        self.friend_pairs = []
        self.contact_pairs = [] # actually means follows
        self.api_req_count = 0

    def _store_userdata(self):
        self.db_cursor.execute("SELECT count(*) FROM users WHERE uid=?",
                               (self.data[0],))
        if not self.db_cursor.fetchone()[0]:
            self.db_cursor.execute("INSERT INTO users VALUES " +
                                   "(?,?,?,?,?,?,?,DATETIME('NOW'))",
                                   self.data)

    def get_data(self):
        if self.data: return self.data

        self.db_cursor.execute("SELECT * FROM users WHERE uid=? OR uid_text=?",
                               (self.uri_id, str(self.uri_id)))
        row = self.db_cursor.fetchone()
        if row:
            self.data = row
            return self.data

        # If not in database, get it via API and save it in database
        p = self.client.GetPeople('/people/%s' % self.uri_id)
        self.api_req_count += 1
        fields = []
        for field, getter in User.Mapper:
            try:
                fields.append(getter(p))
            except (AttributeError, IndexError):
                fields.append(None)
        self.data = fields
        self._store_userdata()
        return self.data

    def _get_userlist_from_api(self, what):
        feed = self.client.GetFriends('/people/%s/%s?max-results=%s' %
                                      (self.uri_id, what, MAX_RESULTS))
        self.api_req_count += 1
        rows = []
        for e in feed.entry:
            fields = []
            for field, getter in User.Mapper:
                try:
                    fields.append(getter(e))
                except (AttributeError, IndexError):
                    fields.append(None)
            rows.append(fields)
        uid_list = [fields[0] for fields in rows]
        rows_store = dict(zip(uid_list, rows))
        self.rows_store.update(rows_store)
        vars(self)[what[:-1] + '_pairs'] = \
                      zip((self.get_data()[0],) * len(uid_list), uid_list)
        return set(uid_list)

    def store_users(self, uids):
        rows = filter(lambda x: x[0] in uids, self.rows_store.values())
        self.db_cursor.executemany("INSERT INTO users VALUES " +
                                   "(?,?,?,?,?,?,?,DATETIME('NOW'))",
                                   rows)

    def store_relations(self):
        for pair in self.friend_pairs:
            self.db_cursor.execute("SELECT count(*) FROM friends " +
                                   "WHERE (user1=? AND user2=?) " +
                                   "OR (user2=? AND user1=?)",
                                   pair*2)
            if self.db_cursor.fetchone()[0] == 0:
                self.db_cursor.execute("INSERT INTO friends VALUES (?, ?)",
                                       pair)
        for pair in self.contact_pairs:
            self.db_cursor.execute("SELECT count(*) FROM follows " +
                                   "WHERE from_user=? AND to_user=?",
                                   pair)
            if self.db_cursor.fetchone()[0] == 0:
                self.db_cursor.execute("INSERT INTO follows VALUES (?, ?)",
                                       pair)

    def get_friends(self):
        self.db_cursor.execute('SELECT user2 FROM friends WHERE user1=?',
                               (self.get_data()[0],))
        set1 = set([x[0] for x in self.db_cursor.fetchall()])
        if not set1:
            set1 = self._get_userlist_from_api('friends')
        self.db_cursor.execute('SELECT user1 FROM friends WHERE user2=?',
                               (self.get_data()[0],))
        set2 = set([x[0] for x in self.db_cursor.fetchall()])
        friends = set1 | set2
        return friends

    def get_follows(self):
        self.db_cursor.execute('SELECT to_user FROM follows WHERE from_user=?',
                               (self.get_data()[0],))
        follows = set([x[0] for x in self.db_cursor.fetchall()])
        if not follows:
            follows = self._get_userlist_from_api('contacts')
        return follows

    def get_followers(self):
        self.db_cursor.execute('SELECT from_user FROM follows WHERE to_user=?',
                               (self.get_data()[0],))
        followers = set([x[0] for x in self.db_cursor.fetchall()])
        return followers

    def get_tags(self):
        # get book (tag, count)
        pass
        return set([])

def main():
    # Connect to database, create it if not exists.
    if not os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executescript(open_and_read(SQL_PATH))
        conn.commit()
    else:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

    # Get the user list to crawl
    if not os.path.exists(USER_PATH):
        curr_list = SEED_USERS
        queue = deque(curr_list)
    else:
        pkl_file = open(USER_PATH, 'rb')
        queue = pickle.load(pkl_file)
        pkl_file.close()

    # Set up the exit function
    def save_queue(queue):
        if queue:
            print 'Saving processing user queue (length: %s) in "%s" ...' % \
                  (len(queue), USER_PATH)
            pkl_file = open(USER_PATH, 'wb')
            pickle.dump(queue, pkl_file)
            pkl_file.close()
        cursor.execute("SELECT count(*) FROM users")
        count = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        print "I have collected %d users so far." % count
    atexit.register(save_queue, queue=queue)

    # BFS
    client = douban.service.DoubanService(api_key=APIKEY)
    cursor.execute("SELECT uid FROM users")
    users_in_db = set([x[0] for x in cursor.fetchall()])
    visited = set(users_in_db) # get a copy
    new_reqs = 0
    total_reqs = 0
    while queue:
        curr_uid = queue.popleft()
        if curr_uid in visited: continue

        now = datetime.datetime.now().isoformat(' ')
        print "%s, QUEUE:%s, DB:%s, API:%s(+%s), getting data for UID %s." % \
              (now, len(queue), len(users_in_db), total_reqs, new_reqs,
               curr_uid)
        user = User(cursor, client, curr_uid)
        uid = user.get_data()[0]
        users_in_db.add(uid)
        new_users = (user.get_friends() | user.get_follows()) - users_in_db
        user.store_users(new_users)
        user.store_relations()
        users_in_db |= new_users
        conn.commit()

        new_reqs = user.api_req_count
        total_reqs += new_reqs
        visited.add(curr_uid)
        queue.extend(new_users)

if __name__ == "__main__":
    main()
