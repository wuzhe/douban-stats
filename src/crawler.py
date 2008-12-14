#
# Crawler to collect data from douban via its API
# author: Wu Zhe <wu@madk.org>
#

import douban.service
import os, sys, sqlite3, atexit, pickle, datetime, time, socket, gdata, signal
from collections import deque
from gdata.service import RequestError

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
VISITED_PATH = os.path.normpath('../visited_users.pkl') # pickle

SEED_USERS = (1000001, 2461197, 1021991) # seed UIDs
REQ_CONTROL = True # control request frenquency or not
REQ_INTERVAL = 60.0/42 # Minimum interval between reqs, API TOS says I
                       # can't req faster than 40 per min
MAX_RESULTS = 50 # max-results per page in douban API, currently API
                 # limits it to 50
TOTAL_USERS = 2000000 # Estimated number of user accounts in douban
TIMEOUT_LIMIT = 10

def nowp():
    return '[' + datetime.datetime.now().isoformat(' ') + ']'

class TimeoutError(Exception): pass

def signal_handler(signum, frame):
    raise TimeoutError()
signal.signal(signal.SIGALRM, signal_handler)

class User:

    # Map fields in `users' table to douban_python gdata api methods
    Mapper = (('uid', lambda x: int(x.GetSelfLink().href.split('/')[-1])),
              ('uid_text', lambda x: x.uid.text),
              ('location', lambda x: x.location.text),
              ('nickname', lambda x: x.title.text),
              ('icon_url', lambda x: x.link[2].href),
              ('homepage', lambda x: x.link[3].href),
              ('description', lambda x: x.content.text))

    Sleep_Timeout_init = 2 # 2 seconds
    Sleep_Banned_init = 3600 + 5 # retry in 1 hour, douban remove ban
                                 # after 1 hour
    last_req_time = 0

    def __init__(self, db_cursor, client, uri_id):
        self.db_cursor = db_cursor
        self.client = client
        self.uri_id = uri_id # uri_id is either uid or uid_text
        self.data = []
        self.rows_store = {}
        self.friend_pairs = []
        self.contact_pairs = [] # actually means `follows' in database
        self.api_req_count = 0

    def _req_api(self, what, uri):
        if what == 'people':
            getter = self.client.GetPeople
        elif what == 'friends':
            getter = self.client.GetFriends

        # Sleep if request too fast
        now = time.time()
        if REQ_CONTROL and (now - User.last_req_time) < REQ_INTERVAL:
            sleep_time = REQ_INTERVAL - (now - User.last_req_time)
            print nowp() + " zzZ for %s seconds, to be polite" % sleep_time
            time.sleep(sleep_time)

        timeout = User.Sleep_Timeout_init
        banned = User.Sleep_Banned_init
        while True:
            signal.alarm(TIMEOUT_LIMIT)
            try:
                f = getter(uri)
            except (TimeoutError, socket.error):
                print nowp() + " ** Connection timeout, retry in %s seconds" % \
                      timeout
                time.sleep(timeout)
                timeout *= 2
            except RequestError:
                print nowp() + " ** I am miserably banned, retry in %s hours" % \
                      (banned/3600.0)
                time.sleep(banned)
                banned *= 2
            else:
                break
            finally:
                signal.alarm(0) # disable the alarm
        User.last_req_time = time.time()
        self.api_req_count += 1
        return f

    def _get_userlist_from_api(self, what):
        entries = []
        start_i = 1
        while start_i == 1 or len(f.entry) == 50:
            f = self._req_api('friends',
                              '/people/%s/%s?start-index=%s&max-results=%s' % \
                              (self.uri_id, what, start_i, MAX_RESULTS))
            entries.extend(f.entry)
            start_i += MAX_RESULTS

        rows = []
        for e in entries:
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
        p = self._req_api('people', '/people/%s' % self.uri_id)
        fields = []
        for field, getter in User.Mapper:
            try:
                fields.append(getter(p))
            except (AttributeError, IndexError):
                fields.append(None)
        self.data = fields
        self._store_userdata()
        return self.data

    def store_users(self, uids):
        rows = filter(lambda x: x[0] in uids, self.rows_store.values())
        self.db_cursor.executemany("INSERT INTO users VALUES " +
                                   "(?,?,?,?,?,?,?,DATETIME('NOW'))",
                                   rows)

    def store_relations(self):
        new_friend_pairs = []
        for pair in self.friend_pairs:
            self.db_cursor.execute("SELECT count(*) FROM friends " +
                                   "WHERE user2=? AND user1=?", pair)
            if self.db_cursor.fetchone()[0] == 0:
                new_friend_pairs.append(pair)
        self.db_cursor.executemany("INSERT INTO friends VALUES (?, ?)",
                                   new_friend_pairs)
        self.db_cursor.executemany("INSERT INTO follows VALUES (?, ?)",
                                   self.contact_pairs)

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
    cursor.execute("PRAGMA cache_size = 20000;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA temp_store = MEMORY;")

    # Get the user list to crawl
    if not os.path.exists(USER_PATH):
        curr_list = SEED_USERS
        queue = deque(curr_list)
    else:
        print "Restoring running state from the previous run"
        print "=" * 8
        pkl_file = open(USER_PATH, 'rb')
        queue = pickle.load(pkl_file)
        pkl_file.close()

    # Get the visited user list
    if not os.path.exists(VISITED_PATH):
        visited = set([])
    else:
        pkl_file = open(VISITED_PATH)
        visited = pickle.load(pkl_file)
        pkl_file.close()

    # Set up the exit function
    def save_state(conn, cursor, queue, visited):
        print "=" * 8
        print "Saving running state ..."
        if queue:
            pkl_file = open(USER_PATH, 'wb')
            pickle.dump(queue, pkl_file)
            pkl_file.close()
        if visited:
            pkl_file = open(VISITED_PATH, 'wb')
            pickle.dump(visited, pkl_file)
            pkl_file.close()
        cursor.execute("SELECT count(*) FROM users")
        count = cursor.fetchone()[0]
        conn.close()
        print "Sir, I have collected %d users for you so far." % count
    atexit.register(save_state, conn, cursor, queue, visited)

    client = douban.service.DoubanService(api_key=APIKEY)
    cursor.execute("SELECT uid FROM users")
    users_in_db = set([x[0] for x in cursor.fetchall()])

    # BFS crawl
    new_reqs = 0
    total_reqs = 0
    queue_length = len(queue)
    while queue:
        curr_uid = queue.popleft()
        if curr_uid in visited: continue

        # API heavy operations
        begin_time = time.time()
        user = User(cursor, client, curr_uid)
        uid = user.get_data()[0]
        users_in_db.add(uid)
        user_users = user.get_friends() | user.get_follows()
        end_time = time.time()

        # CPU heavy operations
        new_users = user_users - users_in_db
        user.store_users(new_users)
        user.store_relations()
        conn.commit()
        users_in_db |= new_users
        visited.add(curr_uid)
        queue.extend(new_users)

        # Update the frequency stats
        duration = end_time - begin_time
        new_reqs = user.api_req_count
        req_freq = int(60.0 * new_reqs / duration) # reqs per min
        visit_freq = int(3600.0 / duration) # visit per hour
        # estimated time remaining
        etr = int((TOTAL_USERS - len(visited)) / visit_freq) \
              if visit_freq != 0 else sys.maxint

        # Stats printing
        total_reqs += new_reqs
        new_queue_length = len(queue)
        queue_delta = new_queue_length - queue_length
        queue_length = new_queue_length
        print "%s V:%d Q:%d(%+d) D:%d(%+d) R:%d(%+d) RF:%d VF:%d ETR:%d U:%s(%s)" % \
              (nowp(), len(visited), queue_length, queue_delta,
               len(users_in_db), len(new_users), total_reqs, new_reqs, req_freq,
               visit_freq, etr, user.data[1], user.data[3])

if __name__ == "__main__":
    main()
