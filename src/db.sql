-- database schema
-- author: Wu Zhe <wu@madk.org>

CREATE TABLE IF NOT EXISTS users (
       uid INTEGER,
       uid_text TEXT,
       location TEXT,
       nickname TEXT,
       icon_url TEXT,
       homepage TEXT,
       description BLOB,
       created DATE,
       PRIMARY KEY (uid)
);

CREATE TABLE IF NOT EXISTS friends (
       user1 INTEGER REFERENCES users (uid),
       user2 INTEGER REFERENCES users (uid),
       PRIMARY KEY (user1, user2)
);

CREATE TABLE IF NOT EXISTS follows (
       from_user INTEGER REFERENCES users (uid),
       to_user INTEGER REFERENCES users (uid),
       PRIMARY KEY (from_user, to_user)
);

CREATE TABLE IF NOT EXISTS tastes (
       uid INTEGER REFERENCES users (uid),
       tag TEXT,
       count INTEGER,
       PRIMARY KEY (uid, tag)
);