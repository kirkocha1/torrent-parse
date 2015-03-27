import os
import bencoder
import argparse
import sqlite3 as sql


def get_all_torrents(dir):
    torrentfilepath = []
    for root, dirs, files in os.walk(dir):
        for original_name in files:
            name = original_name.lower()
            if name.endswith('.torrent'):
                torrentfilepath.append(os.path.join(root, original_name))
    return torrentfilepath


def token_torrent_info(torrent):
    with open(torrent, 'rb') as f:
        raw_data = f.read()
        data = bencoder.decode(raw_data)
        all = {}
        tor = {}
        tor['url'] = data[b'comment']
        tor['date'] = int(data[b'creation date'])
        tor['list'] = [str(a) for a in data[b'announce-list']]
        tor['created by'] = data[b'created by'].decode('utf-8')
        all['tor'] = tor
        all['data'] = raw_data
        return all


def base(name):
    base = sql.connect(name)
    cur = base.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS torrent (id INTEGER PRIMARY KEY NOT NULL,'
                ' url TEXT, date INTEGER, created_by TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS file (id INTEGER, file, FOREIGN KEY (id) REFERENCES torrent (id))')
    cur.execute('CREATE TABLE IF NOT EXISTS list (id INTEGER, id_l INTEGER PRIMARY KEY NOT NULL, list TEXT,'
                ' FOREIGN KEY (id) REFERENCES torrent (id))')
    return base


def main():

    parser = argparse.ArgumentParser(description='torrent database')

    parser.add_argument('dir', metavar='DIRECTORY', type=str,
                        help='input directory of your computer, example /disk1/dir1/')
    args = parser.parse_args()

    torrents = get_all_torrents(args.dir)
    tor_base = base('torrents.sqlite3')
    cur = tor_base.cursor()

    for torrent in torrents:
        all = token_torrent_info(torrent)
        tor = all['tor']
        data = all['data']

        cur.execute('INSERT INTO torrent (url, date, created_by) VALUES (?,?,?)',
                    (tor['url'], tor['date'], tor['created by'], ))
        items = []

        for item in tor['list']:
            items.append((cur.lastrowid, item))

        cur.executemany('INSERT INTO list (id, list) VALUES (?,?)', items)
        cur.execute('INSERT INTO file (id, file) VALUES (?,?)', (cur.lastrowid, data))

    tor_base.commit()
    cur.close()

if __name__ == '__main__':
    main()
