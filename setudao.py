import sqlite3
import os
import logging
from datetime import datetime,timedelta
from hoshino import R,logger
import time

class DBError(Exception):
    def __init__(self, msg, *msgs):
        self._msgs = [msg, *msgs]

    def __str__(self):
        return '\n'.join(self._msgs)

    @property
    def message(self):
        return str(self)

    def append(self, msg:str):
        self._msgs.append(msg)


class DatabaseError(DBError):
    pass

DB_PATH = os.path.expanduser('~/.hoshino/setutax.db')

class SqliteDao(object):
    def __init__(self, table, columns, fields ):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        self._dbpath = DB_PATH
        self._table = table
        self._columns = columns
        self._fields = fields
        self._create_table()


    def _create_table(self):
        sql = "CREATE TABLE IF NOT EXISTS {0} ({1})".format(self._table, self._fields)
        with self._connect() as conn:
            conn.execute(sql)


    def _connect(self):
        return sqlite3.connect(self._dbpath, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)




class SetuDao(SqliteDao):
    def __init__(self):
        super().__init__(
            table='setutax',
            columns='id,gid,uid,fname,lk,unlk,tags,ts,sendct',
            fields='''
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gid INT NOT NULL,
            uid INT NOT NULL,
            fname TEXT NOT NULL,
            lk INT NOT NULL,
            unlk INT NOT NULL,
            tags TEXT NOT NULL,
            ts DATETIME,
            sendct INT NOT NULL
            '''
            )

    def add(self,gid,uid,filename):
        with self._connect() as conn:
            try:
                conn.execute('''
                    insert into {0}({1}) VALUES (NULL,?,?,?,0,0,'',datetime('now','localtime'),0)
                         '''.format(self._table,self._columns),
                    (gid,uid,filename))
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('增加失败')


    def delete(self,gid,setu_id):
        with self._connect() as conn:
            try:
                ret = conn.execute('''
                    SELECT fname FROM {0} WHERE id=? and gid=?
                    '''.format(self._table),
                    ( setu_id,gid) ).fetchone()
                fname = ret[0] if ret else ''
                r = conn.execute('''
                    DELETE FROM {0} WHERE id=? and gid=?
                         '''.format(self._table),
                    (setu_id,gid))
                if r.rowcount >= 1:
                    if fname != '':
                        path = os.path.join(R.img(f'setutax/{gid}/').path, fname)
                        if os.path.exists(path):
                            os.remove(path)
                    return True
                else:
                    return False
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('删除失败')


    def get_setu(self,gid=0,uid=0):
        with self._connect() as conn:
            try:
                sqlstr= ''
                s2 = []
                if gid !=0:
                    s2.append(f' gid={gid}')
                if uid !=0:
                    s2.append(f' uid={uid}')
                s2.append(f" instr(tags,'r18')<=0")
                if len(s2)>0:
                    sqlstr = ' where '+ ' and '.join(s2)
                res = conn.execute('''
                    SELECT {1} FROM {0} {2}
                    '''.format(self._table,self._columns,sqlstr),
                    ()).fetchall()
                return [{"id":r[0],"gid":r[1],"uid":r[2],"fname":r[3],"lk":r[4],"unlk":r[5],"tags":r[6],"ts":r[7],"sendct":r[8]} for r in res]
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('读取失败')
    
    def get_r18_setu(self,gid=0,uid=0):
        with self._connect() as conn:
            try:
                sqlstr= ''
                s2 = []
                if gid !=0:
                    s2.append(f' gid={gid}')
                if uid !=0:
                    s2.append(f' uid={uid}')
                s2.append(f" instr(tags,'r18')>0")
                if len(s2)>0:
                    sqlstr = ' where '+ ' and '.join(s2)
                res = conn.execute('''
                    SELECT {1} FROM {0} {2}
                    '''.format(self._table,self._columns,sqlstr),
                    ()).fetchall()
                return [{"id":r[0],"gid":r[1],"uid":r[2],"fname":r[3],"lk":r[4],"unlk":r[5],"tags":r[6],"ts":r[7],"sendct":r[8]} for r in res]
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('读取失败')


    def count_increase(self,id):
        with self._connect() as conn:
            try:
                conn.execute('''
                    update {0} set sendct=sendct+1 where id=?
                         '''.format(self._table),
                    (id,))
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('增加失败')

    def add_tags(self,id,tags,gid):
        with self._connect() as conn:
            try:
                r = conn.execute('''
                    update {0} set tags=? where id=? and gid=?
                         '''.format(self._table),
                    (tags,id,gid))
                if r.rowcount >= 1:
                    return True
                else:
                    return False
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('更新失败')

    def add_like(self,id,islike,gid):
        with self._connect() as conn:
            try:
                sql2= 'lk=lk+1' if islike else 'unlk=unlk+1'
                r = conn.execute('''
                    update {0} set {1} where id=? and gid=?
                         '''.format(self._table,sql2),
                    (id,gid))
                if r.rowcount >= 1:
                    return True
                else:
                    return False
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('更新失败')

    def cancel_like(self,id,islike,gid):
        with self._connect() as conn:
            try:
                sql2= 'lk=lk-1' if islike else 'unlk=unlk-1'
                r = conn.execute('''
                    update {0} set {1} where id=? and gid=?
                         '''.format(self._table,sql2),
                    (id,gid))
                if r.rowcount >= 1:
                    return True
                else:
                    return False
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('更新失败')

class SetuLikeLogDao(SqliteDao):
    def __init__(self):
        super().__init__(
            table='setutax_log',
            columns='id,gid,uid,setuid,islk,ts',
            fields='''
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gid INT NOT NULL,
            uid INT NOT NULL,
            setuid INT NOT NULL,
            islk INT NOT NULL,
            ts DATETIME
            '''
            )

    def add(self,gid,uid,setuid,islike):
        with self._connect() as conn:
            try:
                conn.execute('''
                    insert into {0}({1}) VALUES (NULL,?,?,?,?,datetime('now','localtime'))
                         '''.format(self._table,self._columns),
                    (gid,uid,setuid,islike))
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('增加失败')



    def get_user_log(self,gid,uid,setuid,islike):
        with self._connect() as conn:
            try:
                res = conn.execute('''
                    SELECT {1} FROM {0} WHERE gid=? and uid=? and setuid=? and islk=?
                    '''.format(self._table,self._columns),
                    (gid,uid,setuid,islike)).fetchone()
                if res:
                    return True
                else:
                    return False
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('读取失败')

    def delete(self,gid,uid,setuid,islike):
        with self._connect() as conn:
            try:
                r = conn.execute('''
                    DELETE FROM {0} WHERE gid=? and uid=? and setuid=? and islk=?
                         '''.format(self._table,self._columns),
                    (gid,uid,setuid,islike))
                if r.rowcount>=1:
                    return True
                else:
                    return False
            except (sqlite3.DatabaseError) as e:
                logger.error(f'{e}')
                raise DatabaseError('增加失败')