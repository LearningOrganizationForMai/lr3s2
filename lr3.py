import psycopg2

class SQL:
    def __init__(self, **config):
        self.con = psycopg2.connect(**config)
        self.cur = self.con.cursor()
        self._reset()
    
    def _reset(self):
        self._cols, self._table, self._joins = '*', '', []
        self._where, self._params = '', ()
        self._order = ''
    
    def select(self, *cols):
        self._cols = ', '.join(cols) if cols else '*'
        return self
    
    def from_(self, table):
        self._table = table
        return self
    
    def where(self, cond, *params):
        self._where, self._params = f'WHERE {cond}', params
        return self
    
    def order_by(self, col, direction='ASC'):
        self._order = f'ORDER BY {col} {direction}'
        return self
    
    def join(self, table, on, type='INNER'):
        self._joins.append(f'{type} JOIN {table} ON {on}')
        return self
    
    def left_join(self, table, on):
        return self.join(table, on, 'LEFT')
    
    def right_join(self, table, on):
        return self.join(table, on, 'RIGHT')
    
    def full_join(self, table, on):
        return self.join(table, on, 'FULL OUTER')
    
    def union(self, query):
        return f'{self.build()[0]} UNION {query}'
    
    def build(self):
        sql = f'SELECT {self._cols} FROM {self._table}'
        if self._joins: sql += ' ' + ' '.join(self._joins)
        if self._where: sql += f' {self._where}'
        if self._order: sql += f' {self._order}'
        return sql, self._params
    
    def execute(self):
        sql, params = self.build()
        self.cur.execute(sql, params)
        self.con.commit()
        result = self.cur.fetchall()
        self._reset()
        return result
    
    def fetch(self):
        sql, params = self.build()
        self.cur.execute(sql, params)
        cols = [d[0] for d in self.cur.description]
        rows = self.cur.fetchall()
        self._reset()
        return [dict(zip(cols, r)) for r in rows]
    
    def insert(self, **values):
        cols = ', '.join(values.keys())
        placeholders = ', '.join(['%s'] * len(values))
        sql = f'INSERT INTO {self._table} ({cols}) VALUES ({placeholders}) RETURNING id'
        self.cur.execute(sql, tuple(values.values()))
        self.con.commit()
        return self.cur.fetchone()[0]
    
    def update(self, **values):
        set_clause = ', '.join([f'{k} = %s' for k in values.keys()])
        sql = f'UPDATE {self._table} SET {set_clause} {self._where}'
        self.cur.execute(sql, tuple(values.values()) + self._params)
        self.con.commit()
        rows = self.cur.rowcount
        self._reset()
        return rows
    
    def delete(self):
        sql = f'DELETE FROM {self._table} {self._where}'
        self.cur.execute(sql, self._params)
        self.con.commit()
        rows = self.cur.rowcount
        self._reset()
        return rows

    def fetch_column_ordered(self, col, direction='ASC'):
        sql = f'SELECT {col} FROM {self._table} ORDER BY {col} {direction}'
        self.cur.execute(sql)
        return [r[0] for r in self.cur.fetchall()]

    def fetch_id_range(self, start, finish):
        sql = f'SELECT * FROM {self._table} WHERE id BETWEEN %s AND %s'
        self.cur.execute(sql, (start, finish))
        cols = [d[0] for d in self.cur.description]
        return [dict(zip(cols, r)) for r in self.cur.fetchall()]

    def delete_id_range(self, start, finish):
        sql = f'DELETE FROM {self._table} WHERE id BETWEEN %s AND %s'
        self.cur.execute(sql, (start, finish))
        self.con.commit()
        return self.cur.rowcount

    def get_table_structure(self):
        sql = 'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position'
        self.cur.execute(sql, (self._table,))
        return self.cur.fetchall()

    def fetch_row_by_value(self, col, value):
        sql = f'SELECT * FROM {self._table} WHERE {col} = %s'
        self.cur.execute(sql, (value,))
        cols = [d[0] for d in self.cur.description]
        row = self.cur.fetchone()
        return dict(zip(cols, row)) if row else None

    def drop_table(self):
        self.cur.execute(f'DROP TABLE IF EXISTS {self._table} CASCADE')
        self.con.commit()
        return True

    def add_column(self, col_name, col_type):
        self.cur.execute(f'ALTER TABLE {self._table} ADD COLUMN {col_name} {col_type}')
        self.con.commit()
        return True

    def remove_column(self, col_name):
        self.cur.execute(f'ALTER TABLE {self._table} DROP COLUMN {col_name}')
        self.con.commit()
        return True

    def export_csv(self, filename):
        with open(filename, 'w', newline='') as f:
            self.cur.copy_expert(f'COPY {self._table} TO STDOUT WITH CSV HEADER', f)
        return True

    def import_csv(self, filename):
        self.cur.execute(f'TRUNCATE TABLE {self._table} RESTART IDENTITY')
        with open(filename, 'r', newline='') as f:
            self.cur.copy_expert(f'COPY {self._table} FROM STDIN WITH CSV HEADER', f)
        self.con.commit()
        return True

    def close(self):
        self.cur.close()
        self.con.close()


db = SQL(host='localhost', database='mydb', user='admin', password='12345')
db.cur.execute('SET search_path TO online_cinema')

names_desc = db.from_('users').fetch_column_ordered('name', 'DESC')
print(f'Имена по убыванию: {names_desc}')

range_users = db.from_('users').fetch_id_range(1, 5)
print(f'Пользователи c id 1-5: {range_users}')

deleted_range = db.from_('users').delete_id_range(100, 200)
print(f'Удалено строк в диапазоне по id: {deleted_range}')

structure = db.from_('users').get_table_structure()
print(f'Структура таблицы: {structure}')

target_user = db.from_('users').fetch_row_by_value('name', 'Charlie')
print(f'Найдена строка: {target_user}')

db.from_('users').add_column('temp_col', 'VARCHAR(50)')
db.from_('users').remove_column('temp_col')
print('Добавление и удаление столбца')

db.from_('users').export_csv('users_export.csv')
db.from_('users').import_csv('users_export.csv')
print('Экспорт и импорт были сделаны')

db.close()