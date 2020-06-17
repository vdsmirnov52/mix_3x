#!/usr/bin/env python
# -*- coding: utf-8 -*-

import	sys
import	sqlite3

class dblite:
	r""" Работа с SQLite
	Warning('You can only execute one statement at a time.',) ... только одно заявление за раз
Функции:
	execute (query, [vals])		- Исполняет SQL запрос.	Возвращает: {True|False}
	get_row  (query, [vals])	- Читает одну запись.   Возвращает: row = (val1, val2, ...)
	get_rows (query, [vals])	- Читает несколько записей      Возвращает: rows = [(row1), (row2), ...]
	get (query, fall, [vals])       - Исполняет запрос и читает данные. Если fall: 1 - fetchall() иначе 0 - fetchone())
	get_table (tname, [swhere], [cols])	- Возвращает (desc, rows) или None
Примеры использования vals 
	C подставновкой по порядку на места знаков вопросов:
		cursor.execute("SELECT Name FROM Artist ORDER BY Name LIMIT ?", ('2'))
	C использованием именнованных замен:
		cursor.execute("SELECT Name from Artist ORDER BY Name LIMIT :limit", {"limit": 3})
	или
		new_artists = [ ('A Aagrh!',), ('A Aagrh!-2',), ('A Aagrh!-3',), ]
		cursor.executemany("insert into Artist values (Null, ?);", new_artists)
	"""
	last_error =	None
	desc = []	## Список наименования полей последнего запроса

	def __init__ (self, file_db = './sqlite.db'):
	#	try:
			self.conn = sqlite3.connect(file_db)
			self.curs = self.conn.cursor()
	#	except:

	def	execute (self, query, uvars = None):
		try:
			if uvars:
				self.curs.execute (query, uvars)
			else:	self.curs.execute (query)
			self.last_error = None
			return	True
		except (sqlite3.OperationalError, sqlite3.IntegrityError, sqlite3.Warning):
			self.last_error = sys.exc_info()[:2]
			return	False
		finally:
			self.conn.commit()

	def	get_row (self, query, uvars = None):
		return  self.get (query, 0)

	def	get_rows (self, query, uvars = None):
		return  self.get (query, 1)
	def     get (self, query, fall, uvars = None):
		self.last_error = None
		try:
			if uvars:
				self.curs.execute (query, uvars)
			else:	self.curs.execute (query)
			self.desc = [f[0] for f in self.curs.description]
			if fall:	return  self.curs.fetchall()
			else:		return  self.curs.fetchone()
		except (sqlite3.OperationalError, sqlite3.Warning):
			print ('except:', query)
			self.last_error = sys.exc_info()[:2]
		finally:
			self.conn.commit()

	def	close(self):
		self.conn.close()

	def	get_table (self, tname, swhere = None, cols = None):
		""" Читать таблицу из БД "SELECT {*|<cols>} FROM <tname> [WHERE <swhere>];"	"""
		if not cols:	cols = '*'
		if not swhere:
			query = "SELECT %s FROM %s;" % (cols, tname)
		else:	query = "SELECT %s FROM %s WHERE %s;" % (cols, tname, swhere)
		self.rows = self.get_rows (query)
		if self.rows:	return	self.desc, self.rows


if __name__ == '__main__':
	sqls = ["""CREATE TABLE whosts (
	id_wh INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	host_name TEXT NOT NULL
	)""",
	"INSERT INTO whosts (host_name) VALUES ('wialon.rnc52.ru')",
	"INSERT INTO whosts (host_name) VALUES ('pp-wialon.rnc52.ru')",
	"INSERT INTO whosts (host_name) VALUES ('sh-wialon.rnc52.ru')",
	"INSERT INTO whosts (host_name) VALUES ('smp-wialon.rnc52.ru')",
	"INSERT INTO whosts (host_name) VALUES ('test-wialon.rnc52.ru')",
	'CREATE TABLE whusers (\n id_whu INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\n login TEXT NOT NULL,\n passwd TEXT,\n token TEXT,\n token_create INTEGER\n)',
	"INSERT INTO whusers (login, token) VALUES ('wialon', '1d5a4a6ab2bde440204e6bd1d53b3af82FD7F6B064E042FBBCC978E2B37A2A95930F80E6')",
	"INSERT INTO whusers (login, token) VALUES ('V.Smirnov', 'c5a76d06f77af04aa4c9fa0699d465c299B67214D257083C5E790742520C44F9EA0E3D80')",
	]
	lite = dblite(r'data/config.db')
	for s in sqls:
		print (s, lite.execute(s))
	print ('get_row', lite.get_row("SELECT * FROM whosts WHERE id_wh = 1;"))
	lite.execute("update whusers SET token = '1d5a4a6ab2bde440204e6bd1d53b3af88083648F594E6BCA5E6CB70EF1F85D7BF1B79E51', token_create = 1515073900 WHERE id_whu != 2;")
	print ('get_row', lite.get_row("SELECT * FROM whusers WHERE id_whu = 1;"))
	lite.execute ("update whusers SET token = 'c5a76d06f77af04aa4c9fa0699d465c2AC7861F24C072495DD635404BDF84C5327051EBF', token_create = 1515075038 WHERE id_whu = 2;")
	print ('last_error', lite.last_error)
	print ('get_row', lite.get_row("SELECT * FROM whusers WHERE id_whu = 2;"))
#	print (help(sqlite3))
