#!/usr/bin/env python

import  sys
import  psycopg2	#, psycopg2.extensions

class   dbtools:
	""" Работа с PostgreSQL
	Функции:
	qexecute (query)	- Исполняет запрос.     Возвращает: {True|False} УСТАРЕТШЕЕ
	execute (query)		- Исполняет запрос.     Возвращает: {True|False}
	get_row  (query)	- Читает одну запись.   Возвращает: row = (val1, val2, ...)
	get_dict (query)	- Читает одну запись.   Возвращает: dict = {key1: val1, key2: val2, ...}
	get_rows (query)	- Читает несколько записей      Возвращает: rows = [(row1), (row2), ...]
	get (query, fall)       - Исполняет запрос и читает данные. Если fall: 1 - fetchall() иначе 0 - fetchone())
	get_table (tname, [swhere], [cols])
	last_error      = (exc_type, exc_value) последняя оштбка доступа к БД
	"""
	print_error = 1	# Вывод ошибок на печать (0 - отменить печать)
	last_error =	None
	desc = []       # Список наименования полей последнего запроса
	def __init__ (self, desc_db = 'host=localhost dbname=b03 port=5432 user=vds', perror = 1):
		self.print_error = perror	# Вывод ошибок на печать
		try:
			self.conn = psycopg2.connect(desc_db)
			self.curs = self.conn.cursor()
		except: # psycopg2.OperationalError:
			exc_type, exc_value = sys.exc_info()[:2]
			self.last_error = (exc_type, exc_value)
			self.perrs ("EXCEPT __init__")


	def     qexecute (self, query):		return self.execute(query)


	def     execute (self, query):
		try:
			self.curs.execute (query)
			res = True
		except (psycopg2.OperationalError, psycopg2.ProgrammingError, psycopg2.IntegrityError):
			self.perrs ()
			res = False
		finally:
			self.conn.commit()
		if res:	last_error = None
		return  res

	def     get_rows (self, query):
		return  self.get (query, 1)

	def     get_row (self, query):
		return  self.get (query, 0)

	def     get_dict (self, query):
		r = self.get (query, 0)
		if r:
			d = {}
			for j in range (len(self.desc)):
				d[self.desc[j]] = r[j]
			return  d
		return  r

	def	get_table (self, tname, swhere = None, cols = None):
		""" Читать таблицу из БД "SELECT {*|<cols>} FROM <tname> [WHERE <swhere>];"	"""
		if not cols:	cols = '*'
		if not swhere:
			query = "SELECT %s FROM %s;" % (cols, tname)
		else:	query = "SELECT %s FROM %s WHERE %s;" % (cols, tname, swhere)
		self.rows = self.get_rows (query)
		if self.rows:	return	self.desc, self.rows

	def     get (self, query, fall):
		try:
			self.curs.execute (query)
			self.desc = [f[0] for f in self.curs.description]
			if fall:	return  self.curs.fetchall()
			else:		return  self.curs.fetchone()
		except (psycopg2.OperationalError, psycopg2.ProgrammingError, psycopg2.IntegrityError):
			self.perrs ()
		finally:
			self.conn.commit()

	def     perrs (self, label = 'EXCEPT'):
		exc_type, exc_value = sys.exc_info()[:2]
		self.last_error = (exc_type, exc_value)
		if self.print_error:
			print (label, exc_type, exc_value)

if __name__ == '__main__':
	print	("""Test connections:
	'host=127.0.0.1 dbname=b03 port=5432 user=smirnov'
	'host=10.40.25.176 dbname=vms_ws port=5432 user=vms'
	'host=212.193.103.20 dbname=worktime port=5432 user=smirnov'
	'host=10.10.2.40 dbname=test port=5432 user=smirnov'
	""")
	if len (sys.argv) > 1:
		print ('\tConnect to', sys.argv[1], end='\t')
		ddb = dbtools(sys.argv[1], 0)
		print ('Ok' if ddb and not ddb.last_error else ddb.last_error)

