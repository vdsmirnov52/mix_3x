#!/usr/bin/env python


import sys, os
import math, time, calendar
from openpyxl import Workbook
import dbtools3 as dbt
import sph

ids_db = {
	# 'vms':	'host=10.40.25.176 dbname=vms_ws port=5432 user=vms',
	'vms': 'host=10.10.2.147 dbname=vms_ws port=5432 user=vms',  # DBMS-Test:
	# 'cntr':	'host=212.193.103.20 dbname=contracts port=5432 user=smirnov',
	'cntr': 'host=10.10.2.241 dbname=contracts port=5432 user=smirnov',  # Houm
}
qegts_orgs = """SELECT id_org, s.label, inn, kpp, ogrn, bank, bname, rname FROM vorganizations o, subsys s WHERE bm_ssys = s.code
	AND id_org IN (
	SELECT id_org FROM wtransports WHERE id_ts IN (
	SELECT autos FROM atts WHERE mark LIKE '%EGTS%прибор%' AND autos IN (
	SELECT id_ts FROM wtransports)) GROUP BY id_org);
	"""
qvms_ts = 	"""SELECT nd.id, imei, nd.code, nd.name, nd.phone, nd.phone2, 
	nd.operator1_id, nd.operator2_id, nd.icc_id1, nd.icc_id2, nd.devicetype_id,
	nt.description as marka, t2d.id as t2d_id, atl.transport_id, atl.begindate, t.regnum, t.garagenum, t.contractnumber,
	t.transporttype_id, t.yearofcar, t.category_id, t.vinnumber, t.ptsnumber, t.ptsdate, t.registrationdate, t.registrationnumber,
	tt.code AS tmarka, tt.description AS tmodele, co.inn, ss.code AS sscode, tg.code AS tgroup
	FROM navigationdevice nd
	INNER JOIN transport2devicelink t2d on t2d.device_id=nd.id
	INNER JOIN abstracttransportlink atl on atl.id=t2d.id AND atl.isdeleted = 0 AND atl.enddate IS NULL
	INNER JOIN transport t on atl.transport_id=t.id
	INNER JOIN subsystem ss ON ss.id = t.subsystem_id
	INNER JOIN transporttype tt ON tt.id = t.transporttype_id
	INNER JOIN navigationdevicetype nt ON nt.id = nd.devicetype_id
	LEFT JOIN transportgroup tg ON tg.id = t.group_id
	LEFT JOIN contractor co ON co.id = t.owner_id
	WHERE nd.isdeleted=0 %s %s
	"""

IDB_VMS = None		# Id DataBase vms_ws
IDB_CNTR = None		# Id DataBase contracts
Rz = (6378245.0+6356863.019)/2	# Радиус земли (м)

def	colab (j):
	xls_columns = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	k = j // len(xls_columns)
	if k == 0:		return xls_columns[j]
	return colab(k-1)+colab(j%len(xls_columns))


def	cntr_get_org (inn):
	global IDB_CNTR
	if not IDB_CNTR:	IDB_CNTR = dbt.dbtools(ids_db['cntr'])
	query = "SELECT bname FROM organizations WHERE inn = %s" % inn
	row = IDB_CNTR.get_row(query)
	return row[0] if row else "???"

def	vms_get_ts (**kwargs):
	global IDB_VMS
	nddata = kwargs.get('nddata') if kwargs.get('nddata') else "nddata_202004"
	fname = kwargs.get('fname') if kwargs.get('fname') else 'nddata_202004'
	where = "AND %s" % kwargs.get('where') if kwargs.get('where') else ''
	order = "ORDER BY %s" % kwargs.get('order') if kwargs.get('order') else ''
	query = qvms_ts % (where, order)
	# print (query)
	IDB_VMS = dbt.dbtools(ids_db['vms'])
	rows = IDB_VMS.get_rows(query)
	d = IDB_VMS.desc
	inn = 0
	# Создать рабочую книгу в Excel:
	wb = Workbook()
	sheet = wb.active
	cnames = ['№ п.п', 'Гос.№', 'Марка ТС', 'Марка прибора', 'Т час', 'S км',]	# 'V пр.', 'V р.']
	cwidth = [None, 14, 14, 14, None, None, None, None, None, None]
	for j in range(len(cnames)):
		sheet[colab(j) +'2'] = cnames[j]
		if cwidth[j]:	sheet.column_dimensions[colab(j)].width = cwidth[j]
	for j in range(31):
		sheet.column_dimensions[colab(6+j)].width = 5
		sheet[colab(6+j) +'2'] = str(j+1)
	jr = 2
	ja = 0
	mndays = []
	hourss = SS = 0
	for r in rows:
		stt = vms_calc_time(nddata, r[d.index('id')])
		if inn != r[d.index('inn')]:
			if TEST:	print("%s\t%s" % (r[d.index('inn')], cntr_get_org(r[d.index('inn')])))
			inn = r[d.index('inn')]
			jr += 1
			sheet[colab(0) + str(jr)] = inn
			sheet[colab(1) + str(jr)] = cntr_get_org(inn)
			ja = 0

		if TEST:	print ("\t%s\t%s\t%s" % (r[d.index('regnum')],  r[d.index('tmarka')], r[d.index('marka')]), end='\t')
		jr += 1
		sheet[colab(1) + str(jr)] = r[d.index('regnum')]
		sheet[colab(2) + str(jr)] = r[d.index('tmarka')]
		sheet[colab(3) + str(jr)] = r[d.index('marka')]
		ja += 1
		sheet[colab(0) + str(jr)] = ja
		if stt:
			H = stt['dt']//3600
			M = (stt['dt']-H*3600)//60
			s = stt['S']/1000	# ???
			Vs = stt['S']*3.6/stt['dt'] if stt['dt'] else 0.0
			if stt['vp'] < 6. or Vs < 6.:
				if TEST:    print('$$$')
				continue
			hourss += stt['dt']
			SS += stt['S']
			# print(r[d.index('regnum')])
			if TEST:	print("\t%4d:%02d\tS:%9.3f\tVs:%6.2f\tVp:%6.2f" % (H, M, s, Vs, stt['vp']), end='\t')
			# print(stt['d'])
			sheet[colab(4) + str(jr)] = stt['dt']//3600
			# sheet[colab(5) + str(jr)] = "%4d.%02d" % (H, M)
			sheet[colab(5) + str(jr)] = int(s)		# float("%9.3f" % s)
			days = stt.get('d')
			if days:
				mndays.append(days)
				for jd in range(days[0]):
					# print(days[0], len(days), jd+1, days[jd+1])
					sheet[colab(6+jd) + str(jr)] = days[jd+1]//1000
			# sheet[colab(7) + str(jr)] = int(stt['vp'])	# float("%6.2f" % stt['vp'])
			# sheet[colab(8) + str(jr)] = int(Vs)		# float("%6.2f" % Vs)
			# sheet[colab(0) + str(jr)] = ja
		else:	pass
		if TEST:	print()
	try:
		day = mndays[0][0]
		adays = [0]*day
		for day in range(mndays[0][0]):
			for a in range(len(mndays)):
				if mndays[a][day +1] > 4000:	adays[day] += 1
		autos = 0
		for j in range(mndays[0][0]):
			sheet[colab(6+j) +str(jr+1)] = adays[j]
			autos += adays[j]
		# print(adays, autos/day)

		wb.save(os.path.join(r'./tmp', fname +".xlsx"))
		return hourss, (SS/1000), autos
	except IndexError:
		wb.save(os.path.join(r'./tmp', fname +".xlsx"))
		print("EXCEPT IndexError:", mndays)

def	vms_calc_time (nddate, devid):
	"""	Запрашивают следующее:
	Количество часов работы подвижного состава за март, апрель, май 2020 по предприятию.
	Среднедневной выпукт ТС по предприятию за месяц и пробег по предприятию за месяц.
		1. это как я понимаю время работы прибора при движении ТС
		2. это сумма количества ТС с пробегом более нескольких км в день / количество дней в месяце по предприятию
	"""
	# query = "SELECT * FROM %s WHERE deviceid = %s LIMIT 3" % (nddate, devid)
	query = "SELECT EXTRACT(EPOCH FROM createddatetime), lat, lon, speed, direction, deviceid, gpssatcount, gsmsignallevel  " \
			"FROM %s WHERE deviceid = %s ORDER BY createddatetime" % (nddate, devid)
	rows = IDB_VMS.get_rows(query)
	if not rows:	return
	d = IDB_VMS.desc
	# print(d)
	days =  [0]*32
	stt = {'c': 0, 'dt': 0, 'S': 0.0, 'vp': 0.0, }
	t0 = x0 = y0 = 0.0
	dist0 = 0
	for r in rows:
		stt['c'] += 1
		jt, y, x, v = r[:4]
		year, mnth, day = time.localtime(jt)[:3]
		if v > 0.0:	stt['vp'] += v
		if t0 and jt - t0 < 5*60:
			dt = int(jt - t0)
			dy = (y - y0)
			dx = (x - x0)*math.cos(math.pi*y/180)
			dist = int(Rz * math.sqrt(dx ** 2 + dy ** 2) * math.pi / 180)
			# dist, azi1 = sph.inverse(y0, x0, y, x)
			# stt['S'] += dist * sph.Rz
			if (dist0 + dist) / 2 > 0.01:	# Фильтрация коротких остановок при расчете Времени работы
			# if (dist0 + dist) / 2 > 0.00000001:	# Фильтрация коротких остановок при расчете Времени работы + ~ 30%
				stt['dt'] += dt
			dist0 = (dist0 + dist)/2
			if dist:	# Есть движение ТС
				days[day] += dist
				stt['S'] += dist
			# else:	print("dist", dist, "dt", dt, (math.fabs(dx), math.fabs(dy), (dist0+dist)/2))
			t0 = jt
			x0 = x
			y0 = y
		elif t0 == 0.0 and jt:
			t0 = jt
			x0 = x
			y0 = y
		else:
			t0 = 0.0
		# print(jt, y, x, end='\r')
	# print ()
	days[0] = calendar.monthrange(year, mnth)[1]
	stt['d'] = days
	if stt['vp'] > 0.0:	stt['vp'] /= stt['c']
	return stt

import dblite3
IDBLite	= None

def	init_dbreport ():
	global IDBLite

	dbl_name = r'/home/smirnov/DATAs/reports.db'
	query = "CREATE TABLE report_subs (inn INTEGER NOT NULL PRIMARY KEY," \
			" bname varchar(265), car INTEGER, H integer, S FLOAT, A integer, year integer, month integer);"
	init = False if os.path.isfile(dbl_name) else True
	IDBLite = dblite3.dblite(dbl_name)
	if init:
		print(query, IDBLite.execute(query))
		# read_statiastic()

def	read_statiastic(fname =r'tmp/dd202003_ALL.csv'):
	""" Пополнение статистики в report_subs	"""
	f = open(fname, 'r')
	line = 'qwe'
	while line:
		line = f.readline().strip()
		if line[:10].isdigit():
			vals = line.split('\t')
			inn, bname, car, H, S, A = vals[:6]
			query = "INSERT INTO report_subs (inn, bname, car, H, S, A, year, month) VALUES (%s, '%s', %s, %s, %s, %s, %s, %s)" % (
				inn, bname, car, H, S, A, 2020, 3)
			if IDBLite.execute(query) == False:
				print(IDBLite.last_error)
				break

def	org_list(ssys):
	""" Список организацие по подсистеме	"""
	global IDB_CNTR, IDBLite

	if not IDBLite:		init_dbreport()
	rrows = IDBLite.get_rows("SELECT inn FROM report_subs")
	inns = []
	for rr in rrows:		inns.append(rr[0])

	if not IDB_CNTR:	IDB_CNTR = dbt.dbtools(ids_db['cntr'])
	# query = "SELECT id_org, inn, bname FROM organizations WHERE bm_ssys = %s AND inn IN (5223034394, 5243019838, 5206024886) ORDER BY bname" % ssys
	query = "SELECT id_org, inn, bname FROM organizations WHERE bm_ssys = %s AND inn NOT IN (%s) ORDER BY bname" % (
		ssys, str(inns)[1:-1] if inns else '123')
	# print(query)
	rows = IDB_CNTR.get_rows(query)
	year = 2020
	month = 5
	ja = 0
	for r in rows:
		id_org, inn, bname = r
		# print (id_org, inn, bname)
		qurepp = None
		car = IDB_CNTR.get_row("SELECT count(*)  FROM wtransports WHERE id_org = %d" % id_org)
		print(id_org, inn, bname, car[0], time.strftime("%D %T", time.localtime(time.time())), sep='\t')
		if car and car[0] > 0:
			res = vms_get_ts(where="inn = '%s'" % inn, nddata="nddata_202005", fname='dd202005_%d' % inn)
			if res:
				H, S, A = res
				print(inn, bname, car[0], H/1000, S, A, sep='\t')
				qurepp = "INSERT INTO report_subs (inn, bname, car, H, S, A, year, month) VALUES (%s, '%s', %s, %s, %s, %s, %s, %s)" % (inn, bname, car[0], H, S, A, year, month)
			else:
				qurepp = "INSERT INTO report_subs (inn, bname, car, year, month) VALUES (%s, '%s', %s, %s, %s)" % (inn, bname, car[0], year, month)
				print(inn, bname, car[0], res, sep='\t')
		else:
			qurepp = "INSERT INTO report_subs (inn, bname, car) VALUES (%s, '%s', 0)" % (inn, bname)
		if qurepp:
			print(qurepp, IDBLite.execute(qurepp))
		ja += 1
		if ja > 222:	break


TEST = False
def	test_vms_get_ts ():
	global TEST
	TEST = True
	inns = [#5213005154, # МП "Гагинское ПАП"
			# 5223034394, # МП "Автостанция" г.Навашино
			# 5243019838,	# МУП "АПАТ"
			5206024886,	# МП "Вадское ПАП"
			# 524701328820,	# ИП Копейкин ЕВ
			]
	for inn in inns:
		print(inn, sep='\t', end='\t')
		res = vms_get_ts(where="inn = '%s'" % inn, order="regnum", nddata="nddata_202004", fname='dd202004_%d' % inn)
		print(res, res[0]//3600, res[2]/31)

def	repport_out(year, month):
	""" Выгрузить сводный рапорт и report_subs	"""
	global IDBLite
	if not IDBLite:		init_dbreport()
	rrows = IDBLite.get_rows("SELECT * FROM report_subs WHERE year = %s AND month = %s ORDER BY bname" % (year, month))
	if not rrows:
		print("Нет данных", IDBLite.last_error)
		return
	fout = "tmp/repport_out_%02d%d.xlsx" % (month, year)
	d = IDBLite.desc
	# Создать рабочую книгу в Excel:
	wb = Workbook()
	sheet = wb.active
	sheet['A1'] = "%02d.%d" % (month, year)
	mnday = calendar.monthrange(year, month)[1]
	cnames = ['ИНН', 'Название организации', 'К-во ТС', 'Время работы час', 'Пройденый путь км', 'Выпуск ТС', 'ТС в день']
	cwidth = [14, 24, None, 12, 12, None, None, None]
	for j in range(len(cnames)):
		sheet[colab(j) +'2'] = cnames[j]
		if cwidth[j]:	sheet.column_dimensions[colab(j)].width = cwidth[j]
	jr = 2
	for rr in rrows:
		jr += 1
		sjr = str(jr)
		for j in range(len(d) -2):
			if not rr[j]:	continue
			if d[j] == 'A':	# and rr[j] and rr[j] > 0:
				aa = rr[j]/mnday
				sheet[colab(j) +sjr] = rr[j]
				sheet[colab(j+1) +sjr] = float("%5.2f" % aa)
			elif d[j] == 'H':	# and rr[j] and rr[j] > 0;
				sheet[colab(j) +sjr] = rr[j]//3600
			else:
				sheet[colab(j) + sjr] = rr[j]
		# print(rr, aa)
	wb.save(fout)
	print("Create", fout)

def	isconnect():
	print("Connect to DataBase:")
	for ddb in ids_db:
		dbi = dbt.dbtools(ids_db[ddb])
		print("\t%s:\t" % ddb, ids_db[ddb], 'Ok' if dbi and not dbi.last_error  else 'Error')

if __name__ == '__main__':
	# init_dbreport()
	# test_vms_get_ts()
	org_list(2)
	repport_out(2020, 5)
	# isconnect()
	# vms_get_ts(where="regnum LIKE 'А%' AND ss.code = 'ПП' ", order="inn, regnum LIMIT 11")
