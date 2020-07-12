#!/usr/bin/env python


import os, sys
from openpyxl import Workbook
from datetime import datetime
import dbtools3 as dbt

ids_db = {
		'b03':	'host=127.0.0.1 dbname=b03 port=5432 user=smirnov',
	#	'vms':	'host=10.40.25.176 dbname=vms_ws port=5432 user=vms',
		'vms':	'host=10.10.2.147 dbname=vms_ws port=5432 user=vms',	# DBMS-Test:
		# 'cntr':	'host=212.193.103.20 dbname=contracts port=5432 user=smirnov',
		'cntr':	'host=10.10.2.241 dbname=contracts port=5432 user=smirnov',	# Houm
	}
#	cat /proc/sys/kernel/hostname
#	psql -h 212.193.103.20 -U smirnov contracts	'%EGTS%прибор%'
qegts_orgs = """SELECT id_org, s.label, inn, kpp, ogrn, bank, bname, rname FROM vorganizations o, subsys s WHERE bm_ssys = s.code
	AND id_org IN (
	SELECT id_org FROM wtransports WHERE id_ts IN (
	SELECT autos FROM atts WHERE mark LIKE '%EGTS%прибор%' AND autos IN (
	SELECT id_ts FROM wtransports)) GROUP BY id_org);
	"""
qvms_ts = 	"""SELECT nd.id, imei, nd.code, nd.name, nd.phone, nd.phone2, 
	nd.operator1_id, nd.operator2_id, nd.icc_id1, nd.icc_id2, nd.devicetype_id,	--yu
	nt.description as marka, t2d.id as t2d_id, atl.transport_id, atl.begindate, t.regnum, t.garagenum, t.contractnumber,
	t.transporttype_id, t.yearofcar, t.category_id, t.vinnumber, t.ptsnumber, t.ptsdate, t.registrationdate, t.registrationnumber, --yu
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
	WHERE nd.isdeleted=0 %s
	ORDER BY contractnumber	-- nd.id
	"""
operators = {
	4026234739: 'МТС', 4069024372: 'Мегафон', 4069024373: 'Билайн', 4309109521: 'НСС',
	4501847583: 'Ростелеком', 5246673376: 'Tele2', 10877759658: 'ГудЛайн',
	}
orecv_dict = {
	'02-20/ОП': "Смотр", '03-20/ОП': "Ниса", '04-20/ОП': "Артикул", '05-20/ОП': "Меркурий НН",
	'06-20/ОП': "Сантел сервис", '07-20/ОП':"Навигационные технологии",
	'08-20/ОП': "Технотрейд НН", '09-20/ОП': "Вектор", '11-20/ОП': "Навигационные решения",
}
idorg_dict = {	'02-20/ОП': 513, '03-20/ОП': 687, '04-20/ОП': 682, '05-20/ОП': 589, '06-20/ОП': 686, '07-20/ОП': 851, '08-20/ОП': 913, }

ssys_dict = {
	1: 'Тест БО', 2: 'СМУ ПП', 4: 'СМУ ША', 8: 'СМ ЖКХ', 16: 'СМ СОГ', 32: 'СМУ ОВ', 64: 'ЖКХ-М', 128: 'СМП', 256: 'ЧТС',
	2048: 'ПП ЦДС', 4096: 'ПП НПАП', 8192: 'ВТ', 16384: 'СХ', 32768: 'ЛХ', 65536: 'МСПОРТ', 131072: 'ДТ-НН', 262144: 'ДТ-НО', 524288: 'КТ',
}
IDB_VMS = None		# Id DataBase vms_ws
IDB_CNTR = None		# Id DataBase contracts

work_directory = r'./tmp/'
# data_directory = r'./data/'


def colab(j):
	""" Формировать сод колнки по индексу    """
	xls_columns = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	k = j // len(xls_columns)
	if k == 0:		return xls_columns[j]
	return colab(k-1)+colab(j%len(xls_columns))


def data2xlsx(tname, head:list, values, fname = None, cwidth:dict = None):
	""" Фпрмироать *.xlsx файл
	:param tname - 	sheet.title
	:param heas - 	наименование колонок [['A1', 'COD'], ['B1', 'NAME'], ...]
	:param values -	данные [[r0, r1, ...], [r...], ...] или ((r0, r1, ...), (r, ...), ...)
	:param fname -	имя файла + dt.strftime("_%Y%m%d_%H%M%S") + '.xlsx'
	:param cwidth -	ширина колонок {'A': 32, 'B': 32, 'D': 20, 'E': 32, ...}
	"""
	dt = datetime.now()

	# Создать рабочую книгу в Excel:
	wb = Workbook()
	sheet = wb.active
	sheet.title =  tname if tname else 'data'
	# Добавить заголовки в рабочую книгу Excel:
	for k, name in head:
		sheet[k] = name
	if cwidth:
		for k in cwidth.keys():
			sheet.column_dimensions[k].width = cwidth[k]
	row = 1		# Заголовок 1 строка !!!
	# Заполнить данными
	for item in values:
		row += 1
		for j in range(len(item)):
			sheet[colab(j)+str(row)] = item[j]
	# Второй лист	???
	# Сохранить файл:
	filename = (fname if fname else 'data') + dt.strftime("_%Y%m%d_%H%M%S") + '.xlsx'
	wb.save(os.path.join(work_directory, filename))

#########################################

def	table_to_xlsx (sidb, tname, query, fname = None):
	""" Конвертировать таблицу в *.xlsx файл	"""
	idb = dbt.dbtools(ids_db[sidb])	# 'host=127.0.0.1 dbname=b03 port=5432 user=smirnov')
	rows = idb.get_rows(query)
	if not rows:	return
	d = idb.desc
	heads = []
	for j in range(len(d)):
		heads.append([colab(j)+'1', d[j].upper()])
	print(heads)
	data2xlsx(tname, heads, rows, fname, cwidth={'A': 14, 'B': 14, 'C': 14, 'D': 14, 'E': 14, 'F': 14})


def	vms_handbks ():
	""" Выгрузить справочники	"""
	tab_list = ['transporttype', 'navigationdevicetype', 'cellularoperator']
	for tn in tab_list:
	#	table_to_xlsx('vms', tn, "SELECT * FROM %s WHERE id >0 ORDER BY id" % tn, tn)
		table_to_xlsx('vms', tn, "SELECT * FROM %s WHERE isdeleted = 0 ORDER BY id" % tn, tn)


org_hdesc = [
	{'c': 'name',	'w': 32, 'ru':		'краткое наименование'},
	{'c': 'fullname',	'w': 32, 'ru':	'полное наименование'},
	{'c': 'ogrn',	'w': 20, 'ru': 'ОГРН'},
	{'c': 'inn',	'w': 20, 'ru': 'ИНН'},
	{'c': 'legalAddress',	'w': 32, 'ru': 'юридический адрес'},
	{'c': 'postAdress',	'w': 32, 'ru': 'фактический адрес'},
	{'c': 'phone', 'ru': 'контактный телефон'},
	{'c': 'priorGroupName',	'w': 22, 'ru': 'приоритетная группа пользователя'},
	{'c': 'regionName',	'w': 22, 'ru': 'район'},
	{'c': 'subSystemCode',	'ru': 'подсистема'},]

def	orgs2xlsx (**kwargs):
	""" Поиск организаций и формирование *.xlsx файла	"""
	xls_columns = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	if kwargs.keys():
		print(kwargs)
		fname = kwargs['fout'] if kwargs.get('fout') else 'organizations'
		IDB_CNTR = dbt.dbtools(ids_db['cntr'])
		inn_list = kwargs.get('inns')
		if not inn_list and 'swhere' in kwargs.keys():
			query = "SELECT inn FROM organizations WHERE " +kwargs.get('swhere')
			print(query)
			rows = IDB_CNTR.get_rows(query)
			if rows:
				inn_list = []
				for r in rows:	inn_list.append(r[0])

		if inn_list:
			# fname = kwargs.get('fout') if 'fout' in kwargs.keys() else 'organizations'
			colms = []
			heads = []
			cwidth = {}
			for j in range(len(org_hdesc)):
				# print(org_hdesc[j])
				colms.append(org_hdesc[j].get('c'))
				heads.append([xls_columns[j] +'1', org_hdesc[j].get('ru')])
				cwidth[xls_columns[j]] = org_hdesc[j]['w'] if org_hdesc[j].get('w') else 12
			rows = []
			print(fname, heads, cwidth)

			for inn in inn_list:
				dr = get_org_descript(inn, fout=False)
				r = []
				for j in range(len(org_hdesc)):
					r.append(dr.get(colms[j]))
				rows.append(r)
				# print(r)
			data2xlsx(fname, heads, rows, fname, cwidth=cwidth)
		else:	print("orgs2xlsx Нет inn_list")
	else:	print("orgs2xlsx Нет аргуметов")

def	get_org_descript (inn, fout=True):
	""" Читать описание организации по ИНН в БД contracts  """
	global	IDB_VMS, IDB_CNTR
	dorg = {}
	if not IDB_CNTR:	IDB_CNTR = dbt.dbtools(ids_db['cntr'])
	# Contracts
	query = "SELECT * FROM vorganizations WHERE inn = %s" % inn
	drow = IDB_CNTR.get_dict(query)
	if fout:	print(inn, '='*33)
	for k in drow.keys():
		if k in ['id_org', 'p_org', 'label', 'region', 'rem', 'bl_mail']:	continue
		if drow[k]:
			if k == 'fname':	dorg['fullname'] = drow[k]
			elif k == 'bname':	dorg['name'] = drow[k]
			elif k == 'ogrn':	dorg['ogrn'] = drow[k]
			elif k == 'inn':	dorg['inn'] = drow[k]
			elif k == 'rname':	dorg['regionName'] = drow[k]
			elif k == 'bm_ssys':	dorg['subSystemCode'] = "%s %s" % (drow[k], ssys_dict.get(drow[k]))
			elif k == 'prior_group_name':	dorg['priorGroupName'] = drow[k]
			elif k == 'ur_addr':	dorg['legalAddress'] = drow[k]
			elif k == 'fact_addr':	dorg['postAdress'] = drow[k]
			elif k == 'phone':	dorg['phone'] = drow[k]
		#	elif k == '':	dorg[''] = drow[k]
			# else:	print("%16s" % k, drow[k])
	if fout and dorg:
		print('#'*22)
		for k in dorg.keys():
			print('%22s:' % k, dorg[k])
	return dorg

def	vms_find_orgs (qand=None):
	""" Поиск организаций по № договора (contractnumber) БД vms_ws	"""
	global	IDB_VMS, IDB_CNTR

	query = qvms_ts % qand
	# print(query)
	if not IDB_CNTR:	IDB_CNTR = dbt.dbtools(ids_db['cntr'])
	if not IDB_VMS:		IDB_VMS = dbt.dbtools(ids_db['vms'])
	rows = IDB_VMS.get_rows(query)
	d = IDB_VMS.desc
	devids = []
	cnumber = ''
	for r in rows:
		if r[d.index('contractnumber')] != cnumber:
			if devids:
				""" Установать связь ТС с организацией ретранслируещей данные id_opr  ""
				qup_id_opr = "UPDATE transports SET id_opr = %s WHERE device_id IN (%s)" % (
					idorg_dict[cnumber] if idorg_dict.get(cnumber) else -1,
					str(devids)[1:-1])
				print(cnumber, orecv_dict.get(cnumber), IDB_CNTR.execute(qup_id_opr))    #, devids)
				"""
				print(cnumber, orecv_dict.get(cnumber))    #, devids)
				q = """SELECT id_org, bname, inn, bm_ssys FROM organizations WHERE id_org IN (
				SELECT id_org FROM transports WHERE device_id IN (%s) GROUP BY id_org) ORDER BY bm_ssys""" % str(devids)[1:-1]
				rrs = IDB_CNTR.get_rows(q)
				for idorg, onmae, inn, ssys in rrs:
					print('\t', inn, "\t%8s\t%s" % (ssys_dict.get(ssys), onmae))
			devids = [r[d.index('id')]]
			cnumber = r[d.index('contractnumber')]
		else:
			devids.append(r[d.index('id')])
	if devids:
			""" Установать связь ТС с организацией ретранслируещей данные id_opr  ""
			qup_id_opr = "UPDATE transports SET id_opr = %s WHERE device_id IN (%s)" % (
				idorg_dict[cnumber] if idorg_dict.get(cnumber) else -1,
				str(devids)[1:-1])
			print(cnumber, orecv_dict.get(cnumber), IDB_CNTR.execute(qup_id_opr))    #, devids)
			"""
			print(cnumber, orecv_dict.get(cnumber), idorg_dict.get(cnumber))  #, devids)
			q = """SELECT id_org, bname, inn, bm_ssys FROM organizations WHERE id_org IN (
			SELECT id_org FROM transports WHERE device_id IN (%s) GROUP BY id_org) ORDER BY bm_ssys""" % str(devids)[1:-1]
			rrs = IDB_CNTR.get_rows(q)
			for idorg, onmae, inn, ssys in rrs:
				print('\t', inn, "\t%8s\t%s" % (ssys_dict.get(ssys), onmae))
	# print(len(rows), qand)


def	contracts_get_ts (inn=None, qand=None):
	""" Поиск ТС в БД contracts по ИНН организации или условию "AND contractnumber LIKE '%0/ОП'"	"""
	global IDB_CNTR

	query = "SELECT id_ts, garnum, gosnum, marka, t.modele, yearofcar, ptsnumber, ptsdate, registrationnumber, registrationdate, vinnumber, uin, sim_1, sim_2, last_date " \
			"FROM transports t JOIN atts a ON t.id_ts = a.autos " \
			"WHERE id_org IN (SELECT id_org FROM organizations WHERE inn = %s) LIMIT 5" % inn
	print(query)
	if not IDB_CNTR:	IDB_CNTR = dbt.dbtools(ids_db['cntr'])
	rows = IDB_CNTR.get_rows(query)
	d = IDB_CNTR.desc
	print(d)
	heads = []
	lines = []
	cwidth = {}
	for j in range(len(ttsc_order)):
		# print(ttsc_order[j])	#, type(rows[0][d.index(ttsc_order[j])]), rows[0][d.index(ttsc_order[j])])
		cname = ttsc_desc.get(ttsc_order[j])
		# if not cname:    cname = d[j].upper()
		if colab(j) in 'CDEGHX':
			cwidth[colab(j)] = 24
		elif colab(j) in 'UVY':
			cwidth[colab(j)] = 16
		else:
			cwidth[colab(j)] = 12
		heads.append([colab(j) + '1', cname])
		# print(cname)
		'''
		# print(ttsc_order[j], cname)
		v = rows[0][d.index(j)]
		if type(v) == str:
			cwidth[colab(j)] = 12 if len(v) < 12 else 24
		elif type(v) == int:
			cwidth[colab(j)] = 8
		else:
			cwidth[colab(j)] = 16
		'''
	print(heads)
	print(cwidth)
	for r in rows:
		print(r)
		l = []
		l.append(r[d.index('garnum')])
		l.append(r[d.index('gosnum')])
		l.append(r[d.index('marka')])
		l.append(r[d.index('modele')])
		l.append('Navtelecom Signal S-2551')
		l.append(r[d.index('uin')])
		l.append(r[d.index('uin')])
		l.append(None)
		# l.append(r[d.index('')])
		print(l)

		lines.append(l)
	# fname = "ts_list_%s" % inn
	# data2xlsx('Транпорт', heads, lines, fname, cwidth=cwidth)


def	vms_get_ts (inn=None, qand=None):
	""" Поиск ТС в БД vms_ws по ИНН организации или условию "AND contractnumber LIKE '%0/ОП'"	"""
	global	IDB_VMS

	sand = "AND co.inn = '%s'" % inn if inn else ''
	if qand:	sand += qand

	query = qvms_ts % sand
	# print(query)
	if not IDB_VMS:
		IDB_VMS = dbt.dbtools(ids_db['vms'])
	rows = IDB_VMS.get_rows(query)
	if not rows:    return
	
	d = IDB_VMS.desc
	print(d)
	heads = []
	lines = []
	cwidth = {}
	for j in range(len(ttsc_order)):
		# print(ttsc_order[j], type(rows[0][d.index(ttsc_order[j])]), rows[0][d.index(ttsc_order[j])])
		cname = ttsc_desc.get(ttsc_order[j])
		if not cname:	cname = ttsc_order[j].upper()
		# print(ttsc_order[j], cname)
		heads.append([colab(j) + '1', cname])
		v = rows[0][d.index(ttsc_order[j])]
		if type(v) == str:			cwidth[colab(j)] = 12 if len(v) < 12 else 24
		elif type(v) == int:		cwidth[colab(j)] = 8
		else:			cwidth[colab(j)] = 16
	# print(cwidth)
	dsuffix = ['16/М', '17/М', '18/М']
	for r in rows:
		l = []
		for k in ttsc_order:
			if 'date' in k and r[d.index(k)]:
				l.append(str(r[d.index(k)]).split(' ')[0])
			elif 'operator' in k and r[d.index(k)]:
				l.append(operators.get(r[d.index(k)]))
			elif 'contractnumber' == k and r[d.index(k)] and len(r[d.index(k)]) > 5 and r[d.index(k)][-4:] in dsuffix:
				l.append(r[d.index(k)][:-4] +'20/Л')
			# elif 'vinnumber' == k and r[d.index(k)] and len(r[d.index(k)]) != 17:		l.append(None)
			else:
				l.append(r[d.index(k)])
		l.append(get_last_date(r[d.index('id')], r[d.index('regnum')]))
		lines.append(l)
	fname = "ts_list_%s" % inn
	data2xlsx('Транпорт', heads, lines, fname, cwidth=cwidth)
	# print(heads)
	# print(l)
#	table_to_xlsx('vms', str(inn), query, str(inn))


def	get_last_date(devid, gosnum):
	""" Читать дате последней работы ТС  """
	global IDB_CNTR
	if not IDB_CNTR:	IDB_CNTR = dbt.dbtools(ids_db['cntr'])
	query = "SELECT device_id, gosnum, last_date, bm_wtime FROM wtransports WHERE device_id = %s AND gosnum = '%s'" % (devid, gosnum)
	wad = IDB_CNTR.get_dict(query)
	if wad and wad.get('last_date'):
		date = str(wad.get('last_date'))[:10]
		return date

ttsc_order = [
	'garagenum', 'regnum', 'tmarka', 'tmodele', 'marka', 'imei', 'code', 'name', 'contractnumber', 'yearofcar',	# 'category_id', 'id',
	'operator1_id', 'phone', 'icc_id1',
	'operator2_id', 'phone2', 'icc_id2',
	'vinnumber', 'ptsnumber', 'ptsdate', 'registrationnumber', 'registrationdate', 'begindate',
	'sscode', 'tgroup']

ttsc_desc = {
	'garagenum': "Гараж. номер", 'regnum': "Гос. номер", 'tmarka': "Марка ТС", 'tmodele': "Модель ТС",
	'imei': "IMEI БНСО", 'code': "Код БНСО", 'name': "Тип БНСО", 'marka': "Марка БНСО",
	'yearofcar': "Год изготовления ТС", 'vinnumber': "VIN", 'ptsnumber': "Серия и номер ПТС", 'ptsdate': "Дата выдачи ПТС",
	'registrationnumber': "№ рег. свидетельства", 'registrationdate': "Дата рег. свидетельства", 'begindate': "Дата подключения ТС",
	'sscode': "Подсистема", 'contractnumber': "№ договора",
	'operator1_id': "Оператор 1", 'phone': "Телефон 1", 'icc_id1': "№ SIM карты 1",
	'operator2_id': "Оператор 2", 'phone2': "Телефон 2", 'icc_id2': "№ SIM карты 2",
	}

if __name__ == '__main__':
	#[5262311940, 5250068811, 5246045628пщ 	# for j in range(55):		print(colab(j))
	# table_to_xlsx('b03', 'Список пользователей', "SELECT * FROM vperson_sp ORDER BY name LIMIT 16", 'vperson_sp')
	# table_to_xlsx('cntr', 'EGTS прибор', qegts_orgs, 'egtsp_orgs')
	# vms_handbks()
	# for inn in [5249057251, 5247048220, 5254000797, 524500120268, 524504481339, 524900992249, 524908186307]:
	# 	get_org_descript(inn)
	# orgs2xlsx(inns=[5249057251, 5247048220, 5254000797, 524500120268, 524504481339, 524900992249, 524908186307])
	# 2020.05.17
	# swhere = "inn IN %s ORDER BY bm_ssys" % str(inns)
	# orgs2xlsx(swhere=swhere)	#"bm_ssys>2 ORDER BY region")
	# vms_find_orgs(qand="AND contractnumber LIKE '%0/ОП'")	# Договор ретрансляции
	# inns = (5249076222, 5249084953, 5249066619, 5249066601, 521500776400, 522401215726, 5260357227, 5260410382, 5263004131, 5258121094, 5259120142, 5263133747, 5250039673, 5215000426, 5246035429)
	# inns = [5259120142, 5249084953]
	# inns = [525405675511]	# 04-20/ОП Артикул
	# inns = [5245024880]	# 05-20/ОП Меркурий НН
	'''
	# INN Santel
	inns = [5205005513, 5248035752, 5204012814, 5205000709, 5250068811, 5261113216, 5250056020, 525100054358, 521475244896, 524701328820, 5246049830, 5217003976,
		5229009027, 5226000996, 5260381460, 5246045628, 525101406034, 525100866350, 5262311940, 526301541890, 5249050619, 5225004758, 5226000410, 5226014364,
		522100130194, 5262288057, 5246046766, 5221006585]
	# 02-20/ОП Смотр
	# inns = [524504481339, 524500120268, 524503379942, 5239002285, 5261120608]
	# 03-20/ОП Ниса
	inns = [5249057251, 5249076222, 5249084953, 5249066619, 521500776400, 5249066601, 522401215726, 5260357227, 5260410382, 5263004131,
	5258121094, 5259120142, 5263133747, 5250039673, 5215000426, 5246035429, ]
	# ООО "Технотрейд НН"   5256128168
	inns = [5256128672, 5203002940, 5256083975, 5213005154, 5244027140, 5256135013, 5256087930, 5260126942, 5232003410]
	inns = [
	524908186307,	# ИП Сафин – ЕГТС (Межсерв)
	5247048220,	# МУП "Выксунское ПАП" – ЕГТС (Межсерв)
	5239010230,	# МУП "Шахунское ПАП" – Wialon
	5254000797,	# МУП «Горавтотранс» - Саров - ЕГТС (Межсерв)
	5252035184,	# ООО "АльянсАвто" – Wialon
	5249120827,	# ООО "Тройка" – Wialon
	5252028596,	# ООО «Автотайм» - Wialon
	5249138831,	# ООО Компания тройка – Wialon
		# Серафимо-Дивеевский монастырь – Wialon
	]
	inns = [	#  2020.07.03
	525600851619,	# ИП Пазюков - ЕГТС (Межсерв)
	5257057840,	# МП "КХ" – Wialon
	5256021545,	# МП РЭД Автозаводского района - ЕГТС (Межсерв)
	5263059130,	# ООО " СПЖРТ " Володарский" - ЕГТС (Межсерв)
	5256103702,	# ООО "Автодорстрой" – Wialon
	5263087032,	# ООО "Володарский" - ЕГТС (Межсерв)
	5256077756,	# ООО "Гамма" - ЕГТС (Межсерв)
	#,	# ООО "ДПП" Нижний Новгород – Wialon
	5261104275,	# ООО "Дюф-Строй" - ЕГТС (Межсерв)
	5257163775,	# ООО "Новиком" – Wialon
	5260148174,	# ООО "Партнер-ВП" – Wialon
	5258066887,	# ООО "Ремстройком" – Wialon
	5260436976,	# ООО "Экострой" – Wialon
	#,	# ООО «АТП» ( 09-20/ОП)  - ЕГТС (Межсерв)
	5262021430,	# ООО «Русский Бизнес Концерн – РУБИКОН» - ЕГТС (Межсерв)
	5212511285,	# ООО «Строй Нижний» - ЕГТС (Межсерв)
	5252005775,	# АО "Дорожное" - W
	5223003100,	# АО Нижегородец – E
	5238000045,	# АО Строитель – W
	5247015496,	# ЗАО "Дорожная компания" – E
	5247005924,	# ЗАО "ПМК "Выксунская" - E
	5234004641,	# ООО "Амиго" – W
	5246049830,	# ООО "Борская ДПМК" – W
	5246052270,	# ООО "Борское ДРСП" – W
	#,	# ООО "Вираж" – W	5245001868, 5228006922 ??? (15 и 13 машин)
	5208005776,	# ООО "ДРСУ Навашино" – E
	5237000317,	# ООО "Магистраль" – W
	5231001106,	# ООО "ПМК "Сосновская" – W
	#,	# ООО "ЭксАвтоДор" – W
	5212511285,	# ООО «Строй Нижний» - W
	5259136738,	# ООО Дорстройнн - E
	5233003349,	# ООО ДСК Тонкинская - E
	5211760056,	# ООО Радор – E
	5257009364,	# ООО фирма "Магистраль" -E
	]
	inns = [	# 2020.07.09
	5259137080,	#ООО СМК
	5256068455,	#ООО ОРБ Нижний
	5263111359,	#ООО Автоспецмонтаж
	5261092573,	#ООО Чистый двор-НН
	5257084971,	#ООО СитиЛюкс НН
	5258122475,	#ООО СИТИЛЮКС 52
	5263127359,	#ООО Мехуборка регионы
	5260375963,	#ООО Химвелл
	5261038449,	#ООО Ремонтно-эксплуатационное предприятие 2
	7734699480,	#ООО "МСК-НТ
	]
	for inn in inns:		vms_get_ts(inn=inn)
 	'''
	contracts_get_ts(inn=5249006828)	# МУП "Экспресс" г.Дзержинск
	# vms_get_ts(inn=5243019838)	# АПАТ
	# vms_get_ts(qand="AND contractnumber LIKE '%0/ОП'")	# Договор ретрансляции
	# vms_get_ts(5247048220)	# 5247048220	МУП "Выксунское ПАП",
	# vms_get_ts(5254000797)	# 5254000797	МУП "Горавтотранс"
	# vms_get_ts(524500120268)	# 524500120268	ИП Джуга Ю.П.,
	# vms_get_ts(524504481339)	# 524504481339	ИП Куртаев А.В.,
	# vms_get_ts(524900992249)	# 524900992249	ИП Лазарев С.Ю.,
	# vms_get_ts(524908186307)	# 524908186307	ИП Сафин Х.М. и
