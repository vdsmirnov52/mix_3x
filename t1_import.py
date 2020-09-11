#!/usr/bin/env python

import	os, sys, time, getopt
import	dbtools3
import	requests


def	get_allts (turl = None):
	""" Читать список ТС 	"""
	if turl in ['actv', 'updt']:
		url = 'http://10.10.21.20:8000/v.1/messages/all/8d6fbe03e99f4b13966e622981c9a11f'	# выводит актуальное число ТС
	#	dict_keys(['time', 'org_inn', 'vin', 'org_name', 'id', 'angle', 'speed', 'gosnumber', 'org_id', 'typets', 'idDev', 'lon', 'lat', 'date'])
	else:
		url = 'http://10.10.21.20:8000/v.1/vehicles_dic/8d6fbe03e99f4b13966e622981c9a11f'	# выдает теперь полный список машин.
	#	dict_keys(['groupname', 'vin', 'id', 'name', 'idDev', 'gosnumber', 'org_name', 'org_inn', 'typets', 'createts', 'org_id', 'markats'])
	print(url)
	try:
		response = requests.get(url)
		response_json = response.json()
	except:
		print(response)
		return
	if not response_json:	return
	if response_json and response_json[0].get('error'):
		print("ERROR: %s %s" % (response_json[0].get('error'), response_json[1].get('info')))
		return
	j = k = 0
	org_id2name = {}
	j = 0
	for r in response_json: #response.json():
	#	print (r.keys())	#;	break
		org_id = r.get('org_id')
		print (r)
		j += 1
	#	if j > 15:	break
		if org_id not in org_id2name.keys():
			org_id2name[org_id] = {'tname': r.get('org_name'), 'inn': r.get('org_inn'), 'ts': []}
		org_id2name[org_id]['ts'].append({'id_t1': r.get('id'), 'gosnum': r.get('gosnumber').upper(), 'id_dev': r.get('idDev'), 'last_date': r.get('date'), 'last_tm': r.get('time')})
	
	print (turl, '\tLen response_json:', len(response_json), '\n')
	# ddb = dbtools3.dbtools('host=212.193.103.20 dbname=test port=5432 user=smirnov')
	ddb = dbtools3.dbtools('host=10.10.2.241 dbname=test port=5432 user=smirnov')
	print('#'*33)
	if turl == 'creat':   create_000(ddb, org_id2name)
	if turl == 'actv':    create_all(ddb, org_id2name)
	if turl == 'updt':    update_avtv(ddb, org_id2name)
	

def create_000(ddb, org_id2name):
	""" Поиск и формирование первоначального набора данных  """
	for k in org_id2name:
		tss = org_id2name[k].get('ts')
		if not tss:     continue
		for t in tss:
			print(t)

def update_avtv(ddb, org_id2name):
	for k in org_id2name:
		tss = org_id2name[k].get('ts')
		if not tss:     continue
		inn = org_id2name[k].get('inn')
		# print(org_id2name[k].keys());		break
		for t in tss:
			last_tm = t.get('last_tm')
			if not last_tm:     continue
			# print(t)
			# k = t['id_t1']
			query = "SELECT id, id_t1, id_dev, inn, gosnum, last_date, last_tm FROM t1_atts WHERE gosnum = '%s'" % t.get('gosnum')
			dctt = ddb.get_dict(query)
			if dctt:
				if dctt['last_tm'] < last_tm:
					sets = ["last_date = '%s'" % t['last_date'], "last_tm = %s" % last_tm]
					if inn and inn != dctt['inn']:
						sets.append("inn = %s" % inn)
					query = "UPDATE t1_atts SET %s  WHERE id = %d" % (', '.join(sets), dctt['id'])
					print(query, ddb.execute(query))
			else:
				query = "INSERT INTO t1_orgs (id_t1, inn, t1name) VALUES ('%s', %s, '%s')" % (k, inn if inn else 'NULL', org_id2name[k]['tname'])
				# print(query, ddb.execute(query))
				print(query)
				add_atts(ddb, t, inn)

	
def create_all(ddb, org_id2name):
	for k in org_id2name:
		print (k, org_id2name[k]['inn'], org_id2name[k]['tname'])
		if not org_id2name[k]['ts']:	continue
		inn = org_id2name[k]['inn']
		query = "INSERT INTO t1_orgs (id_t1, inn, t1name) VALUES ('%s', %s, '%s')" % (k, inn if inn else 'NULL', org_id2name[k]['tname'])
		print (query, ddb.execute(query))
		for ts in org_id2name[k]['ts']:
			add_atts(ddb, ts, inn)
		
		
def add_atts (ddb, dts, inn):
	""" Добавить ТС в БД    """
	cols = ['inn'] if inn else []
	vals = ['%s' % inn] if inn else []
	# if inn:
	# 	cols.append('inn')
	# 	vals.append('%s' % inn)
	for c in dts.keys():
			if dts[c]:
				cols.append(c)
				vals.append("'%s'" % dts[c])
	query = "INSERT INTO t1_atts (%s) VALUES (%s)" % (', '.join(cols), ', '.join(vals))
	print (query, ddb.execute(query))


def check_org ():
	""" Контроль наличия организаций и наличия у них ТС """
	# Отсутствуют в бухгалтерии ( 1С )
	query = """SELECT count(*), id_org FROM wtransports WHERE id_org IN  (SELECT id_org FROM organizations WHERE inn IN (
        5202007618, 5203001016, 5203001016, 5204001114, 5204012814, 5205000709, 5205004446, 5205005513, 5206001367, 5206001409, 5206001720, 5208005744,
        5209004310, 5212005112, 5212007286, 5212511278, 5214006030, 5214006721, 5215000426, 5215009468, 5216001711, 5216017126, 5217003976, 5219000071,
        5219000850, 5221006585, 5222013313, 5223002339, 5225001122, 5225004758, 5226000273, 5226000410, 5226011758, 5226014364, 5227005926, 5229001638,
        5229009027, 5229009027, 5229009027, 5229009027, 5229009027, 5229009027, 5229009027, 5229009027, 5229009027, 5229009027, 5246045628, 5246046766,
        5247015111, 5247016958, 5247048220, 5249055381, 5249057251, 5249061106, 5249066601, 5249066619, 5249076222, 5249084953, 5250039673, 5250043790,
        5250046783, 5250056020, 5250057707, 5250061809, 5256060544, 5256064757, 5256083975, 5256087930, 5256132894, 5256133168, 5256135013, 5256140969,
        5258029518, 5258121094, 5259115086, 5259120142, 5259131313, 5260041390, 5260076949, 5260139980, 5260148174, 5260357227, 5260381460, 5260403233,
        5260410382, 5261015145, 5261113216, 5261120608, 5262035560, 5262123520, 5262288057, 5262311940, 5263049020, 5263089640, 5263111359, 5263133747,
        521475244896, 521500776400, 521600283133, 522100130194, 522200043347, 522401215726, 524500120268, 524503379942, 524504481339, 524612643925,
        524701328820, 524802195252, 525004789303, 525100054358, 525100866350, 525101406034, 525405675511, 525405760340, 526301541890
	)) GROUP BY id_org ORDER BY id_org;"""
	# Отсутствуют старой РНИС ( БД contracts )
	query = """SELECT count(*), id_org FROM wtransports WHERE id_org IN  (SELECT id_org FROM organizations WHERE inn IN (
		5247007858, 5209004415, 5256122617, 5258082536, 5260255391, 5260279106, 5262277263
	)) GROUP BY id_org ORDER BY id_org;"""
	ddb = dbtools3.dbtools('host=10.10.2.241 dbname=contracts port=5432 user=smirnov')
	print(query)
	rows = ddb.get_rows(query)
	for c, id_org in rows:
		dorg = ddb.get_dict("SELECT * FROM organizations WHERE id_org = %s" % id_org)
		print(c, id_org, dorg['inn'], dorg['bname'])
		if c > 0:
			tsrs = ddb.get_rows("SELECT id_ts, gosnum, last_date FROM wtransports WHERE id_org = %s" % id_org)
			for id_ts, gosnum, last_date in tsrs:
				print("\t%s\t%s" % (gosnum, last_date))
	sys.exit()

def	test():
	print ('Test connections:')
	dblist = [
	'host=127.0.0.1 dbname=b03 port=5432 user=smirnov',
	'host=10.40.25.176 dbname=vms_ws port=5432 user=vms',
	# 'host=212.193.103.20 dbname=worktime port=5432 user=smirnov',
	'host=10.10.2.241 dbname=test port=5432 user=smirnov',
	'host=10.10.2.40 dbname=test port=5432 user=smirnov',
	]
	for sdb in dblist:
		print ('\tConnect to', sdb, end='\t')
		ddb = dbtools3.dbtools(sdb, 0)
		print ('Ok' if ddb and not ddb.last_error else ddb.last_error)
	sys.exit()

if __name__ == '__main__':
	sttmr = time.time()
	print("Start %i" % os.getpid(), sys.argv, time.strftime("%Y-%m-%d %T", time.localtime(sttmr)))
	# check_org()   # Контроль наличия организаций и наличия у них ТС
	
	try:
		optlist, args = getopt.getopt(sys.argv[1:], 'tuacc')
		for o in optlist:
			if o[0] == '-t':    test()
			if o[0] == '-c':    get_allts ('creat')     # Поиск и формирование первоначального набора данных
			if o[0] == '-u':    get_allts ('updt')
			if o[0] == '-a':    get_allts ('actv')
			
	#	get_allts ()
	except SystemExit:
		pass
	except:
		print("EXCEPT:", sys.exc_info[:2])
