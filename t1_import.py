#!/usr/bin/env python

import	sys, time, opt
import	dbtools3
import	requests


def	get_allts (turl = None):
	""" Читать список ТС 	"""
	if turl == 'act':
		url = 'http://10.10.21.20:8000/v.1/messages/all/8d6fbe03e99f4b13966e622981c9a11f'	# выводит актуальное число ТС
	#	dict_keys(['time', 'org_inn', 'vin', 'org_name', 'id', 'angle', 'speed', 'gosnumber', 'org_id', 'typets', 'idDev', 'lon', 'lat', 'date'])
	else:
		url = 'http://10.10.21.20:8000/v.1/vehicles_dic/8d6fbe03e99f4b13966e622981c9a11f'	# выдает теперь полный список машин.
	#	dict_keys(['groupname', 'vin', 'id', 'name', 'idDev', 'gosnumber', 'org_name', 'org_inn', 'typets', 'createts', 'org_id', 'markats'])
	response = requests.get(url)
	response_json = response.json()
	if not response_json:	return
	if response_json and response_json[0].get('error'):
		print("ERROR: %s %s" % (response_json[0].get('error'), response_json[1].get('info')))
		return
	j = k = 0
	dinn2orgs = {}
	name_orgs = {}
	dorg2inn = {}
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
	ddb = dbtools3.dbtools('host=212.193.103.20 dbname=test port=5432 user=smirnov')
	for k in org_id2name:
		print (k, org_id2name[k]['inn'], org_id2name[k]['tname'])
		if not org_id2name[k]['ts']:	continue
		inn = org_id2name[k]['inn']
		query = "INSERT INTO t1_orgs (id_t1, inn, t1name) VALUES ('%s', %s, '%s')" % (k, inn if inn else 'NULL', org_id2name[k]['tname'])
		print (query, ddb.execute(query))
		for ts in org_id2name[k]['ts']:
		#	print ('\t>', ts)
			cols = []
			vals = []
			for c in ts.keys():
				if ts[c]:
					cols.append(c)
					vals.append("'%s'" % ts[c])
			query = "INSERT INTO t1_atts (%s) VALUES (%s)" % (', '.join(cols), ', '.join(vals))
			print (query, ddb.execute(query))


def	test():
	print ('Test connections:')
	dblist = [
	'host=127.0.0.1 dbname=b03 port=5432 user=smirnov',
	'host=10.40.25.176 dbname=vms_ws port=5432 user=vms',
	'host=212.193.103.20 dbname=worktime port=5432 user=smirnov',
	'host=10.10.2.40 dbname=test port=5432 user=smirnov',
	]
	for sdb in dblist:
		print ('\tConnect to', sdb, end='\t')
		ddb = dbtools3.dbtools(sdb, 0)
		print ('Ok' if ddb and not ddb.last_error else ddb.last_error)


if __name__ == '__main__':
#	test()
	get_allts ('act')
#	get_allts ()

