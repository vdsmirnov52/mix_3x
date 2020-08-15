import sys,time
import requests
'''	Методы доступа к Т1 через http запросы
Метод http://10.10.21.20:8000/v.1/vehicles_dic/8d6fbe03e99f4b13966e622981c9a11f выдает теперь полный список машин.
Метод http://10.10.21.20:8000/v.1/messages/idOrg:1d35c7d8-684a-11ea-bcc8-021ac3a7b695/8d6fbe03e99f4b13966e622981c9a11f выдает все ТС организации
Метод http://10.10.21.20:8000/v.1/update_dics/8d6fbe03e99f4b13966e622981c9a11f выводи актуальное число всех ТС
Метод http://10.10.21.20:8000/v.1/messages/all/8d6fbe03e99f4b13966e622981c9a11f выводит актуальное число ТС
запрос http://10.10.21.20:8000/v.1/messages/idOrg:3dc07dfa-b43f-11e9-8848-029975b11713/8d6fbe03e99f4b13966e622981c9a11f выводит число всех ТС предприятия АПАТ у коого есть БНСО

	Во всех запросах возвращается null, если значения нет.
	ИНН теперь также возвращается.
'''

TOKEN = '8d6fbe03e99f4b13966e622981c9a11f'

def t1_request (url, opt=None, data=None, json=None, **kwargs):
	if opt == 'POST':
		response = requests.post(url, data = data, json = json) #, kwargs)
	else:
		response = requests.get(url)
	print(response.text)
	print('#'*33, type(response))
	# dict_keys(['id', 'idDev', 'typets', 'gosnumber', 'vin', 'lat', 'lon', 'speed', 'angle', 'date', 'time', 'org_id', 'org_name', 'org_inn'])
	response_json = response.json()
	if not response_json:	return
	if response_json and response_json[0].get('error'):
		print("ERROR: %s %s" % (response_json[0].get('error'), response_json[1].get('info')))
		return
	j = k = 0
	for r in response_json: #response.json():
		print (r.get('id'), r.get('idDev'), r.get('gosnumber').upper(), r.get('org_inn'), r.get('org_name'), sep = '\t')
		if r.get('time'):
			print('\t', r.get('date'))
			k += 1
		j += 1
		# break
	print(r.keys())
	print('Rows:', j, 'K:', k)


def is_update_dics():
	""" выводит актуальное число всех ТС    """
	try:
		url = 'http://10.10.21.20:8000/v.1/update_dics/' +TOKEN    # 8d6fbe03e99f4b13966e622981c9a11f'
		response = requests.get(url)
		response_json = response.json()
		# print(response_json)
		# return
		for k in response_json:
			print('%32s:' % k, response_json[k])
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		print(exc_type, exc_value)

		
def actual_tss():
	currtm = time.time()
	url = 'http://10.10.21.20:8000/v.1/messages/all/8d6fbe03e99f4b13966e622981c9a11f'
	response = requests.get(url)
	response_json = response.json()
	j= 0
	for r in response_json:
		jtm = r.get('time')
		if not jtm:   continue
		if 60+currtm > jtm:
			for k in r.keys():
				print(r[k], end = '\t')
			print()
			# print(r.get('gosnumber'), r.get('date'), r.get('lat'), r.get('lon'), sep = '\t')
			j += 1
		# break
	print(j, r.keys())


def get_ts_list (**kwargs):
	''' Читать список ТС    '''
	url = 'http://10.10.21.20:8000/v.1/messages/all/8d6fbe03e99f4b13966e622981c9a11f'
	cols = kwargs.get('cols')
	if not cols:      return
	dtm = kwargs.get('dtime') if kwargs.get('dtime') else 60
	currtm = int(time.time())
	response = requests.get(url)
	response_json = response.json()
	ts_list = []
	for r in response_json:
		jtm = r.get('time')
		if not jtm:     continue
		if dtm+currtm > jtm:
			# print(cols, type(cols))
			if type(cols) == list:
				l = []
				for k in cols:
					if r.get(k):    l.append(r.get(k))
				if l:   ts_list.append(l)
			else:
				v = r.get(cols)
				ts_list.append(v)
		# break
	return ts_list


def comp_vms2t1 ():
	url = 'http://10.10.21.20:8000/v.1/messages/all/8d6fbe03e99f4b13966e622981c9a11f'
	response = requests.get(url)
	# print(response.text)
	# print('#'*33, type(response))
	# dict_keys(['id', 'idDev', 'typets', 'gosnumber', 'vin', 'lat', 'lon', 'speed', 'angle', 'date', 'time', 'org_id', 'org_name', 'org_inn'])
	response_json = response.json()
	if not response_json:	return
	if response_json and response_json[0].get('error'):
		print("ERROR: %s %s" % (response_json[0].get('error'), response_json[1].get('info')))
		return
	j = k = 0
	dinn2orgs = {}
	name_orgs = {}
	dorg2inn = {}
	for r in response_json: #response.json():
		# print (r.get('idDev'), r.get('gosnumber').upper(), type(r.get('org_inn')), r.get('org_name'), sep = '\t')
		
		if r.get('org_inn') and r.get('org_inn').isdigit():
			iinn = int(r.get('org_inn'))
			if not iinn in dinn2orgs.keys():
					dinn2orgs[iinn] = {'name': r.get('org_name'), 'ts': []}
					dorg2inn[r.get('org_name')] = iinn
		else:
			iinn = dorg2inn.get(r.get('org_name'))
		if not iinn:
			# print('ERR:\t', r.get('idDev'), r.get('gosnumber').upper(), r.get('org_inn'), r.get('org_name'), sep = '\t')
			if r.get('org_name') not in name_orgs.keys():
				name_orgs[r.get('org_name')] = []
			name_orgs[r.get('org_name')].append([r.get('gosnumber').upper(), r.get('date'), r.get('time')])
			continue
		dinn2orgs[iinn]['ts'].append([r.get('gosnumber').upper(), r.get('date'), r.get('time')])
		if r.get('time'):
				k += 1
		j += 1
		# if j > 9:  break
	print(r.keys())
	print('Rows:', j, 'K:', k)
	# print(dorg2inn)
	
	'''
	for inn in dinn2orgs.keys():
		print(inn, dinn2orgs[inn]['name'])
		for ts in dinn2orgs[inn]['ts']:
			ts.extend(check_gnum(ts[0]))
			print("\t", ts)
		xl.data2xlsx(dinn2orgs[inn]['name'],
		             [['A1', 'Гос №'], ['B1', 'Дата в Т1'], ['C1', 'секунд'], ['D1', 'Дата в РНИС'], ['E1', 'ИНН РНИС'], ['F1', 'Организация в РНИС']],
		             dinn2orgs[inn]['ts'],
		             cwidth = {'A': 22, 'B': 22, 'C': 16, 'D': 22, 'E': 22, 'F': 22, 'G':22},
		             fname = str(inn))
	'''
	'''
	for oname in name_orgs.keys():
		print(oname)
		for ts in name_orgs[oname]:
			ts.extend(check_gnum(ts[0]))
			print('\t', ts)
		try:
			fname = oname
			xl.data2xlsx(oname,
			             [['A1', 'Гос №'], ['B1', 'Дата в Т1'], ['C1', 'секунд'], ['D1', 'Дата в РНИС'], ['E1', 'ИНН РНИС'], ['F1', 'Организация в РНИС']],
			             name_orgs[oname],
			             cwidth = {'A': 22, 'B': 22, 'C': 16, 'D': 22, 'E': 22, 'F': 22, 'G':22},
			             fname = fname)
		except: print("EXCEPT", fname)
	'''


import tst_openpyxl as xl
import dbtools3
DBID = None


def check_gnum(gnum):
	global  DBID
	
	query = "SELECT t.gosnum, a.last_date, o.inn, o.bname FROM transports t " \
	        "JOIN atts a ON t.id_ts = a.autos AND t.device_id = a.device_id " \
	        "JOIN organizations o ON t.id_org = o.id_org WHERE gosnum = '%s'" % gnum
	if not DBID:    DBID = dbtools3.dbtools('host=10.10.2.241 dbname=contracts port=5432 user=smirnov')
	dr = DBID.get_dict(query)
	if dr and dr.get('last_date'):
		return [str(dr.get('last_date'))[:10], dr.get('inn'), dr.get('bname')]
	# return [str(dr['last_date']), dr['inn']]
	return []

if __name__ == "__main__":
	is_update_dics()
	comp_vms2t1()
	# actual_tss()

	url = 'http://10.10.21.20:8000/v.1/messages/all/8d6fbe03e99f4b13966e622981c9a11f'
	# url = 'http://10.10.21.20:8000/v.1/vehicles_dic/8d6fbe03e99f4b13966e622981c9a11f'
	# url = 'http://10.10.21.20:8000/v.1/messages/idOrg:6a94317c-c2b8-11ea-ac79-021ac3ba17f9/8d6fbe03e99f4b13966e622981c9a11f'
	# url = 'http://10.10.21.20:8000/v.1/messages/idOrg:1d35c7d8-684a-11ea-bcc8-021ac3a7b695/8d6fbe03e99f4b13966e622981c9a11f'
	# url = 'http://10.10.21.20:8000/v.1/messages/idOrg:1d35c7d8-684a-11ea-bcc8-021ac3a7b695/8d6fbe03e99f4b13966e622981c9a111'    # ERROR
	# t1_request (url)

