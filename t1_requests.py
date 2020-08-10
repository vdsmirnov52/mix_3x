import sys
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
	j = 0
	for r in response_json: #response.json():
		print (r.get('id'), r.get('idDev'), r.get('gosnumber'), r.get('org_id'), r.get('org_name'), sep = '\t')
		j += 1
		# break
	print(r.keys())
	print('Rows:', j)


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
		
if __name__ == "__main__":
	is_update_dics()

	# url = 'http://10.10.21.20:8000/v.1/messages/all/8d6fbe03e99f4b13966e622981c9a11f'
	# url = 'http://10.10.21.20:8000/v.1/vehicles_dic/8d6fbe03e99f4b13966e622981c9a11f'
	# url = 'http://10.10.21.20:8000/v.1/messages/idOrg:6a94317c-c2b8-11ea-ac79-021ac3ba17f9/8d6fbe03e99f4b13966e622981c9a11f'
	url = 'http://10.10.21.20:8000/v.1/messages/idOrg:1d35c7d8-684a-11ea-bcc8-021ac3a7b695/8d6fbe03e99f4b13966e622981c9a11f'
	# url = 'http://10.10.21.20:8000/v.1/messages/idOrg:1d35c7d8-684a-11ea-bcc8-021ac3a7b695/8d6fbe03e99f4b13966e622981c9a111'    # ERROR
	t1_request (url)
	
