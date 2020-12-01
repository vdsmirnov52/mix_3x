#!/usr/bin/env python 
# -*- coding: utf-8 -*-
""" gRPC прием данных от Т1 и отправка БД receiver (АнтиСнег)	

	nohup /home/smirnov/VProjects/mix_35/mix_35/bin/python /home/smirnov/VProjects/mix_35/t1_anti_snow.py > /home/smirnov/VProjects/t1_anti_snow.log &
"""

import  time, sys, os
import	grpc
import	Api_pb2
import	Api_pb2_grpc

DEVICES = ["157231", "1284891", "1284890", "1323475", "24964441", "353451048047755"]
NUMBERS = []
'''
#   Муниципальное казенное учреждение "Специалист"
NUMBERS = ['a422aa152', 'a523aa152', 'a592aa152', 'а421аа52', 'а423аа152', 'а639кр152', 'а642кр152', 'в296на152', 'е013тк152', 'е588ху152',
            'е995та152', 'к623хо152', 'м385тн152', 'н044нн52', 'н046нн52', 'н154нн52', 'н204ао152', 'о197ур152', 'р202еу152', 'х555рс52']
#   МУП Экспресс Дзержинск
NUMBERS = ['а908ко152', 'о014рр52', 'о146аа52', 'о632ма52', 'т415хк52', 't006', 't009', 't017', 't019', 't035', 't051', 't054', 't055', 't056',
           't057', 't059', 't060', 't061', 't062', 't063', 't064', 't065', 't066', 't067', 't068', 't069', 't070', 't072', 't075', 't077', 't078',
           't079', 't080', 't081', 't082', 'а003ех152', 'а012ет152', 'а858ет152', 'ао92052', 'ао92152', 'ао92352', 'ао92452', 'ар91852', 'ар92252',
           'ар99752', 'ат47652', 'ау48652', 'ау75952', 'ау76052', 'ау76152', 'ау76252', 'ау76352', 'ау76452', 'ау76552', 'ау76652', 'ау76752',
           'ау76852', 'ау76952', 'ау77052', 'в293вк152', 'о013ес52', 'о433мт52', 'о531ме52', 'с128еа152', 'с135еа152', 'с421ат152', 'с427ат152',
           'с467ат152', 'с482ат152', 'с486ат152', 'с488ат152', 'с489ат152', 'с491ат152', 'с497ат152', 'с498ат152', 'с504ат152', 'с505ат152', 'с507ат152',
           'с508ат152', 'с511ат152', 'с516ат152', 'с517ат152', 'с518ат152', 'с521ат152', 'с523ат152', 'с562еа152', 'с568еа152', 'с570еа152', 'с629еа152',
           'с638еа152', 'с642еа152', 'с644еа152', 'с649еа152', 'с650еа152', 'с655ве152', 'с659ва152', 'с677еа152', 'с684еа152', 'с702ве152', 'с703ве152',
           'с705ве152', 'с708ве152', 'с711ве152', 'с719ве152', 'с723ве152', 'с724ве152', 'с725ве152', 'с751ве152', 'с753ве152', 'с755ве152', 'с756ве152',
           'с758ве152', 'с760ве152', 'с763ве152', 'с765ве152', 'с769ве152', 'т403хк52', 'т734от52', 'т741от52']
# '''


def getState(stub):
	""" Текущее состояние   """
	Filter = Api_pb2.DataFilter(DeviceCode = DEVICES, StateNumber = NUMBERS)
	# Filter = Api_pb2.DataFilter(StateNumber = NUMBERS)
	Fields = Api_pb2.FieldsToggle(Position=True, Motohours=True, Mileage = True)
	request = Api_pb2.ObjectsStateRequest(Filter = Filter, Fields = Fields)

	result = stub.GetObjectsState(request)
	print ('RRR', type(result))   # result.Objects содержит в себе результаты запроса по кажому из устройств
	for o in result.Objects:
		d = o.Data[0]
		print (o.StateNumber, d.DeviceTime, time.strftime("\t%d-%m-%Y %T", time.localtime(d.DeviceTime)), d.DeviceCode, sep = '\t', end = ' ->')
		print ("\tLon: %s\tLat: %s\tCourse: %s" % (d.Position.Longitude, d.Position.Latitude, d.Position.Course))
		# print (o.Data[0].Position)
		# print (o.Data)
	# print(o.Data, d.DeviceTime)


def getDataRange (stub, dtime = None):
	""" получение телематических данных за период (пследние dtime секунд)   """
	global NUMBERS, DEVICES
	# print('DataRange NUMBERS:', NUMBERS)
	itm = int(time.time())
	itmf = itm-dtime if dtime and dtime > 10 else itm-600
	# print('DateFrom:', time.strftime("\t%d-%m-%Y %T ", time.localtime(itmf)), time.strftime("\t%d-%m-%Y %T ", time.localtime(itm)))
	Filter = Api_pb2.DataFilter(DeviceCode = DEVICES, StateNumber = NUMBERS, DateFrom = itmf, DateTo = itm)
	Fields = Api_pb2.FieldsToggle(Position=True)
	request = Api_pb2.ObjectsStateRequest(Filter = Filter, Fields = Fields)
#	DeviceTime  - врема передачи прибором
#	ReceivedTime - время приема данных сервером
	nums_is_data = []
	war_devids = {}
	dev_list = DEVID_GNUM.keys()
	try:
		rrr = stub.GetObjectsDataRange(request)
		print("RRR", type(rrr))
		for o in rrr.Objects:
		#	print ("%16s" % o.DeviceCode, o.StateNumber.upper())
			id_dev = o.DeviceCode
			gosnum = o.StateNumber.upper()
			if o.Data:
				print ("%16s" % id_dev, gosnum)	#o.DeviceCode, o.StateNumber.upper())
				nums_is_data.append(o.StateNumber)
			for d in o.Data:
				print (time.strftime("\t%d-%m-%Y %T", time.localtime(d.DeviceTime)), time.strftime("\t%d-%m-%Y %T ", time.localtime(d.ReceivedTime)), end = '\t')
				print (d.Position.Longitude, '\t', d.Position.Latitude, d.State.Mileage, "\tdt:", (d.ReceivedTime - d.DeviceTime))
			if o.DeviceCode not in dev_list:
				print("#"*22, "[%s] [%s]" % (id_dev, gosnum), o.Data[-1] if o.Data else 'ZZZ')
				'''
					t = point.DeviceTime
					x = point.Position.Longitude
					y = point.Position.Latitude
					ht = point.Position.Altitude

				'''
				war_devids[gosnum] = o.DeviceCode
	except grpc._channel._InactiveRpcError:
		print('except getDataRange:\t', sys.exc_info()[:2])
	return	war_devids


dcode_ok = {}	# Рсчет среднего времени задержки при передаче данных

def getDataStream (stub, numbers = None):
	""" подписка на поток телематических данных устройства  """
	global dcode_ok, QDATAS, DEVID_GNUM, FL_BREAK

	StateNumber = numbers if numbers else NUMBERS
	print('Lenth StateNumber:\t', len(StateNumber))
	tt = time.time()
	Filter = Api_pb2.DataFilter(DeviceCode = None, StateNumber = StateNumber)   #numbers if numbers else NUMBERS)
	# Filter = Api_pb2.DataFilter(DeviceCode = DEVICES, StateNumber = None)   #numbers if numbers else NUMBERS)
	Fields = Api_pb2.FieldsToggle(Position = True)
	request = Api_pb2.ObjectsDataStreamRequest(Filter = Filter, Fields = Fields)
	try:
		datas = stub.GetObjectsDataStream(request)
		print("\nSSS request: ", type(request))	#, type(datas))
		# help(datas);    return
		j = 0
		for point in datas:
			# print(">>> %s" % point)
			dTime = (point.ReceivedTime - point.DeviceTime)
			if point.Position.Latitude == 0.0:  continue
			dev_code = point.DeviceCode.strip()
			QDATAS.put((dev_code, DEVID_GNUM.get(dev_code), point))
			dcode_ok[dev_code] = (4 * dcode_ok[dev_code] + dTime)/5 if dcode_ok.get(dev_code) else dTime
		#	print("%16s  DeviceTime: %s  dT:%7d \t" % (point.DeviceCode, time.strftime('%d.%m.%Y %T', time.localtime(point.DeviceTime)), dTime),
		#	      point.Position.Longitude, point.Position.Latitude, point.Position.Course, point.Position.Speed)
		#	print('point:', point)
			j += 1
		#	if j > 3:				break
		print('Finish', datas)
	except:
		print('except getDataStream:\t', sys.exc_info()[:2])
		print("dcode_ok", len(dcode_ok))
		for k in dcode_ok:	print("%22s" % k, dcode_ok[k])

	print('\trunning:', datas.running())
	print(time.strftime('Start:\t%d.%m.%Y %T', time.localtime(tt)))
	print(time.strftime('Stop:\t%d.%m.%Y %T', time.localtime(time.time())))
#	FL_BREAK = True
	return


from queue import Queue
from threading import Thread
import	dbtools3 as dbt

DEVID_GNUM = {}		# dev_code: 
FL_BREAK = False
QDATAS = None


def	getQueue():
	""" Читать очередь данных из Т1 и отправка БД receiver (АнтиСнег)	"""
	global	QDATAS

	idb_receiver = dbt.dbtools('host=212.193.103.20 dbname=receiver port=5432 user=smirnov')
	def get_recv_gnums ():
		query = "SELECT inn, gosnum, device_id, rem FROM recv_ts WHERE inn IN (SELECT inn FROM org_desc WHERE bm_ssys = 131072)"
		rows = idb_receiver.get_rows(query)
		recv_gnums = {}
		for inn, gosnum, device_id, rem in rows:
			recv_gnums[gosnum] = inn, device_id, rem
		return	recv_gnums


	recv_gnums = get_recv_gnums()
	recv_gn_list = list(recv_gnums.keys())
	count_empt = 0

	while not FL_BREAK:
		try:
			if QDATAS.empty():
				time.sleep(1)
				count_empt += 1
				if count_empt // 900:
					print("#"*22, "\tcount_empt:", count_empt)
					count_empt = 0
					sys.exit()
				#	os._exit()
				continue
			count_empt = 0
			data = QDATAS.get()
			dev_code, ggg, point = data
			if ggg:
				gnum, inn = ggg
			else:	gnum, inn = None, None
			t = point.DeviceTime
			x = point.Position.Longitude
			y = point.Position.Latitude
			ht = point.Position.Altitude
			cr = point.Position.Course
			st = point.Position.Satellites
			sp = point.Position.Speed

			querys = []
			is_new_ts = False
			is_rlast = False
		#	print("%16s" % dev_code, t, inn, gnum, x, y, ht, cr, st, sp, end = " \t")	#point.Position)
		#	print("%16s" % dev_code, t, inn, gnum, end = " \t")
			print("%16s" % dev_code, time.strftime(" %T %d.%m.%Y", time.localtime(t)), inn, gnum, end = " \t")
			if not ggg:
				print("ggg:", ggg)
				continue
			if gnum and gnum in recv_gn_list:
				rcv_inn, rcv_device_id, rcv_rem = recv_gnums[gnum]
				if rcv_rem and 'T1' in rcv_rem:
					print("#"*6, rcv_inn, rcv_device_id, rcv_rem)
					querys.append("DELETE FROM last_pos WHERE ida =%s" % dev_code)
	#				querys.append("INSERT INTO last_pos (ida, idd, x, y, t, cr, sp, st) VALUES (%s, '%s', %s, %s, %s, %s, %s, %s)" % (dev_code, dev_code, x, y, t, cr, sp, st))
	#				querys.append("INSERT INTO data_pos (ida, idd, x, y, t, cr, sp, st) VALUES (%s, '%s', %s, %s, %s, %s, %s, %s)" % (dev_code, dev_code, x, y, t, cr, sp, st))
				else:
					rlast = idb_receiver.get_dict("SELECT * FROM vlast_pos WHERE gosnum = '%s'" % gnum)
					code_ffff = int(dev_code)
					if rlast:
						is_rlast = True
						dTime = (t - rlast['t'])
						print("dTime: %s" % dTime)
						if dTime < 3600:	continue	# < Часа
						if code_ffff != rlast['ida']:
							if dTime > 2592000:	# > 30 дней 5184000:	# > 60 дней
								is_new_ts = True
								querys.append("UPDATE recv_ts SET idd = 'idd%s', device_id = %s, rem = 'T1 %s' WHERE device_id = %s" % (code_ffff, code_ffff, code_ffff, rcv_device_id))
								querys.append("DELETE FROM last_pos WHERE ida =%s" % code_ffff)
							else:	print("\t\tdev_code %s != " % code_ffff, rlast['ida'])
					else:
						if code_ffff != rcv_device_id:
							is_new_ts = True
							print("UPDATE device_id")
							querys.append("UPDATE recv_ts SET idd = 'idd%s', device_id = %s, rem = 'T1 %s' WHERE device_id = %s" % (code_ffff, code_ffff, code_ffff, rcv_device_id))
							querys.append("DELETE FROM last_pos WHERE ida =%s" % code_ffff)
						else:	print("\tNOT in vlast_pos")
			else:
			#	print("%16s" % dev_code, t, inn, gnum, x, y, ht, cr, st, sp)	#point.Position)
				print("\tNOT in recv_ts")
				is_new_ts = True
				querys.append("INSERT INTO recv_ts (idd, inn, gosnum, device_id, rem) VALUES ('idd%s', %s, '%s', %s, '%s')" % (dev_code, inn, gnum, dev_code, 'T1 new'))
				querys.append("DELETE FROM last_pos WHERE ida =%s" % dev_code)

			if querys:
				querys.append("INSERT INTO last_pos (ida, idd, x, y, t, cr, sp, st) VALUES (%s, '%s', %10.6f, %10.6f, %s, %s, %s, %s)" % (dev_code, dev_code, x, y, t, cr, sp, st))
				querys.append("INSERT INTO data_pos (ida, idd, x, y, t, cr, sp, st) VALUES (%s, '%s', %10.6f, %10.6f, %s, %s, %s, %s)" % (dev_code, dev_code, x, y, t, cr, sp, st))
				if idb_receiver.qexecute(';\n'.join(querys)):
					if is_new_ts:
					#	recv_gnums[gosnum] = inn, dev_code, 'T1 add %s' % dev_code
						recv_gnums = get_recv_gnums()
						recv_gn_list = list(recv_gnums.keys())
					#	recv_gn_list.append(dev_code)
				else:
					print('is_new_ts', is_new_ts, querys[0])
		except:
			print("getQueue", sys.exc_info()[:2])
		#	print("querys", ";\n".join(querys))
	print("#"*33, "Finish getQueue")


def	getActiveNumbers ():
	""" Читаем госномера машин которые ранее отправляли данные	"""
	global	DEVID_GNUMD

	idb_contracts = dbt.dbtools('host=212.193.103.20 dbname=contracts port=5432 user=smirnov')
	query = "SELECT id_dev, inn, gosnum FROM t1_atts WHERE inn IN (SELECT inn FROM t1_orgs WHERE bm_ssys = 131072) AND last_tm > 0;"	### 104  ts
#	query = "SELECT id_dev, inn, gosnum FROM t1_atts WHERE inn IN (SELECT inn FROM t1_orgs WHERE bm_ssys = 131072) AND x > 0;"	### 80 ts
	rows = idb_contracts.get_rows(query)
	nums = []
	inns = []
	for id_dev, inn, gosnum in rows:
		nums.append(gosnum.lower())
		DEVID_GNUM[id_dev] = [gosnum, inn]
		if inn not in inns:	inns.append(inn)
###	for k in DEVID_GNUM.keys():	print ("%22s" % k, DEVID_GNUM[k])
	for i in inns:	print(i, end = ', ')
	print()

	return	nums
	

def	main (cname, activ_nums):
#	sys.exit()

	print('\n\tОткрываем канал и создаем клиент:\t', cname, time.strftime("\t%d.%m.%Y %T", time.localtime(time.time())))
	channel = grpc.insecure_channel(cname)  # 'rnis-api.rnc52.ru:6161')
	stub = Api_pb2_grpc.APIStub(channel)
	# activ_nums = getDataRange(stub, dtime = 22)  # получение телематических данных за период
	getDataRange(stub, dtime = 33)  # получение телематических данных за период
	print('Len NUMBERS:\t', len(NUMBERS))
	print('Len activ_nums:\t', len(activ_nums))
	getDataStream(stub, activ_nums)


#	'1500АА52', '8326НО52', 'Р474СА152', 'Е452СА152', 'Н950СН152', 'Н182РХ197'
if __name__ == '__main__':
	NUMBERS = getActiveNumbers ()
	if NUMBERS:
		print("NUMBERS:", NUMBERS)
		QDATAS = Queue()
		ttt = Thread(target = getQueue, args = ())
		ttt.start()
		try:
			while 1:
				dcode_ok = {}
				main ('10.10.21.22:6161', NUMBERS)
				time.sleep(30)
		except KeyboardInterrupt:			FL_BREAK = True
		except SystemExit:
			print ("#"*33, "SystemExit")
	else:
		print("Отсутствует список NUMBERS", NUMBERS)
