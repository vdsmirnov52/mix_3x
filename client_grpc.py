#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" gRPC прием данных от Т1 (TEST)	"""

import  time, sys
import	grpc
import	Api_pb2
import	Api_pb2_grpc

DEVICES = ["157231", "1284891", "1284890", "1323475", "24964441", "353451048047755"]
#   Active T1
# NUMBERS = ['р984кх152', 'р999кх152', 'ар31752', 'ау20652', 'м063ов152']
#   Муниципальное казенное учреждение "Специалист"
NUMBERS = ['a422aa152', 'a523aa152', 'a592aa152', 'а421аа52', 'а423аа152', 'а639кр152', 'а642кр152', 'в296на152', 'е013тк152', 'е588ху152',
            'е995та152', 'к623хо152', 'м385тн152', 'н044нн52', 'н046нн52', 'н154нн52', 'н204ао152', 'о197ур152', 'р202еу152', 'х555рс52']
# '''
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
	rrr = stub.GetObjectsDataRange(request)
	print("RRR", type(rrr))
	"""
	DeviceTime  - врема передачи прибором
	ReceivedTime - время приема данных сервером
	"""
	activ_num = []
	for o in rrr.Objects:
		print ("%16s" % o.DeviceCode, o.StateNumber.upper())
		if o.Data:
			activ_num.append(o.StateNumber)
		for d in o.Data:
			print (time.strftime("\t%d-%m-%Y %T", time.localtime(d.DeviceTime)), time.strftime("\t%d-%m-%Y %T ", time.localtime(d.ReceivedTime)), end = '\t')
			print (d.Position.Longitude, '\t', d.Position.Latitude, d.State.Mileage, "\tdt:", (d.ReceivedTime - d.DeviceTime))
	return activ_num

dcode_ok = {}

def getDataStream (stub, numbers = None):
	""" подписка на поток телематических данных устройства  """
	global dcode_ok
	StateNumber = numbers if numbers else NUMBERS
	print('getDataStream\n\tStateNumber:\t', len(StateNumber))
	Filter = Api_pb2.DataFilter(DeviceCode = None, StateNumber = StateNumber)   #numbers if numbers else NUMBERS)
	# Filter = Api_pb2.DataFilter(DeviceCode = DEVICES, StateNumber = None)   #numbers if numbers else NUMBERS)
	Fields = Api_pb2.FieldsToggle(Position = True)
	request = Api_pb2.ObjectsDataStreamRequest(Filter = Filter, Fields = Fields)
	datas = stub.GetObjectsDataStream(request)
	print("\nSSS request: ", type(request))	#, type(datas))
	# help(datas);    return
	print('Len StateNumber: %d\n' % len(StateNumber))#, StateNumber)
	tt = time.time()
	j = 0
	try:
		for point in datas:
			# print(">>> %s" % point)
			dTime = (point.ReceivedTime - point.DeviceTime)
			if point.Position.Latitude == 0.0:  continue
			if point.DeviceCode in dcode_ok.keys():
				dcode_ok[point.DeviceCode] = (4 * dcode_ok[point.DeviceCode] + dTime)/5
			else:
				dcode_ok[point.DeviceCode] = dTime
			print("%16s  DeviceTime: %s  dT:%7d \t%10.6f %10.6f" % (
				point.DeviceCode, time.strftime('%d.%m.%Y %T', time.localtime(point.DeviceTime)), dTime,
				point.Position.Longitude, point.Position.Latitude), point.Position.Course, point.Position.Speed, point.Position.State)
			j += 1
			# if j > 3:				break
		print('Finish', datas)
	except:
		print('except:\t', sys.exc_info()[:2])
		print("dcode_ok", len(dcode_ok))
		for k in dcode_ok:
			print("%22s" % k, dcode_ok[k])
	
	print('\trunning:', datas.running())
	print(time.strftime('Start:\t%d.%m.%Y %T', time.localtime(tt)))
	print(time.strftime('Stop:\t%d.%m.%Y %T', time.localtime(time.time())))
	return


def callback(arg):
	print ("\ncallback:".upper(), type(arg))
	for j in range(11):
		print (j, arg)
		print ("result", arg.result(timeout=1000))
		if arg.debug_error_string:  break


def getInfo (stub):
	""" получение метаданных устройства """
	itm = int(time.time())
	itmf = itm-600
	Filter = Api_pb2.DataFilter(DeviceCode = DEVICES, StateNumber = NUMBERS, DateFrom = itmf, DateTo = itm)
	request = Api_pb2.ObjectsInfoRequest(Filter=Filter)
	rrr = stub.GetObjectsInfo(request)  #ObjectsInfoRequest)
	# print ("Info", type(rrr)
	# print (  rrr


def test():
	# help (stub.GetObjectsEventsStream)    # MultiThreadedRendezvous)
	# help(stub.GetObjectsState)
	pass


list_canals = [
	'rnis-api.rnc52.ru:6161',	# Наша система
	'rnis-tm.t1-group.ru:18082',	# Разработчик Т1
	'10.10.21.22:6161',
	'10.10.21.21:6161',	### 
]


def	get_range (ts_numbers: list, dt=60):
	""" Запрос данных состояния ТС за последние dt секунд
	Возвращает activ_data - справочник активных ТС.
	DeviceTime  - врема передачи прибором
	ReceivedTime - время приема данных сервером
	"""

	channel = grpc.insecure_channel('10.10.21.22:6161')
#	channel = grpc.insecure_channel('10.10.21.21:6161')
	stub = Api_pb2_grpc.APIStub(channel)
	if not ts_numbers:
		print("Нет списка ТС")
		return

	itm = int(time.time())
	itmf = itm-dt if dt and dt > 10 else itm-600
	Filter = Api_pb2.DataFilter(StateNumber = ts_numbers, DateFrom = itmf, DateTo = itm)
	Fields = Api_pb2.FieldsToggle(Position=True)
	request = Api_pb2.ObjectsStateRequest(Filter = Filter, Fields = Fields)
	rrr = stub.GetObjectsDataRange(request)
	# print("RRR", type(rrr))

	activ_num = []
	activ_data = {}
	for o in rrr.Objects:
		# print ("%16s" % o.DeviceCode, o.StateNumber.upper())
		if o.Data:
			activ_num.append(o.StateNumber)
			# for d in o.Data:
			# 	print (time.strftime("\t%d-%m-%Y %T", time.localtime(d.DeviceTime)), time.strftime("\t%d-%m-%Y %T ", time.localtime(d.ReceivedTime)), end = '\t')
			# 	print (d.Position.Longitude, '\t', d.Position.Latitude, d.State.Mileage, "\tdt:", (d.ReceivedTime - d.DeviceTime))
			activ_data[o.StateNumber.upper()] = o.Data[-1]
		# else:   print(o)
		
	return activ_data


def	get_actual_ts():
	""" Читать актуальные данные от ТС	"""
	global	NUMBERS

	# for j in range(len(NUMBERS)):   NUMBERS[j] = NUMBERS[j].upper()
#	NUMBERS = []    # Весь транспорт
	# get_range(['К795СА152', '0551АА52', 'К796СА152'])

	# test()
	# t1.actual_tss()
	# print(t1.get_ts_list(cols = ['date', 'gosnumber', 'time']))
	activ_nums = NUMBERS
	# NUMBERS = t1.get_ts_list(cols = 'gosnumber')
	# '''
	cname = list_canals[2]
	print('\n\tОткрываем канал и создаем клиент:\t', cname, time.strftime("\t%d.%m.%Y %T", time.localtime(time.time())))
	channel = grpc.insecure_channel(cname)  # 'rnis-api.rnc52.ru:6161')
	stub = Api_pb2_grpc.APIStub(channel)
	# getInfo(stub)
	print(NUMBERS)
	# activ_nums = getDataRange(stub, dtime = 22)  # получение телематических данных за период
	getDataRange(stub, dtime = 33)  # получение телематических данных за период
	print('Len NUMBERS:\t', len(NUMBERS))
	print('Len activ_nums:\t', len(activ_nums))
	getDataStream(stub, activ_nums)
	# '''


import threading
if __name__ == '__main__':
	get_actual_ts()
	'''
	worker = threading.Thread(target = get_actual_ts)
	worker.start()
	print("#" * 33)
	for j in range(111):
		time.sleep(1)
		print('sleep:', j, end = '\r')
	threading._shutdown()
	'''
