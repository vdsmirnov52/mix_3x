#!/usr/bin/python -u
# -*- coding: utf-8 -*-
""" gRPC прием данных от Т1 (TEST)	"""

import  time, sys
import	grpc
import	Api_pb2
import	Api_pb2_grpc

DEVICES = ["157231", "1284891", "1284890", "1323475", "24964441", "353451048047755"]
#NUMBERS = ["н756се750", "н851се750", "ат59652"]
# Active T1
# NUMBERS = ['р984кх152', 'р999кх152', 'ар31752', 'ау20652', 'м063ов152']
NUMBERS = ['a422aa152', 'a523aa152', 'a592aa152', 'а421аа52', 'а423аа152', 'а639кр152', 'а642кр152', 'в296на152', 'е013тк152', 'е588ху152',
            'е995та152', 'к623хо152', 'м385тн152', 'н044нн52', 'н046нн52', 'н154нн52', 'н204ао152', 'о197ур152', 'р202еу152', 'х555рс52']
''' NUMBERS = ['а908ко152', 'о014рр52', 'о146аа52', 'о632ма52', 'т415хк52', 't006', 't009', 't017', 't019', 't035', 't051', 't054', 't055', 't056',
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
	itm = int(time.time())
	itmf = itm-dtime if dtime and dtime > 10 else itm-600
	Filter = Api_pb2.DataFilter(DeviceCode = DEVICES, StateNumber = NUMBERS, DateFrom = itmf, DateTo = itm)
	Fields = Api_pb2.FieldsToggle(Position=True)
	request = Api_pb2.ObjectsStateRequest(Filter = Filter, Fields = Fields)
	rrr = stub.GetObjectsDataRange (request)
	print ("RRR", type(rrr))
	"""
	DeviceTime  - врема передачи прибором
	ReceivedTime - время приема данных сервером
	"""
	activ_num = []
	for o in rrr.Objects:
		if o.Data:
			print (o.StateNumber.upper())
			activ_num.append(o.StateNumber)
		for d in o.Data:
			print (time.strftime("\t%d-%m-%Y %T", time.localtime(d.DeviceTime)), time.strftime("\t%d-%m-%Y %T ", time.localtime(d.ReceivedTime)), end = '\t')
			print (d.Position.Longitude, '\t', d.Position.Latitude, d.State.Mileage, "\tdt:", (d.ReceivedTime - d.DeviceTime))
	return activ_num


def getDataStream (stub, numbers = None):
	""" подписка на поток телематических данных устройства  """
	StateNumber = numbers if numbers else NUMBERS
	print('StateNumber:\t', len(StateNumber))
	Filter = Api_pb2.DataFilter(DeviceCode = None, StateNumber = StateNumber)   #numbers if numbers else NUMBERS)
	# Filter = Api_pb2.DataFilter(DeviceCode = DEVICES, StateNumber = None)   #numbers if numbers else NUMBERS)
	Fields = Api_pb2.FieldsToggle(Position = True)
	request = Api_pb2.ObjectsDataStreamRequest(Filter = Filter, Fields = Fields)
	datas = stub.GetObjectsDataStream(request)
	print("\nSSS request: %s, ", type(request), type(datas))
	# help(datas);    return
	print('Len StateNumber: %d\n' % len(StateNumber), StateNumber)
	tt = time.time()
	j = 0
	try:
		for point in datas:
			# print(">>> %s" % point)
			dTime = (point.ReceivedTime-point.DeviceTime)
			print("%16s  DeviceTime: %s  dT:%6d \t" % (point.DeviceCode, time.strftime('%d.%m.%Y %T', time.localtime(point.DeviceTime)), dTime),
			      point.Position.Longitude, point.Position.Latitude, point.Position.Course, point.Position.Speed)
			#, point.Position, point.State)
			j += 1
			# if j > 3:				break
		print('Finish', datas)
	except:
		print('except:\t', sys.exc_info()[:2])
		print(time.strftime('\t%d.%m.%Y %T', time.localtime(tt)))
	
	print('\trunning:', datas.running())
	print(time.strftime('Start:\t%d.%m.%Y %T', time.localtime(tt)))
	print(time.strftime('Stop:\t%d.%m.%Y %T', time.localtime(time.time())))
	return

	print("#"*11, j, int(time.time() - tt))
	print("details\t", rrr.details())       # Висим тихо
	# print('add_done_callback\t', rrr.add_done_callback(callback))       # Висим тихо
	# print('initial_metadata\t', rrr.initial_metadata())       # Висим тихо
	# print('trailing_metadata\t', rrr.trailing_metadata())
	print('debug_error_string\t', rrr.debug_error_string())       # Висим тихо
	# print (rrr.result(timeout=1000))    # Висим тихо
	# print (rrr.traceback(timeout=1000))    # Висим тихо


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
	'''
	aaa = """A422AA152	A523AA152	A592AA152	А421АА52	А423АА152	А639КР152	А642КР152	В296НА152
Е013ТК152	Е588ХУ152	Е995ТА152	К623ХО152	М385ТН152	Н044НН52	Н046НН52	Н154НН52
Н204АО152	О197УР152	Р202ЕУ152	Х555РС52"""
	# ''
	aaa = """А908КО152	О014РР52	О146АА52	О632МА52	Т415ХК52	t006	t009	t017
t019	t035	t051	t054	t055	t056	t057	t059
t060	t061	t062	t063	t064	t065	t066	t067
t068	t069	t070	t072	t075	t077	t078	t079
t080	t081	t082	А003ЕХ152	А012ЕТ152	А858ЕТ152	АО92052	АО92152
АО92352	АО92452	АР91852	АР92252	АР99752	АТ47652	АУ48652	АУ75952
АУ76052	АУ76152	АУ76252	АУ76352	АУ76452	АУ76552	АУ76652	АУ76752
АУ76852	АУ76952	АУ77052	В293ВК152	О013ЕС52	О433МТ52	О531МЕ52	С128ЕА152
С135ЕА152	С421АТ152	С427АТ152	С467АТ152	С482АТ152	С486АТ152	С488АТ152	С489АТ152
С491АТ152	С497АТ152	С498АТ152	С504АТ152	С505АТ152	С507АТ152	С508АТ152	С511АТ152
С516АТ152	С517АТ152	С518АТ152	С521АТ152	С523АТ152	С562ЕА152	С568ЕА152	С570ЕА152
С629ЕА152	С638ЕА152	С642ЕА152	С644ЕА152	С649ЕА152	С650ЕА152	С655ВЕ152	С659ВА152
С677ЕА152	С684ЕА152	С702ВЕ152	С703ВЕ152	С705ВЕ152	С708ВЕ152	С711ВЕ152	С719ВЕ152
С723ВЕ152	С724ВЕ152	С725ВЕ152	С751ВЕ152	С753ВЕ152	С755ВЕ152	С756ВЕ152	С758ВЕ152
С760ВЕ152	С763ВЕ152	С765ВЕ152	С769ВЕ152	Т403ХК52	Т734ОТ52	Т741ОТ52"""
	for k in aaa.split():
		print("'%s'" % k.lower(), end = ', ')
	sys.exit()
	# '''
	
''' gRPC	'р984кх152', 'р999кх152', 'ар31752'
	apis = Api_pb2_grpc.APIServicer()
	help(apis.GetObjectsState)
	help(apis.GetObjectsDataRange)
	ostate	= Api_pb2_grpc.GetObjectsState(channel)
	odrange = Api_pb2_grpc.GetObjectsDataRange(channel)
	
	print (ostate)
	print (odrange)
	tags = Api_pb2_grpc.ResolveTag(channel)
	print('Tags', tags, type(tags), tags.ResolveTag('state'))
'''
import t1_requests as t1

list_canals = [
	'rnis-api.rnc52.ru:6161',	# Наша система
	'rnis-tm.t1-group.ru:18082',	# Разработчик Т1
	'10.10.21.22:6161',
]


if __name__ == '__main__':
	# test()
	# t1.actual_tss()
	# print(t1.get_ts_list(cols = ['date', 'gosnumber', 'time']))
	activ_nums = NUMBERS
	NUMBERS = t1.get_ts_list(cols = 'gosnumber')
	# '''
	cname = list_canals[2]
	print('\n\tОткрываем канал и создаем клиент:\t', cname)
	channel = grpc.insecure_channel(cname)  # 'rnis-api.rnc52.ru:6161')
	stub = Api_pb2_grpc.APIStub(channel)
	# getInfo(stub)
	activ_nums = getDataRange(stub, dtime = 22)  # получение телематических данных за период
	print('Len NUMBERS:\t', len(NUMBERS))
	print('Len activ_nums:\t', len(activ_nums))
	getDataStream(stub, activ_nums)
	# '''
