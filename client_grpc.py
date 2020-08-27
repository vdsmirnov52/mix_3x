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
NUMBERS = ['р984кх152', 'р999кх152', 'ар31752', 'ау20652', 'м063ов152']
#DEVICES = []

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
	for o in rrr.Objects:
		print (o.StateNumber.upper())
		for d in o.Data:
			print (time.strftime("\t%d-%m-%Y %T", time.localtime(d.DeviceTime)), time.strftime("\t%d-%m-%Y %T ", time.localtime(d.ReceivedTime)), end = '\t')
			print (d.Position.Longitude, '\t', d.Position.Latitude, d.State.Mileage, "\tdt:", (d.ReceivedTime - d.DeviceTime))


def getDataStream (stub):
	""" подписка на поток телематических данных устройства  """
	Filter = Api_pb2.DataFilter(DeviceCode = DEVICES, StateNumber = NUMBERS)
	Fields = Api_pb2.FieldsToggle(Position = True)
	request = Api_pb2.ObjectsDataStreamRequest(Filter = Filter, Fields = Fields)
	#   ObjectsDataStreamRequest
	# rrr = stub.GetObjectsEventsStream(request)    # подписка на поток событий по устройствам
	rrr = stub.GetObjectsDataStream(request)
	print("\nSSS", type(request), type(rrr))    # help(rrr))
	print('NUMBERS', NUMBERS)
	tt = time.time()
	# '''
	for j in range(11):
		if rrr.done():
			print('\trunning:', rrr.running())
			break
		try:
			# res = rrr.debug_error_string()
			# help(rrr)
			res = rrr.result(timeout=111)
			if res:
				print("\tresult", j, res, rrr.details())
				# break
			else:
				print('res:', res, j)
				time.sleep(3)
		except:
			print('except', sys.exc_info()[:2])
	# '''
	print("#"*11, int(time.time() - tt))
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


def test(stub):
	# help (stub.GetObjectsEventsStream)    # MultiThreadedRendezvous)
	# help(stub.GetObjectsState)
	'''
	for j in range(2):
		print (j, "#"*22)
		getState(stub)      # Текущее состояние
		time.sleep(7)
	'''
	getDataRange(stub, dtime = 22)  # получение телематических данных за период
	
	
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
	# t1.actual_tss()
	# print(t1.get_ts_list(cols = ['date', 'gosnumber', 'time']))
	NUMBERS = t1.get_ts_list(cols = 'gosnumber')
	print('Len NUMBERS:', len(NUMBERS))
	# '''
	cname = list_canals[2]
	print('\n\tОткрываем канал и создаем клиент:\t', cname)
	channel = grpc.insecure_channel(cname)  # 'rnis-api.rnc52.ru:6161')
	stub = Api_pb2_grpc.APIStub(channel)
	getInfo(stub)
	# test(stub)
	# getDataStream(stub)
	# '''
