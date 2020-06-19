#!/usr/bin/python -u
# -*- coding: utf-8 -*-
""" 	"""

import  time
import	grpc
import	Api_pb2
import	Api_pb2_grpc

DEVICES = ["157231", "1284891", "1284890", "1323475"]
#NUMBERS = ["н756се750", "н851се750", "ат59652"]
# Active T1
NUMBERS = ['р984кх152', 'р999кх152', 'ар31752']
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
		print (o.StateNumber, o.Data[0].DeviceTime, time.strftime("\t%d-%m-%Y %T", time.localtime(o.Data[0].DeviceTime)))
		print ("\tLon: %s\tLat: %s\tCourse: %s" % (o.Data[0].Position.Longitude, o.Data[0].Position.Latitude, o.Data[0].Position.Course))
		# print (o.Data[0].Position)
		# print (o.Data)

def getDataRange (stub):
	""" получение телематических данных за период   """
	itm = int(time.time())
	itmf = itm-600
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
		print (o.StateNumber)
		for d in o.Data:
			print ( time.strftime("\t%d-%m-%Y %T", time.localtime(d.DeviceTime)), time.strftime("\t%d-%m-%Y %T ", time.localtime(d.ReceivedTime)))
			print (  d.Position.Longitude, '\t', d.Position.Latitude, d.State.Mileage)
		print()


def getDataStream (stub):
	""" подписка на поток телематических данных устройства  """
	Filter = Api_pb2.DataFilter(DeviceCode = DEVICES, StateNumber = NUMBERS)
	Fields = Api_pb2.FieldsToggle(Position = True)
	request = Api_pb2.ObjectsDataStreamRequest(Filter = Filter, Fields = Fields)
	#   ObjectsDataStreamRequest
	# rrr = stub.GetObjectsEventsStream(request)    # подписка на поток событий по устройствам
	rrr = stub.GetObjectsDataStream(request)
	print ("SSS", rrr)    # help(rrr))
	# rrr.details()       # Висим тихо
	print ('running', rrr.running())     # finished with exit code 0
	print ('add_done_callback\t', rrr.add_done_callback(callback))       # Висим тихо
	# print ('initial_metadata\t', rrr.initial_metadata())       # Висим тихо
	print ('debug_error_string\t', rrr.debug_error_string())       # Висим тихо
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


# открываем канал и создаем клиент
# channel = grpc.insecure_channel('rnis-tm.t1-group.ru:18082')
channel = grpc.insecure_channel('rnis-api.rnc52.ru:6161')
stub =	Api_pb2_grpc.APIStub(channel)

# help (stub.GetObjectsEventsStream)    # MultiThreadedRendezvous)
# print (help(stub.GetObjectsState)
for j in range(5):
	print (j, "#"*22)
	getState(stub)      # Текущее состояние
	time.sleep(11)
# getDataRange(stub)  # получение телематических данных за период
# getDataStream(stub)
# getInfo(stub)

''' gRPC	'р984кх152', 'р999кх152', 'ар31752'
apis = Api_pb2_grpc.APIServicer()
help(apis.GetObjectsState)
help(apis.GetObjectsDataRange)
ostate	= Api_pb2_grpc.GetObjectsState(channel)
odrange = Api_pb2_grpc.GetObjectsDataRange(channel)

print (ostate)
print (odrange)
'''
