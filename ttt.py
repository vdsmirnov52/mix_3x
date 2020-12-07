""" Временные инструменты """
import sys, time
import dbtools3

# gn_list = ['м643сх152', 'нн981152', 'м586хр152', 'н686рн152', 'е207сн152', 'м849тх152', 'м949хн152', 'м404хк152', 'а201мм52', 'м850тх152', 'м627сх152', 'м408хк152']
gn_list = ['с458ко152', 'а097мк152', 'резерв41483515', '0791нт52', '9906нн52', '4982нр52', 'м276хн152', 'м113ас152', 'м120ас152', 'а376ас52', '4639нн52', '9918нн52', 'с150кх152', 'в747еу152', 'м117ас152', 'м121ас152', 'а241св52']
# for gn in gn_list:	print(gn.upper(), end=', ')
count_activ = 0


def	check_t12receiver():
	idb_t1 = dbtools3.dbtools("host=10.10.2.241 dbname=contracts port=5432 user=smirnov")
	idb_rcv = dbtools3.dbtools("host=10.10.2.241 dbname=receiver port=5432 user=smirnov")

	def	is_activ(id_dev, inn, gosnum):
		global count_activ
		query = "SELECT * FROM recv_ts WHERE gosnum = '%s'" % gosnum
		dts = idb_rcv.get_dict(query)
		if dts:
			if id_dev == dts['device_id']:
				count_activ += 1
				return	dts['rem']
			if dts['rem']:
				count_activ += 1
				return	dts['device_id'], dts['rem']
			return	dts['device_id'], False
		else:	return "Not TS"

	curr_tm = int(time.time())
	query = "SELECT id_dev, inn, gosnum FROM t1_atts WHERE inn IN (SELECT inn FROM t1_orgs WHERE bm_ssys = 131072) AND last_tm > %s;" % (curr_tm - 172800)
	rows = idb_t1.get_rows(query)
	d = idb_t1.desc
	for r in rows:
		# break
		id_dev, inn, gosnum = r
		print("%18s %16s\t%s" % (r[d.index('id_dev')], r[d.index('gosnum')], r[d.index('inn')]), is_activ(id_dev, inn, gosnum))
	print("len rows:", len(rows))
	print("count_activ:", count_activ)


from queue import Queue
from threading import Thread


FL_BREAK = False
QDATAS = None
import os

def	getQueue():
	jem = 0
	while not FL_BREAK:
		if QDATAS.empty():
			time.sleep(1)
			jem += 1
			print("QDATAS.empty", jem)
			if jem > 6:
				print("exit getQueue")
				signal.alarm(2)
				# sys.exit()
				# os._exit(2)
			continue
		jem = 0
		data = QDATAS.get()
		print(data, time.strftime ("%T", time.localtime(data[1])))
	print("#" * 33, "Finish getQueue")


import random
def	mmm():
	print('Генерим очередь сообщений', random.random())

	j = 0
	try:
		while j < 11:
			t = time.time()
			QDATAS.put([j, t])
			j += 1
			time.sleep(int(10*random.random()))
	except IOError:
		print("Break mmm")

import signal

def	queue_alarm(signum, frame):
	print("queue_alarm signum:", signum, frame)
	signal.alarm(0)
	raise IOError("queue_alarm")

if __name__ == '__main__':
	# check_t12receiver ()
	# while True:
	# 	pid = os.fork()
	# 	if pid == 0:
			signal.signal(signal.SIGALRM, queue_alarm)
			QDATAS = Queue()
			try:

				ttt = Thread(target=getQueue, args=())
				ttt.start()
				while 1:
					mmm()
					print("exit mmm", QDATAS.qsize())
					time.sleep(11)
			except:
				print("MAIN except", sys.exc_info()[:2])
			FL_BREAK = True
		# else:
		# 	try:
		# 		os.wait()
		# 	except KeyboardInterrupt:
		# 		print('#'*33, pid, FL_BREAK)
		# 		sys.exit()
	# print("Q"*44)