#!/usr/bin/env python
"""	Обрабоика файла data_cv19.csv и/или Формирование файлов:
	- map.html виблиотека folium
	- cv19.db БД SQLite
"""

import os, sys, time
import dblite3
import folium
from folium.plugins import MarkerCluster

def ppoint(spoint):
	""" Разобрать строку с координатами lat, lon
	форматы: '"61.28369,73.41383"' '58.4937 49.7429'
	"""
	try:
		if spoint[0] == '"' and spoint[-1] == '"':	spoint = spoint[1:-1]
		sss = spoint.split(',')
		if len(sss) == 2:
			return float(sss[0]), float(sss[1])
		if len(sss) == 3:
			sss = spoint.split(' ')
			return float(sss[0].replace(',', '.')), float(sss[1].replace(',', '.'))
		sss = spoint.split(' ')
		return float(sss[0]), float(sss[1])
	except:
		pass

#Function to change colors
def color_change(elev):
	if(elev < 1000):
		return('green')
	elif(1000 <= elev <3000):
		return('orange')
	else:
		return('red')

def	stm_parce (stm):
	""" Определить время события
	:param stm: строка data time форматы:
	:return: int localtime
	"""
	tm = None
	try:
		tm = time.strptime(stm, "%d.%m.%y %H:%M")
	except:
		try:
			if len(stm) > 24 and stm[20:24] == '000+':
				tm = time.strptime(stm[:19], "%Y-%m-%dT%H:%M:%S")
			else:
				tm = time.strptime(stm, "%d.%m.%Y %H:%M")
		except:	pass
	finally:
		if tm:		stm = time.strftime("%d.%m.%Y %T", tm)
		return tm, stm

def	data_parce(file_name, **kwargs):
	html_out = kwargs.get('html')
	dbid = None
	if html_out:
		#Create base map
		map = folium.Map(location=[56.318060,43.999216], zoom_start = 11)
		#Create Cluster
		marker_cluster = MarkerCluster().add_to(map)
	sql_out = kwargs.get('sqlite')
	if sql_out:
		# if not os.path.isfile(sql_out):
		qinit = "CREATE TABLE covin19 (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, tm INTEGER, lat FLOAT, lon FLOAT, rem VARCHAR(64));"
		dbid = dblite3.dblite(sql_out)
		print(qinit, dbid.execute(qinit))

	f = open(file_name, 'rt')
	line = 'Start'
	elevation = 100
	i = 0
	while line:
		line = f.readline().strip()
		if not line[:2].isdigit():	continue
		stm, sp = line.split(',', 1)
		point = ppoint(sp)
		tm, stm = stm_parce(stm)
		if not point:	continue
		print(point, '\t', stm)
		lat, lon = point
		# folium.Marker(location=[lat, lon], popup=stm, icon=folium.Icon(color='grey')).add_to(map)
		i += 1
		if html_out:
			elevation += 100
			folium.CircleMarker(location=[lat, lon], radius=9, popup=stm,
								fill_color=color_change(elevation), color="gray", fill_opacity=0.9).add_to(marker_cluster)
		if dbid:
			itm = int(time.mktime(tm)) if tm	else i*1000
			# print("""INSERT INTO covin19 (tm, lat, lon, rem) VALUES (%s, %s, %s, '%s')""" % (itm, lat, lon, stm))
			dbid.execute("""INSERT INTO covin19 (tm, lat, lon, rem) VALUES (%s, %s, %s, '%s')""" % (itm, lat, lon, stm))
			# dbid.execute("""INSERT INTO covin19 (id, tm, lat, lon, rem) VALUES (%s, %s, %s, %s, "%s")""" % (i, i*1000, lat, lon, stm))
			if dbid.last_error:
				print("ERROR:", dbid.last_error)
				break
	f.close()

	if html_out:	map.save('/home/smirnov/HTML/map.html')


if __name__ == '__main__':
	# stm_parce("11.04.20 3:24")
	# stm_parce("2020-04-11T12:35:01.000+03:00")
	# '''
	data_file = r'/home/smirnov/DATAs/data_cv19.csv'
	if os.path.isfile(data_file):
		# data_parce (data_file)
		data_parce (data_file, html=r'/home/smirnov/HTML/map.html')
		# data_parce (data_file, sqlite=r'/home/smirnov/DATAs/cv19.db')
	else:	print("\tОтсутствует файл c данными:", data_file)
	# '''