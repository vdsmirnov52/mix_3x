"""
    TEST:   Преобразование координат на глобусе
"""
from sys import argv
import math
import sph


if len(argv) == 2:
    script, fn = argv

    fp = open(fn, 'r')
    for line in fp:
        lat1, lon1, lat2, lon2 = map(float, line.split(" "))
        lat1 = math.radians(lat1)
        lon1 = math.radians(lon1)
        lat2 = math.radians(lat2)
        lon2 = math.radians(lon2)
        dist, azi2 = sph.inverse(lat2, lon2, lat1, lon1)
        dist, azi1 = sph.inverse(lat1, lon1, lat2, lon2)
        print ("%f %f %.4f" % (math.degrees(azi1), math.degrees(azi2), dist * sph.a_e))
    fp.close()
else:
    print(argv[0], """Отсутст файл с данными inv.dat
    30 0 52 54
    43.930203 56.346800 43.970883 56.333574    """)
print('#'*33)
# help(sph)