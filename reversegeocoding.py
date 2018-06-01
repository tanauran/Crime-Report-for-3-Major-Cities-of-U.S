import csv
from decimal import *
from functools import partial
from ast import literal_eval as make_tuple

class ReverseGeoCoding:
  def setup_lookup(self):
    self.lookup = {}
    self.coordinates = {}
    self.latArray = []
    with open('US.txt','rb') as tsvin:
      tsvin = csv.reader(tsvin, delimiter='\t')
      for row in tsvin:
        key = row[9]+":"+row[10]
        value = row[1]+"$$$"+row[2]+"$$$"+row[4]
        self.lookup[key] = value
        lat = float(format(Decimal(row[9]), '3.4f'))
        long = float(format(Decimal(row[10]), '3.4f'))
        self.coordinates[lat] = long
        self.latArray.append(lat)

geocodelookup = ReverseGeoCoding()
geocodelookup.setup_lookup()
geocodelookup.latArray.sort()

def listclamp2(nlist, minn, maxn):
  return sorted([x for x in nlist if (minn <= x <= maxn)])


def zipcode_lookup(x):
  dist=lambda s,d: (s[0]-d[0])**2+(s[1]-d[1])**2
  if x is None:
    return ['NONE','NONE','NONE']
  try:
    y = ''.join([chr(c) for c in x])
    coordstr = make_tuple(y)
  except ValueError:
    return ['NONE','NONE', str(y)]

  coordlat = float(format(Decimal(coordstr[0]), '3.4f'))
  coordlong = float(format(Decimal(coordstr[1]), '3.4f'))
  range_1 = []
  range_1 = listclamp2(geocodelookup.latArray, coordlat-0.5, coordlat+0.5)
  if not range_1:
    range_1 = listclamp2(geocodelookup.latArray, coordlat-2, coordlat+2)

  if not range_1:
    return ['NONE','NONE', str(y)]
  coordinate_pairs = [(k,geocodelookup.coordinates[k]) for k in range_1 if k in geocodelookup.coordinates]
  knowncode = min(coordinate_pairs, key=partial(dist, (coordlat,coordlong)))
  key = str(knowncode[0]) + ":" + str(knowncode[1])
  values = geocodelookup.lookup[key].split('$$$')
  return [values[0],values[1],values[2]]
