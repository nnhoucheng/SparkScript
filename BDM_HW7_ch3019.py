from pyspark import SparkContext
from datetime import datetime as dt

def distance(origin, destination):
    import math
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371 # km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c # km
    d = d * 0.621371 # mile

    return d

def filter_yellow(partId, list_of_records):
    if partId==0: 
        list_of_records.next() # skipping the first line
    import csv
    reader = csv.reader(list_of_records)
    sla = 40.73901691
    slon = -74.00263761
    for row in reader:
        day_time = row[1].split('.')[0]
        if day_time.split(' ')[0] == '2015-02-01':
            try:
                yla = float(row[4])
                ylon = float(row[5])
            except ValueError:
                continue
            if distance((sla, slon), (yla, ylon)) <= 0.25:
                yield (day_time)
                
def filter_trip(partId, list_of_records):
    if partId==0: 
        list_of_records.next() # skipping the first line
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        day_time = row[3].split('+')[0]
        if row[6] == 'Greenwich Ave & 8 Ave' and day_time.split(' ')[0] == '2015-02-01':
            yield (day_time)

if __name__ == '__main__':
    sc = SparkContext()
    yellow = sc.textFile('/tmp/yellow.csv.gz')
    citibike = sc.textFile('/tmp/citibike.csv')
    yellowtrips = yellow.mapPartitionsWithIndex(filter_yellow).collect()
    trips = citibike.mapPartitionsWithIndex(filter_trip).collect()
    trips = map(lambda x: dt.strptime(x, '%Y-%m-%d %H:%M:%S'), trips)
    S = 0
    for yt in yellowtrips:
        yt_ = dt.strptime(yt, '%Y-%m-%d %H:%M:%S')
        for t in trips:
            if t > yt_:
                delta = t - yt_
                if delta.seconds < 600:
                    S += 1
    print '\n'*10
    print S
    with open('tmp.txt', 'wb') as fo:
        fo.write(S)