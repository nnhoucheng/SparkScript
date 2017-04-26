from pyspark import SparkContext
from datetime import datetime as dt


def distance(origin, destination):
    '''
    Compute distance between two points given their latitude and longitude.
    Original code from: https://gist.github.com/rochacbruno/2883505
    '''
    import math
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371 # earth radius in km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    d = radius * c # disatnce in km
    d = d * 0.621371 # distance in mile

    return d


def filter_yellow(partId, list_of_records):
    '''
    Filter yellow taxi trips not within 0.25 miles of the station "Greenwich Ave & 8 Ave" 
                                 or at "February 1st 2015"
    '''
    import csv
    sla = 40.73901691 # latitude of the station
    slon = -74.00263761 # longitude of the station
    
    if partId==0: 
        list_of_records.next() # skipping the first line
        
    reader = csv.reader(list_of_records)
    for row in reader:
        day_time = row[1].split('.')[0] # uniform the time format to yyyy-mm-dd hh:mm:ss
        if day_time.split(' ')[0] == '2015-02-01': # filter trips not at "February 1st 2015"
            # some trips miss information of dropoff location
            try:
                yla = float(row[4])
                ylon = float(row[5])
            except ValueError:
                continue
                
            if distance((sla, slon), (yla, ylon)) <= 0.25: # Filter trips not within 0.25 miles
                yield (day_time)

                
def filter_citibike(partId, list_of_records):
    '''
    Filter citibike trips not start at the station "Greenwich Ave & 8 Ave" 
                              or at "February 1st 2015"
    '''
    import csv
    if partId==0: 
        list_of_records.next() # skipping the first line
        
    reader = csv.reader(list_of_records)
    for row in reader:
        day_time = row[3].split('+')[0] # uniform the time format to yyyy-mm-dd hh:mm:ss
        if row[6] == 'Greenwich Ave & 8 Ave' and day_time.split(' ')[0] == '2015-02-01':
            yield (day_time)


def filter_pair(list_of_records):
    '''
    Keep records of the citibike trip starting after the taxi trip ended 
                                      and no more than 10 minutes after that.
    '''
    for pair in list_of_records:
        (yt, ct) = pair
        if ct > yt:
            if (ct - yt).seconds <= 600: # 10 mins equals 600 seconds
                yield (1)
   
    
if __name__ == '__main__':
    sc = SparkContext()
    
    # Read the data
    yellow = sc.textFile('/tmp/yellow.csv.gz')
    citibike = sc.textFile('/tmp/citibike.csv')
    
    yellowtrips = yellow.mapPartitionsWithIndex(filter_yellow) \
                        .map(lambda x: dt.strptime(x, '%Y-%m-%d %H:%M:%S')) \
                        .collect()
            
    citibiketrips = citibike.mapPartitionsWithIndex(filter_citibike) \
                            .map(lambda x: dt.strptime(x, '%Y-%m-%d %H:%M:%S')) \
                            .collect()
    
    # Calculate the matched pairs
    S = yellowtrips.cartesian(citibiketrips).mapPartitions(filter_pair).count()
    
    ## print 10 blank line to find result at the command line window
    print '\n'*10            
    print S