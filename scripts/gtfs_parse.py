"""Calc headways and travel times from gtfs network

parse5 - added all routes option & merged output tables
parse6 - added nowrite for debugging
         squashed bug where same-stop transfers not written if stop in
           transfer table
"""

import csv
import psycopg2
import calendar
import time
from datetime import datetime
from pprint import pprint

#### user parameters
nowrite = False
min_time = '06:00'
max_time = '10:00'
dayofweek = 'Mon'   # first 3 chars of day name, e.g. 'Mon'
route_set = None # set(['9', '19']) # set(['58', '68', '20', '6'])   # None for all routes OR e.g. set(['58', '68', '20', '6'])
table_nm = 'tran_links_0600_1000_mon'
####

if nowrite:
    print 'Running in NOWRITE mode!'

t0 = time.time()
# connect to postgis db
conn = psycopg2.connect(host='localhost', database='scratch', user='postgres',
                        password='', port=5439)
cur = conn.cursor()

# parse day of week from calendar_dates.txt codes
daysofweek = {}
day_names = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
with open('calendar_dates.txt', 'rb')  as f:
    rows = csv.DictReader(f, restkey='extra')
    for r in rows:
        code = r['service_id']
        pydate = datetime.strptime(r['date'], '%Y%m%d')
        #print r['date'], pydate
        y = r
        if code not in daysofweek:
            daysofweek[code] = set()
        day = pydate.weekday()  # Mon=0...Sun=6
        daysofweek[code].add(day_names[day])
        
##print 'Service code lookup table:'
##for code, days in daysofweek.iteritems():
##    print code, days

# load walk network nodes (external to gtfs)


# load stop locations
##stops = {}
##with open('stops.txt', 'rb')  as f:
##    rows = csv.DictReader(f, restkey='extra')
##    for r in rows:
##        stop = r['stop_id']
##        lat = r['stop_lat']
##        lon = r['stop_lon']
##        stops[stop] = (float(lat), float(lon))
##    print 'read {} stops'.format(len(stops))
##w = csv.writer(open('stops.csv', 'wb'))
##w.writerow(['stop_id, stop_lat, stop_lon'])
##for stop, (lat, lon) in sorted(stops.iteritems()):
##    w.writerow([stop, lat, lon])

# load transfer table
transfers = {}
with open('transfers.txt', 'rb')  as f:
    rows = csv.DictReader(f)
    for r in rows:
        fstop = r['from_stop_id']
        tstop = r['to_stop_id']
        if fstop not in transfers:
            transfers[fstop] = []
        transfers[fstop].append(tstop)
    print 'read {} transfers'.format(len(transfers))

# load trip to route/dir lookup table, inlcuding service_id
trips = {}
routes = {}  # keep track of routes to avoid multiple service ids per route
with open('trips.txt', 'rb')  as f:
    rows = csv.DictReader(f)
    for r in rows:
        route = r['route_id']
        service_id = r['service_id']
        if (not route_set or route in route_set) and dayofweek in daysofweek[service_id]:
            trip = r['trip_id']
            drct = r['direction_id']
            if trip in trips:
                # Note: this shouldn't happen in GTFS data
                print 'duplicate trip_id found for {}'.format(trip)
                raw_input()
            # Allow only one service_id per route
            # TODO make this smarter than random assignment of active
            #   service id
            if route not in routes or service_id == routes[route]:
                trips[trip] = (route, drct, service_id)
                routes[route] = service_id
    print 'read {} trips'.format(len(trips))

# get stop-to-stop times & stop arrivals within time window per route

seg_times = {}
arrivals = {}
with open('stop_times.txt', 'rb')  as f:
    rows = csv.DictReader(f)
    this_trip = None
    for r in rows:
        trip = r['trip_id']
        # trip may not be in trips if doesn't run on dayofweek or
        # not in route_set
        if trip in trips: 
            if trip != this_trip:
                # reset prev stops & times
                fstop = None
                psec = None
            service_id = trips[trip][2]
            if dayofweek in daysofweek[service_id]:
                stop = r['stop_id']
                stop_seq = int(r['stop_sequence'])        
                arr = r['arrival_time']
                h, m, s = arr.split(':')
                arr_sec = int(h) * 3600 + int(m) * 60 + int(s)
                # TODO think of ways to use depaarture time
                #dep = r['departure_time']
                #h, m, s = dep.split(':')
                #dep_sec = int(h) * 3600 + int(m) * 60 + int(s)
                route = trips[trip][0]  # (route, drct, service_id)
                if stop_seq == 1:
                    pass
                else:
                    if fstop and (fstop, stop, route) not in seg_times:
                        seg_times[(fstop, stop, route)] = []
                    if arr < max_time and psec:
                        seg_times[(fstop, stop, route)].append(arr_sec - psec)
                # record headway stamp if within time period
                if (stop, route) not in arrivals:
                    arrivals[(stop, route)] = []
                if arr < max_time:
                    arrivals[(stop, route)].append(arr_sec)
                # remember previous stop and arrival time (secs)
                fstop = stop
                if arr >= min_time and arr < max_time:
                    psec = arr_sec
                else:
                    psec = None
            this_trip = trip

    print ('parsed {} arrivals between {} & {} on {}'
           .format(len(arrivals), min_time, max_time, dayofweek))
    print ('parsed {} segment times between {} & {} on {}'
           .format(len(seg_times), min_time, max_time, dayofweek))

# calc average headways & stop to stop travel times per route
seg_avg_time = {}
for k, times in seg_times.iteritems():
    if len(times) > 0:
        seg_avg_time[k] = sum(times) / len(times)
    else:
        # Note: segment will be missing for this period
        pass
print '{} seg avg. times'.format(len(seg_avg_time))

headways = {}
# calc period time to use if no or only one arrival
h, m = min_time.split(':')
min_time_sec = int(h) * 3600 + int(m) * 60
h, m = max_time.split(':')
max_time_sec = int(h) * 3600 + int(m) * 60
period_sec = max_time_sec - min_time_sec
n_headways = 0
for (stop, route), times in arrivals.iteritems():
    if stop not in headways:
        headways[stop] = {}
    if route not in headways[stop]:
        headways[stop][route] = 0
    if len(times) <= 1:
        headways[stop][route] = period_sec
    else:
        n_headways += 1
        ptime = None
        for t in times:
            if ptime:
                headways[stop][route] += t - ptime
            ptime = t
        headways[stop][route] = headways[stop][route] / (len(times) - 1)    
print '{} valid headways'.format(n_headways)



# Put it all together!
# 1) Create transit pseudo-links between stops by route
# Note: there will be overlapping links for routes serving same stop pairs
query = """
    CREATE TABLE IF NOT EXISTS data.{table_nm}
    (geom geometry(Linestring, 2913),
    length double precision,
    fstop integer,
    tstop integer,
    froute integer,
    troute integer,
    ttime integer,
    wtime integer,
    headway integer,
    f_zlev integer,
    t_zlev integer,
    oneway integer);
    """.format(table_nm=table_nm)
cur.execute(query)

for (fstop, stop, route) in seg_times:
    try:
        ttime = seg_avg_time[(fstop, stop, route)]
    except KeyError:
        # set missing times to arbitrarily high cost
        ttime = 999999
    query ="""
        WITH stop1 AS (
          SELECT geom
          FROM data.stops
          WHERE loc_id=%s
          LIMIT 1),
        stop2 AS (
          SELECT geom
          FROM data.stops
          WHERE loc_id=%s
          LIMIT 1)
        INSERT INTO data.{table_nm}
        SELECT ST_MakeLine(ARRAY[stop1.geom, stop2.geom]),
          ST_Distance(stop1.geom, stop2.geom) as length,
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        FROM stop1, stop2
        """.format(table_nm=table_nm)
    #lat1, lng1 = stops[fstop]
    #lat2, lng2 = stops[stop]
    froute = troute = int(route)
    wtime = 0
    headway = 0
    f_zlev = froute  # use route numbers as pseudo node elevations
    t_zlev = troute
    oneway = 1
    data = [fstop, stop, fstop, stop, froute, troute, ttime,
            wtime, headway, f_zlev, t_zlev, oneway]
    if not nowrite:
        cur.execute(query, data)

# 2) Create defined connector links between transit stops

n_xfers = 0
for fstop, tstops in transfers.iteritems():
    for tstop in tstops:
        # skip same stop transfers for now (handle later)
        if fstop in headways and fstop != tstop:
            for froute in headways[fstop]:
                froute = int(froute)
                if tstop in headways:
                    for troute in headways[tstop]:
                        headway = headways[tstop][troute]
                        query = """
                        WITH stop1 AS (
                          SELECT geom
                          FROM data.stops
                          WHERE loc_id=%s
                          LIMIT 1),
                        stop2 AS (
                          SELECT geom
                          FROM data.stops
                          WHERE loc_id=%s
                          LIMIT 1)
                            INSERT INTO data.{table_nm}
                            SELECT ST_MakeLine(ARRAY[stop1.geom, stop2.geom]),
                              ST_Distance(stop1.geom, stop2.geom) as length,
                              %s,%s,%s,%s,0 as ttime,
                              ST_Distance(stop1.geom, stop2.geom)/(5280*3.0)*3600 as wtime,
                              %s,%s,%s,%s
                            FROM stop1, stop2
                            """.format(table_nm=table_nm)
                        troute = int(troute)
                        f_zlev = froute
                        t_zlev = troute
                        oneway = 1
                        data = [fstop, tstop, fstop, tstop, froute,
                                troute, headway, f_zlev, t_zlev, oneway]
                        if not nowrite:
                            cur.execute(query, data)
                        n_xfers += 1
                else:
                    pass
print '{} defined transfers (excluding intra-stop)'.format(n_xfers)                

# Add same-stop transfers
n_xfers = 0
for fstop in headways:
    tstop = fstop
    for froute in headways[fstop]:
        for troute in headways[tstop]:
            #if froute != troute and (fstop not in transfers or tstop not in transfers[fstop]):
            if froute != troute:
                headway = headways[tstop][troute]
                query = """
                  WITH stop AS (
                      SELECT geom
                      FROM data.stops
                      WHERE loc_id=%s
                      LIMIT 1),
                    jitter AS (
                      SELECT ST_Translate(geom, -10, 10) g1,
                        ST_Translate(geom, 10, 10) g2  
                      FROM stop)
                    INSERT INTO data.{table_nm}
                    SELECT ST_MakeLine(ARRAY[stop.geom, jitter.g1, jitter.g2,
                      stop.geom]) geom, 0 length,
                      %s fstop, %s tstop, %s froute, %s troute, 0 ttime, 0 as wtime,
                      %s headway, %s f_zlev, %s t_zlev, %s oneway
                    FROM stop, jitter
                    """.format(table_nm=table_nm)
                f_zlev = froute
                t_zlev = troute
                oneway = 1
                data = [fstop, fstop, tstop, int(froute), int(troute),
                        headway, f_zlev, t_zlev, oneway]
                if not nowrite:
                    cur.execute(query, data)
                n_xfers += 1
print '{} implied same-stop transfers'.format(n_xfers)             

# Add transit/walk transfers by creating connectors from each stop to
#   nearest two walk nodes.
# TODO speed this up (currently ~10-30s/100 connectors!)
# tran -> walk
ii = 0
for stop in headways:
    ii += 1
    if ii % 100 == 0:
        print '{} connectors of {}'.format(ii, len(headways))
    for route in headways[stop]:
        headway = 0   # from transit no time cost
        # http://workshops.boundlessgeo.com/postgis-intro/knn.html
        query = """
            --need distinct on because stops can appear more than once in table
            WITH stop as (SELECT DISTINCT ON (loc_id) loc_id, geom
              FROM data.stops
              WHERE loc_id=%s),
            nearest2 as (
              SELECT walk.geom wgeom, walk.zelev, stop.geom sgeom
              FROM data.RLIS_walk_nodes walk, stop
              ORDER BY walk.geom <-> stop.geom
              LIMIT 2)
              
            INSERT INTO data.{table_nm} (
            SELECT ST_MakeLine(ARRAY[sgeom, wgeom]),
              ST_Distance(sgeom, wgeom) as length,
              %s,999999 as tstop,%s,999999 as troute, 0 as ttime,
              ST_Distance(sgeom, wgeom)/(5280*3.0)*3600 as wtime,
              %s,%s,zelev as t_zlev,%s
            FROM stop, nearest2)
            """.format(table_nm=table_nm)
        #lat1, lng1 = stops[stop]
        f_zlev = int(route)
        oneway = 1
        data = [int(stop), int(stop), int(route), headway,
                f_zlev, oneway]
        if not nowrite:
            cur.execute(query, data)
        n_xfers += 1
print '{} tran -> walk transfers'.format(n_xfers)
n_xfers = 0
# walk -> tran
n_xfers = 0
ii = 0
for stop in headways:
    ii += 1
    if ii % 100 == 0:
        print '{} connectors of {}'.format(ii, len(headways))
    for route in headways[stop]:
        headway = headways[stop][route] 
        query = """
            WITH connectors as (
              SELECT geom, length, t_zlev as zelev, wtime
              FROM data.{table_nm}
              WHERE fstop=%s and tstop=999999
              LIMIT 2)
              
            INSERT INTO data.{table_nm} (
            SELECT ST_REVERSE(geom), connectors.length,
              999999 as fstop,%s,999999 as froute,%s,0 as ttime,
              connectors.wtime,
              %s,connectors.zelev as f_zlev,%s,%s
            FROM connectors)
            """.format(table_nm=table_nm)
        #lat1, lng1 = stops[stop]
        t_zlev = int(route)
        oneway = 1
        data = [int(stop), int(stop), int(route), headway,
                t_zlev, oneway]
        if not nowrite:
            cur.execute(query, data)
        n_xfers += 1
print '{} walk -> tran transfers'.format(n_xfers) 

# Add a unique id field
query = """
    ALTER TABLE data.{table_nm}
    add gid serial unique;
    """.format(table_nm=table_nm)
cur.execute(query)

conn.commit()
conn.close()

t1 = time.time()
print '{}s elapsed'.format(int(t1 - t0))
