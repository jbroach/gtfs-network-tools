"""Show all trips by route for stop"""

import csv
import calendar
from datetime import datetime

min_time = '06:00'
max_time = '10:00'
dayofweek = 'Mon'   # first 3 chars of day name, e.g. 'Mon'
route_set = set(['58'])
stop_test = '9983'

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

# load trip to route/dir lookup table, inlcuding service_id
trips = {}
routes = {}  # keep track of routes to avoid multiple service ids per route
with open('trips.txt', 'rb')  as f:
    rows = csv.DictReader(f)
    for r in rows:
        route = r['route_id']
        service_id = r['service_id']
        if route in route_set and dayofweek in daysofweek[service_id]:
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
                if stop == stop_test:
                    print trip, fstop, stop, arr, arr_sec - psec
                fstop = stop
                if arr >= min_time and arr < max_time:
                    psec = arr_sec
                else:
                    psec = None
            this_trip = trip
            
