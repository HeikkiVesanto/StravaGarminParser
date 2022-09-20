import csv
import gzip
import fitparse
import xml.etree.ElementTree as ET
import os
import psycopg2
from psycopg2 import sql

# Usage: Put into the same folder as your unzipped stava data. Only works on Garmin file types, GPX, FIT, and TCX.
# Has requirements.

CONNECTION = psycopg2.connect(user="postgres",
                              password="postgres",
                              host="localhost",
                              port="5432",
                              database="postgis")

SCHEMA = 'strava'  # this should exist
TABLE = 'strava_pts2'

# Column names in activities.csv
f_id = 'Activity ID'
f_date = 'Activity Date'
f_type = 'Activity Type' 
f_path = 'Filename'
f_dist = 'Distance'


cursor = CONNECTION.cursor()

cursor.execute(
    sql.SQL("DROP TABLE if exists {schema}.{table}")
        .format(schema=sql.Identifier(SCHEMA), table=sql.Identifier(TABLE)))

CONNECTION.commit()

cursor.execute(
    sql.SQL("create table {schema}.{table}"
            "(uid SERIAL PRIMARY KEY NOT NULL,"
            "aid BIGINT,"
            "sid BIGINT,"
            "pid BIGINT,"
            "atime timestamp ,"
            "atype varchar,"
            "adist numeric,"
            "ptime timestamp,"
            "pelev numeric,"
            "geom geometry(POINT, 4326)"
            ")"
            ).format(schema=sql.Identifier(SCHEMA), table=sql.Identifier(TABLE)))

CONNECTION.commit()

def un_gzip(infile):
    try:
        if infile.split('.')[-1] == 'gz':
            infile.replace('/', os.path.sep)
            f = gzip.open(infile, 'rb')
            file_content = f.read()
        else:
            infile.replace('/', os.path.sep)
            f = open(infile, "rb")
            file_content = f.read()
        return file_content
    except(FileNotFoundError):
        print('File not found', infile)
        return None


def notNone(s,d):
    if s is None:
        return d
    else:
        return s


def addPoint(type, aid, sid, pid, atime, atype, adist, ptime, pelev, lat, long):

    if type in ('tcx','gpx'):
        date, time = ptime.replace('Z', '').split('T')
        dt = date + ' ' + time
    elif type in ('fit'):
        dt = str(ptime)
    else:
        print(atype)
        print(type)
        print(ptime)
    cursor.execute(
        sql.SQL("insert into {schema}.{table} (aid, sid, pid, atime, atype, adist, ptime, geom)"
                "values (%s,%s,%s,%s,%s,%s,TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'),st_setsrid(st_makepoint(%s,%s),4326))"
                ""
                ).format(schema=sql.Identifier(SCHEMA), table=sql.Identifier(TABLE)),
                [aid, sid, pid, atime, atype, adist, dt, long, lat]
    )
    CONNECTION.commit()


aid = 0

pelev = 0

run = True

if run:
    with open('activities.csv') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        headers = csv_reader.fieldnames
        next(csv_reader)
        for row in csv_reader:
            file_type = None
            root = None
            pid = 0

            if row[f_path] is not None and len(row[f_path]) > 0:
                if row[f_path].split('.')[-1] in ('gz', 'zip'):
                    file_type = row[f_path].split('.')[-2]
                else:
                    file_type = row[f_path].split('.')[-1]
                if file_type in ('tcx', 'gpx'):
                    print(row[f_path])
                    file = un_gzip(row[f_path]).strip()
                    if file:
                        root = ET.fromstring(file)
                if file_type in ('fit'):
                    print(row[f_path])
                    file = un_gzip(row[f_path])
                    if file:
                        root = fitparse.FitFile(file)

            if root:
                sid = int(row[f_id])
                atime = row[f_date]
                atype = row[f_type]
                adist = str(notNone(row[f_dist], 0)).replace(',', '')
                if adist == '':
                    adist = 0
                else:
                    adist = float(adist)

                if file_type == 'gpx':
                    for trks in root.findall("{http://www.topografix.com/GPX/1/1}trk"):
                        for segs in trks.findall("{http://www.topografix.com/GPX/1/1}trkseg"):
                            pts = list(segs)
                            for p in pts:
                                for t in p.findall("{http://www.topografix.com/GPX/1/1}time"):
                                    time = t.text
                                addPoint('gpx', aid, sid, pid, atime, atype, adist, time, pelev, float(p.attrib['lat']), float(p.attrib['lon']))
                                pid += 1
                elif file_type == 'tcx':
                    for acts in root.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Activities"):
                        for act in acts.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Activity"):
                            for lps in act.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Lap"):
                                for trk in lps.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Track"):
                                    for trkp in trk.findall(
                                            "{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Trackpoint"):
                                        for tim in trkp.findall(
                                                "{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Time"):
                                            time = tim.text
                                        for pos in trkp.findall(
                                                "{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Position"):
                                            for lat in pos.findall(
                                                    "{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}LatitudeDegrees"):
                                                lati = lat.text
                                            for lon in pos.findall(
                                                    "{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}LongitudeDegrees"):
                                                loni = lon.text
                                            addPoint('tcx', aid, sid, pid, atime, atype, adist, time, pelev, float(lati), float(loni))
                                            pid += 1
                elif file_type == 'fit':
                    # print(root)
                    for r in root.get_messages('record'):
                        if r.get_value('position_lat') is not None:
                            time = r.get_value('timestamp')
                            lat = int(r.get_value('position_lat')) * (180.0 / 2 ** 31)
                            lon = int(r.get_value('position_long')) * (180.0 / 2 ** 31)
                            if lat != 0 and lon != 0:
                                addPoint('fit', aid, sid, pid, atime, atype, adist, time, pelev, lat, lon)
                                pid += 1

            aid += 1

        print("Done")
