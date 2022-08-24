import csv
import gzip
import fitparse
import json
import xml.etree.ElementTree as ET
import osgeo.ogr as ogr
import sys, os
import osgeo.osr as osr

# Usage: Put into the same folder as your unzipped stava data. Only works on Garmin file types, GPX, FIT, and TCX.
# Has requirements.

FILENAME = 'strava'  # extension gets added.

# Column id's
# f_id = 0
# f_date = 1
# f_type = 3
# f_path = 11  # Column in activities.csv that contains the file paths. Seems to move around.
# f_dist = 6

# Column names in activities.csv
f_id = 'Activity ID'
f_date = 'Activity Date'
f_type = 'Activity Type' 
f_path = 'Filename'
f_dist = 'Distance'

#parse to line or point:


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


FILENAME = FILENAME + '.shp'

# Shapefile creation
print('Creating file', FILENAME)
driver = ogr.GetDriverByName('ESRI Shapefile')
ds = driver.CreateDataSource(FILENAME)
srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)
layer = ds.CreateLayer(FILENAME[:-4], srs, ogr.wkbLineString)
fieldDefn_ = ogr.FieldDefn('aid', ogr.OFTInteger64)
layer.CreateField(fieldDefn_)
fieldDefn_ = ogr.FieldDefn('sid', ogr.OFTInteger64)
layer.CreateField(fieldDefn_)
fieldDefn_ = ogr.FieldDefn('atime', ogr.OFTString)
layer.CreateField(fieldDefn_)
fieldDefn_ = ogr.FieldDefn('atype', ogr.OFTString)
layer.CreateField(fieldDefn_)
fieldDefn_ = ogr.FieldDefn('adist', ogr.OFTReal)
layer.CreateField(fieldDefn_)
print('Created file', FILENAME)

aid = 0

with open('activities.csv') as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    headers = csv_reader.fieldnames
    next(csv_reader)
    for row in csv_reader:
        line = ogr.Geometry(ogr.wkbLineString)
        file_type = None
        root = None

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

            if file_type == 'gpx':
                for trks in root.findall("{http://www.topografix.com/GPX/1/1}trk"):
                    for segs in trks.findall("{http://www.topografix.com/GPX/1/1}trkseg"):
                        pts = list(segs)
                        for p in pts:
                            line.AddPoint(float(p.attrib['lon']), float(p.attrib['lat']))
            elif file_type == 'tcx':
                for acts in root.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Activities"):
                    for act in acts.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Activity"):
                        for lps in act.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Lap"):
                            for trk in lps.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Track"):
                                for trkp in trk.findall(
                                        "{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Trackpoint"):
                                    for pos in trkp.findall(
                                            "{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Position"):
                                        for lat in pos.findall(
                                                "{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}LatitudeDegrees"):
                                            lati = lat.text
                                        for lon in pos.findall(
                                                "{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}LongitudeDegrees"):
                                            loni = lon.text
                                        line.AddPoint(float(loni), float(lati))
            elif file_type == 'fit':
                # print(root)
                for r in root.get_messages('record'):
                    if r.get_value('position_lat') is not None:
                        lat = int(r.get_value('position_lat')) * (180.0 / 2 ** 31)
                        lon = int(r.get_value('position_long')) * (180.0 / 2 ** 31)
                        if lat != 0 and lon != 0:
                            line.AddPoint(lon, lat)

            feature = ogr.Feature(layer.GetLayerDefn())
            feature.SetGeometry(line)
            print(row[f_id], aid, row[f_date], row[f_type], row[f_dist])
            feature.SetField('sid', int(row[f_id]))
            feature.SetField('aid', aid)
            feature.SetField('atime', row[f_date])
            feature.SetField('atype', row[f_type])
            feature.SetField('adist', float(str(notNone(row[f_dist], 0)).replace(',', '')))
            # if row[f_id] != '1350056089':
            layer.CreateFeature(feature)

    aid += 1

    print("Done")
