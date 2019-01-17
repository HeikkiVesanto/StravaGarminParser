import csv
import gzip
import fitparse
import json
import xml.etree.ElementTree as ET
import ogr, sys, os
import osgeo.osr as osr

# Usage: Put into the same folder as your unzipped stava data. Only works on Garmin file types, GPX, FIT, and TCX.
# Has requirements.

FILENAME = 'strava.shp'

# Column id's
f_id = 0
f_date = 1
f_type = 3
f_path = 9
f_dist = 6


def un_gzip(infile):
    try:
        if infile.split('.')[-1] == 'gz':
            f = gzip.open(infile, 'rb')
            file_content = f.read()
        else:
            file = open(infile, "rb")
            file_content = file.read()
        return file_content
    except(FileNotFoundError):
        print('File not found', infile)
        return None


# Shapefile creation
print('Creating file', FILENAME)
driver = ogr.GetDriverByName('ESRI Shapefile')
ds = driver.CreateDataSource(FILENAME)
srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)
layer = ds.CreateLayer(FILENAME[:-4], srs, ogr.wkbLineString)
fieldDefn_ = ogr.FieldDefn('aid', ogr.OFTInteger)
layer.CreateField(fieldDefn_)
fieldDefn_ = ogr.FieldDefn('sid', ogr.OFTInteger)
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
    csv_reader = csv.reader(csv_file, delimiter=',')
    next(csv_reader)
    for row in csv_reader:
        line = ogr.Geometry(ogr.wkbLineString)
        file_type = None

        if row[f_path].split('.')[-1] in ('gz', 'zip'):
            file_type = row[f_path].split('.')[-2]
        else:
            file_type = row[f_path].split('.')[-1]
        if file_type in ('tcx', 'gpx'):
            print(row[f_path])
            file = un_gzip(row[f_path]).strip()
            if file:
                root = ET.fromstring(file)
        if file_type in('fit'):
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
                            line.AddPoint( float(p.attrib['lon']), float(p.attrib['lat']))
            elif file_type == 'tcx':
                for acts in root.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Activities"):
                    for act in acts.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Activity"):
                        for lps in act.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Lap"):
                            for trk in lps.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Track"):
                                for trkp in trk.findall("{http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2}Trackpoint"):
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
                #print(root)
                for r in root.get_messages('record'):
                    if r.get('position_lat') is not None:
                        lat = int(r.get_value('position_lat')) * (180 / 2 ** 31)
                        lon = int(r.get_value('position_long')) * (180 / 2 ** 31)
                        line.AddPoint(lon, lat)

            feature = ogr.Feature(layer.GetLayerDefn())
            feature.SetGeometry(line)
            print(row[f_id], aid, row[f_date], row[f_type], row[f_dist])
            feature.SetField('sid', int(row[f_id]))
            feature.SetField('aid', aid)
            feature.SetField('atime', row[f_date])
            feature.SetField('atype', row[f_type])
            feature.SetField('adist', float(row[f_dist]))
            if row[f_id] != '1350056089':
                layer.CreateFeature(feature)

    aid += 1

    print("Done")