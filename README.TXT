Very simple script for parsing Garmin files from Strava to ShapeFile.

Supports GPX, TCX, and FIT files.

Usage: Put into the same folder as your unzipped stava data and run. Python3. Only works on Garmin file types, GPX, FIT, and TCX. Has some requirements.

Like:

fitparse
ogr
osr

Also has a parse to points script, which loads directly to a PostgreSQL/PostGIS database.
Further processing the points: https://gist.github.com/HeikkiVesanto/3fbd55cda45394d069773a34ea244e4b
