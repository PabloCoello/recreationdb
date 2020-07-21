import flickrapi
import json
from collections import defaultdict
import geopandas as gpd
import pandas as pd
import time
from pymongo import MongoClient, GEOSPHERE
import shapely.geometry


with open('./conf.json', 'r') as f:
    conf = json.load(f)

flickr = flickrapi.FlickrAPI(
    conf['api_key'], conf['api_secret'], format='parsed-json')

client = MongoClient('localhost', 27017)
db = client.geo
collection = db.location
collection.create_index([("geometry", GEOSPHERE)])


try:
    init = 1
    counter = 0
    page = 1
    records = 0
    while init > 0:
        photos = flickr.photos.search(tags=conf['tags'],
                                      bbox=conf['bbox'],
                                      accuracy=12,
                                      has_geo=1,
                                      geo_context=2,
                                      extras='geo, views, date_taken, owner_name',
                                      page=10,
                                      per_page=500)

        toret = defaultdict(list)
        for row in photos['photos']['photo']:
            toret['id'].append(row['id'])
            toret['date'].append(row['datetaken'])
            toret['Title'].append(row['title'])
            toret['owner'].append(row['owner'])
            toret['owner_name'].append(row['owner_name'])
            toret['views'].append(row['views'])
            toret['latitude'].append(row['latitude'])
            toret['longitude'].append(row['longitude'])

        df = pd.DataFrame(toret)
        gdf = gpd.GeoDataFrame(
            matrix, geometry=gpd.points_from_xy(matrix.longitude.astype(float), matrix.latitude.astype(float)))
        data = gdf.to_dict(orient='records')
        collection.insert_many(data)

        page += 1
        init = len(photos['photos']['photo'])
        counter += init
        records += init

        print(records)

        if counter > 2900:
            print('waiting one hour')
            time.sleep(3650)
            counter = 0

except KeyboardInterrupt:
    client.close()
