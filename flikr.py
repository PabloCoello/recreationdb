import flickrapi
import json
from collections import defaultdict
import geopandas as gpd
import pandas as pd
import time

api_key = u'29222bf718a8d96ccb100cc8b340fbff'
api_secret = u'773ddbdab9c8fcb2'

asnevesbbox = '-8.471084, 42.070502, -8.346844, 42.184293'
guadarramabbox = '-4.002646,40.889620,-3.936707, 40.904501'
sudoe =  "-11.811238,35.852175,8.369423,46.840323"

flickr = flickrapi.FlickrAPI(api_key, api_secret, format='parsed-json')

num_pags=10


matrix = pd.DataFrame()
counter = 0
for i in range(21,31):
    print(i)
    photos = flickr.photos.search(tags='nature, forest',
                                bbox=sudoe,
                                accuracy=12,
                                has_geo=1,
                                geo_context=2,
                                extras='geo, views',
                                page=i,
                                per_page=500)


    toret = defaultdict(list)
    for row in photos['photos']['photo']:
        toret['id'].append(row['id'])
        toret['Title'].append(row['title'])
        toret['views'].append(row['views'])
        toret['latitude'].append(row['latitude'])
        toret['longitude'].append(row['longitude'])

    df = pd.DataFrame(toret)
    matrix = pd.concat([matrix, df])
    counter += 1
    if counter == 10:
        matrix.to_excel('sudoe.xlsx')
        #time.sleep(3650)
        counter = 0
        

gdf =gpd.GeoDataFrame(
    matrix, geometry=gpd.points_from_xy(matrix.longitude.astype(float), matrix.latitude.astype(float)))

gdf.to_file('fotos_sudoe.shp')

matrix.longitude.astype(float)