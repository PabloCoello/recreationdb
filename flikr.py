import flickrapi
import json
from collections import defaultdict
import geopandas as gpd
import pandas as pd
import time
from pymongo import MongoClient, GEOSPHERE
import shapely.geometry
import datetime
import re
import warnings
import datetime
import sys
from paramiko import SSHClient, AutoAddPolicy


class retrieve_data():
    '''
    '''

    def __init__(self, path):
        '''
        '''
        self.get_conf(path)
        self.set_flickr_con()
        self.set_mongodb_con()
        try:
            self.init = 1
            self.counter = 0
            self.records = 0
            while self.init > 0:
                photos = self.get_flickr_photos()
                if len(photos['photos']['photo']) == 0:
                    break

                data = self.get_data(photos)
                self.collection.insert_many(data)
                self.set_record(path)
                self.print_status(photos)

                if self.counter > 2900:
                    time.sleep(3650)
                    self.counter = 0

        except KeyboardInterrupt:
            pass

        print('Last page:' + str(self.conf['page']))
        print('Total records:' + str(self.records))
        self.close_mongodb_con()
        self.close_ssh_con()

    def get_conf(self, path):
        '''
        '''
        with open(path, 'r') as f:
            self.conf = json.load(f)

    def set_flickr_con(self):
        '''
        '''
        self.flickr = flickrapi.FlickrAPI(
            self.conf['api_key'], self.conf['api_secret'], format='parsed-json')

    def set_mongodb_con(self):
        '''
        '''

        self.ssh_client = SSHClient()
        self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        self.ssh_client.connect(
            self.conf['ssh_server'], username=self.conf['ssh_user'], password=self.conf['ssh_password'])

        if re.search(' ', self.conf['database']) is None and len(self.conf['database']) < 20:
            if re.search(' ', self.conf['collection']) is None and len(self.conf['collection']) < 20:

                self.client = MongoClient('localhost', 27017)
                db = eval('self.client.' + self.conf['database'])
                self.collection = eval('db.' + self.conf['collection'])
                self.collection.create_index([("geometry", GEOSPHERE)])

            else:
                print('invalid collection name')
        else:
            print('invalid database name')

    def close_mongodb_con(self):
        '''
        '''
        self.client.close()

    def close_ssh_con(self):
        '''
        '''
        self.ssh_client.close()

    def get_flickr_photos(self):
        '''
        '''
        photos = self.flickr.photos.search(tags=self.conf['tags'],
                                           bbox=self.conf['bbox'],
                                           accuracy=12,
                                           has_geo=1,
                                           geo_context=0,
                                           min_taken_date=self.conf['from_date'],
                                           max_taken_date=self.conf['to_date'],
                                           extras='geo, views, date_taken, owner_name, description, tags, url_q',
                                           page=self.conf['page'],
                                           per_page=500)
        return photos

    def get_data(self, photos):
        '''
        '''
        toret = defaultdict(list)
        for row in photos['photos']['photo']:
            toret['id'].append(row['id'])
            toret['date'].append(datetime.datetime.strptime(
                row['datetaken'], "%Y-%m-%d %H:%M:%S"))
            toret['Title'].append(row['title'])
            toret['tags'].append(row['tags'])
            toret['owner'].append(row['owner'])
            toret['owner_name'].append(row['ownername'])
            toret['views'].append(float(row['views']))
            toret['url'].append(row['url_q'])
            toret['latitude'].append(row['latitude'])
            toret['longitude'].append(row['longitude'])
            toret['context'].append(row['context'])

        df = pd.DataFrame(toret)
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.longitude.astype(float), df.latitude.astype(float)))
        gdf['geometry'] = gdf['geometry'].apply(
            lambda x: shapely.geometry.mapping(x))
        data = gdf.to_dict(orient='records')
        return data

    def print_status(self, photos):
        '''
        '''
        now = datetime.datetime.now()
        self.counter += self.init
        self.records += self.init
        self.init = len(photos['photos']['photo'])
        print(str(now.day) + '-' + str(now.month) + '-' + str(now.year) + ' ' + str(now.hour) +
              ':'+str(now.minute)+':'+str(now.second)+'. Number of records: '+str(self.records))

    def set_record(self, path):
        '''
        '''
        self.conf['page'] += 1
        with open(path, 'w') as f:
            json.dump(self.conf, f, indent=4)


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    retrieve_data(path=str(sys.argv[1]))