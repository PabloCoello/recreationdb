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
        self.get_bbox()
        self.set_flickr_con()
        self.set_mongodb_con()
        self.reset_record(path)
        try:
            self.init = 1
            self.counter = 0
            self.records = 0
            data = gpd.GeoDataFrame()
            while self.init > 0:
                photos = self.get_flickr_photos()
                if len(photos['photos']['photo']) == 0:
                    self.store_pipeline(data)
                    break

                data = pd.concat([data, self.get_data(photos)])

                self.set_record(path)
                self.print_status(photos)

                if len(data.id.unique()) >= 4000:
                    self.store_pipeline(data)
                    break

                if self.counter > 2900:
                    time.sleep(3650)
                    self.counter = 0

        except KeyboardInterrupt:
            self.store_pipeline(data)

        self.reset_record(path)
        self.close_mongodb_con()
        if self.conf['ssh']:
            self.close_ssh_con()

    def get_conf(self, path):
        '''
        '''
        with open(path, 'r') as f:
            self.conf = json.load(f)

    def get_bbox(self):
        '''
        '''
        with open('./conf/countries_bbox.json', 'r') as f:
            self.bbox = json.load(f)

    def set_flickr_con(self):
        '''
        '''
        self.flickr = flickrapi.FlickrAPI(
            self.conf['api_key'], self.conf['api_secret'], format='parsed-json')

    def set_mongodb_con(self):
        '''
        '''

        if self.conf['ssh']:
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
                self.collection.create_index('id', unique=True)
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
                                           tag_mode='any',
                                           bbox=self.bbox[self.conf['country']][1],
                                           geo_context=0,
                                           min_taken_date=self.conf['from_date'],
                                           max_taken_date=self.conf['to_date'],
                                           content_type=7,
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
        return gdf

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

    def reset_record(self, path):
        '''
        '''
        self.conf['page'] = 1
        with open(path, 'w') as f:
            json.dump(self.conf, f, indent=4)

    def store_data(self, data):
        '''
        '''
        data = data.drop_duplicates('id')
        data = data.to_dict(orient='records')
        self.collection.insert_many(data, ordered=False)

    def print_total_records(self):
        '''
        '''
        print('Total records:' + str(self.collection.count()))

    def store_pipeline(self, data):
        '''
        '''
        self.store_data(data)
        self.print_total_records()


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    retrieve_data(path=str(sys.argv[1]))
