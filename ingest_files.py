import sys
import json
import os
import boto3
import re
import numpy as np
import subprocess
import gdal
import tempfile
import csv
import datetime
import dateutil
import time
import glob
import ntpath

import ee


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gef-ld-toolbox-858b8c8b0b84.json'
ASSET = 'projects/trends_earth/hls'
ee.Initialize()

SLEEP_SECONDS = 3600 / 2

# Read in the list of tiles
with open('tiles.txt') as f:
    tiles = f.readlines()
tiles = [t.strip('\n').split(',') for t in tiles]

years = np.arange(2015, 2020)
base_paths = ['PRO/v1.5/S2/S30', 'PRO/v1.5/L8/L30']

try:
    with open(os.path.join(os.path.dirname(__file__), 'aws_credentials.json'), 'r') as fin:
        keys = json.load(fin)
    s3_client = boto3.client('s3',
                          aws_access_key_id=keys['access_key_id'],
                      aws_secret_access_key=keys['secret_access_key'])
except IOError:
    print('Warning: AWS credentials file not found. Credentials must be in environment variable.')
    s3_client = boto3.client('s3')


# times for GEE need to be formatted in milliseconds since the Unix epoch
epoch = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def list_s3_objects(bucket, s3_prefix):
    resp = s3_client.list_objects(Bucket=bucket, Prefix='{}/'.format(s3_prefix))
    if 'Contents' in resp:
        objects = resp['Contents']
        # Catch the case of the key pointing to the root of the bucket and skip it
        objects = [o for o in objects if os.path.basename(o['Key']) != '']
        return objects
    else:
        return []


def download_from_s3(bucket, objects, local_folder):
    local_files = []
    for obj in objects:
        local_path = os.path.abspath(os.path.join(local_folder, ntpath.basename(obj['Key'])))
        os.makedirs(os.path.join(os.path.abspath(os.path.dirname(local_path)), ''), exist_ok=True)
        s3_client.download_file(Key=obj['Key'], Bucket=bucket, Filename=local_path)
        local_files.append(local_path)
    return local_files


# HDF files need to be converted to tifs prior to upload to GEE
def hdf_to_tif(f):
    out_file = os.path.splitext(f)[0] + '.tif'
    subprocess.check_call(['gdal_translate', f, out_file])


def get_metadata(files):
    m = []
    for f in files:
        this_m = gdal.Open(f).GetMetadata()
        # sensor
        times = re.findall(r'[\w.\-: ]+', this_m['SENSING_TIME'])
        t0 = dateutil.parser.parse(times[0].strip(' '))

        
        if re.search('HLS\.S30', f):
            sensor = 'Sentinel-2'
        elif re.search('HLS\.L30', f):
            sensor = 'Landsat-8'
        else:
            sensor = 'Unknown'

        # # Mask layers don't have a long_name field, so assign one if this is a 
        # # mask layer
        # long_name = this_m.get('long_name', None)
        # if not long_name:
        #     if 'Fmask bit description' in this_m:
        #         long_name = 'Fmask'
        #     if 'ACmask bit description' in this_m:
        #         long_name = 'ACmask'

        m.append({'filename': os.path.splitext(ntpath.basename(f))[0].replace('.', '_'),
                  'sensor': sensor,
                  'cloud_coverage': this_m.get('cloud_coverage', None),
                  #'MEAN_SUN_AZIMUTH_ANGLE': ''.join(this_m.get('MEAN_SUN_AZIMUTH_ANGLE', None)),
                  #'MEAN_SUN_ZENITH_ANGLE': ''.join(this_m.get('MEAN_SUN_ZENITH_ANGLE', None)),
                  'spatial_coverage': this_m.get('spatial_coverage', None),
                  'SENSING_TIME': this_m.get('SENSING_TIME', None),
                  'system:time_start': unix_time_millis(t0)})
    return m


# Loop over all the tiles
for tile in tiles:
    print("*************************************************************\nProcessing tile {}".format(tile))
    for year in years:
        print("*************************************************************\nProcessing year {}".format(year))
        for base_path in base_paths:
            print("*************************************************************\nProcessing base_path {}".format(base_path))
            l_tiles = base_path + '/' + str(year) + '/' + '/'.join(tile)

            # Function to download files from S3
            objects = list_s3_objects('hlsanc', l_tiles)
            if len(objects) == 0:
                continue

            # Filter out any objects that are already present in the collection
            hls = ee.ImageCollection(ASSET)
            existing = [feat['properties']['filename'] for feat in hls.getInfo()['features']]
            objects = [obj for obj in objects if re.search('hdf$', obj['Key'])]
            objects = [obj for obj in objects if os.path.splitext(ntpath.basename(ntpath.basename(obj['Key'])))[0].replace('.', '_') not in existing]

            if len(objects) == 0:
                continue
            else:
                hdr_files = download_from_s3('hlsanc', objects, '.')

                n = 0
                for f in hdr_files:
                    n += 1
                    print('Processing {} (file {} of {})...'.format(f, n, len(hdr_files)))
                    sds = [sd[0] for sd in gdal.Open(f).GetSubDatasets()]
                    band_names = [item.split(':')[-1] for item in sds]

                    band_vrts = []
                    for sd in sds:
                        out = tempfile.NamedTemporaryFile(suffix='.vrt').name
                        subprocess.check_call(['gdal_translate', '-a_scale', '1', '-ot', 'Int16', '-q', sd, out])
                        band_vrts.append(out)

                    out_base = os.path.splitext(f)[0].replace('.', '_')
                    vrt = tempfile.NamedTemporaryFile(suffix='.vrt').name
                    gdal.BuildVRT(vrt, band_vrts, separate=True)
                    tif = out_base + '.tif'
                    subprocess.check_call(['gdal_translate', '-co', 'COMPRESS=LZW', '-q', vrt, tif])

                m = get_metadata(hdr_files)
                with open('metadata.csv', 'w', newline='') as csvfile:
                    fieldnames = m[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for item in m:
                        writer.writerow(item)
                
                subprocess.call(['geebam', 'upload', '--source', '.', '-m', 'metadata.csv', '--dest', ASSET, '--bucket', 'trendsearth-hls', '--bands', ','.join(band_names)])

                print('Deleting files......')
                for p in glob.glob('*.tif'):
                    os.remove(p)
                for p in glob.glob('*.hd*'):
                    os.remove(p)
                for p in glob.glob('*.xml'):
                    os.remove(p)

                current_time = datetime.datetime.now()
                print('Sleeping from {} until {}......'.format(current_time, current_time + datetime.timedelta(seconds=SLEEP_SECONDS)))
                time.sleep(SLEEP_SECONDS)
