import json
import boto3
from datetime import datetime
from calendar import monthrange

s3 = boto3.resource('s3')
s3_prefix_uri = 's3-us-east-1://patch-user-action-event-data/'
s3_uri = 's3://patch-user-action-event-data/'
keys = []
num_of_keys = [0]
manifest = {
	'fileLocations': [
		{
			'URIPrefixes': [s3_prefix_uri]
		},
		{
			'URIs': []
		}
	],
	'globalUploadSettings': {
		'format': 'JSON',
	}
}
QUICKSIGHT_FILE_COMPUTATIONAL_LIMIT = 1000
manifest_uri = 'kinesisFirehoseManifest.json'

def lambda_handler(event, context):
	# Compute URIPrefix(es) based on current day, month, and year
	# Update manifest URI's to past 30 days
	# update_current_month_uri()
	# update_prev_month_uri()

	update_manifest_uri()

	# Upload manifest to s3 bucket
	upload_manifest_to_s3()

	# Refresh Quicksight

	# Trigger the lambda every night

	return {
		'statusCode': 200,
		'body': manifest,
		'keys': keys,
		'keysLength': len(keys)
	}

# Pulls in the latest URIs until the QUICKSIGHT_FILE_COMPUTATIONAL_LIMIT is reached
def update_manifest_uri():
	iteration = 0
	fileLimitReached = False
	current_month = datetime.today().month
	current_year = datetime.today().year

	while not fileLimitReached:
		
		uri_month_prefix = '0' if current_month < 10 else ''
		current_prefix = str(current_year) + '/' + uri_month_prefix + str(current_month) + '/'
		current_month_keys = []
		get_matching_s3_keys(current_month_keys, 'patch-user-action-event-data', current_prefix,)
		
		if (iteration != 0 and len(current_month_keys) == 0):
			break


		for key in reversed(current_month_keys):
			if num_of_keys[0] < QUICKSIGHT_FILE_COMPUTATIONAL_LIMIT:
				uri = s3_uri + key
				keys.append(uri)
				num_of_keys[0] = num_of_keys[0] + 1
				manifest['fileLocations'][1]['URIs'].append(uri)
			else:
				fileLimitReached = True
				break

		if current_month == 1:
			current_month = 12
			current_year = current_year - 1
			iteration = iteration + 1
		else:
			current_month = current_month - 1
			iteration = iteration + 1

# Uploads manifest to Firehose bucket
def upload_manifest_to_s3():
	s3object = s3.Object('patch-user-action-event-data', 'kinesisFirehoseManifest.json')
	s3object.put(Body=(bytes(json.dumps(manifest).encode('UTF-8'))))

# Get objects whose key starts with prefix
def get_matching_s3_objects(bucket, prefix=""):

	s3 = boto3.client("s3")
	paginator = s3.get_paginator("list_objects_v2")

	kwargs = {'Bucket': bucket}

	if isinstance(prefix, str):
		prefixes = (prefix, )
	else:
		prefixes = prefix

	for key_prefix in prefixes:
		kwargs["Prefix"] = key_prefix

		for page in paginator.paginate(**kwargs):
			try:
				contents = page["Contents"]
			except KeyError:
				break

			for obj in contents:
				key = obj["Key"]
				yield obj

# Gets keys that starts with prefix
def get_matching_s3_keys(keys, bucket, prefix=""):
	for obj in get_matching_s3_objects(bucket, prefix):
		keys.append(obj['Key'])
		

# Updates Quicksight with data from the last 30 days
def update_last_thirty_days_data():
	update_current_month_uri()
	update_prev_month_uri()

def update_current_month_uri():
	current_month = datetime.today().month
	uri_month_prefix = '0' if current_month < 10 else ''

	current_month_uri = s3_uri + str(datetime.today().year) + '/' + uri_month_prefix + str(current_month) + '/'
	prefix.append(current_month_uri[len(s3_uri):])
	
	get_matching_s3_keys('patch-user-action-event-data', current_month_uri[len(s3_uri):], '')
	manifest['fileLocations'][0]['URIPrefixes'].append(current_month_uri)	
    	
def update_prev_month_uri():    
	prev_month = datetime.today().month - 1
	current_year = datetime.today().year
	current_day = datetime.today().day

	# Pull data from previous year's december
	if prev_month == -1:
		prev_month = 12
		current_year = current_year - 1
    
	uncomputed_days = current_day - 30
	if uncomputed_days < 0:
		# Calculates the total number of days in the previous month
		prev_month_range = monthrange(current_year, prev_month)[1]
		uri_month_prefix = '0' if prev_month < 10 else ''

		for day in range(prev_month_range, prev_month_range - (uncomputed_days * -1),-1):
			uri_day_prefix = '0' if day < 10 else ''
			uri = s3_uri + str(current_year) + '/' +  uri_month_prefix + str(prev_month) + '/' + uri_day_prefix + str(day) + '/'
			# get_file_count(uri[len(s3_uri)])
			
			manifest['fileLocations'][0]['URIPrefixes'].append(uri)