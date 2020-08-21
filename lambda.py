from update_manifest import update_manifest

manifests_information = [
  {
    prefix_uri: 's3-us-east-1://page-tracking-firehose-records/',
    uri: 's3://page-tracking-firehose-records/',
    manifest_id: 'kinesisFirehoseManifest.json'
  },
  {
    prefix_uri: 's3-us-east-1://pinpoint-kpi-data-bucket/',
    uri: 's3://pinpoint-kpi-data-bucket/',
    manifest_id: 'manifest.json'
  },
  {
    prefix_uri: 's3-us-east-1://patch-user-action-event-data/',
    uri: 's3://patch-user-action-event-data/',
    manifest_id: 'manifest.json'
  }
]

def lambda_handler(event, context):
  for item in manifests_information:
    update_manifest(item.prefix_uri, item.uri, item.manifest_id)
    