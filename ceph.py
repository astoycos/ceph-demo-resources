import boto3
import wget
import botocore
import boto.s3.connection
import os 
import glob 


s3_endpoint_url = ''
s3_access_key_id = ""
s3_secret_access_key = ""
s3_bucket = 'MyBucket'

# configure boto S3 client
# Low level service access
s3 = boto3.client('s3',
                  '',
                  use_ssl = False,
                  verify = False,
                  endpoint_url = s3_endpoint_url,
                  aws_access_key_id = s3_access_key_id,
                  aws_secret_access_key = s3_secret_access_key,
                  )

# Configure an S3 Resource 
# Higher level object oriented API 
s3r = boto3.resource('s3',
    '',
    use_ssl = False,
    verify = False,
    endpoint_url = s3_endpoint_url,
    aws_access_key_id = s3_access_key_id,
    aws_secret_access_key = s3_secret_access_key,)

#Configure and S3 Connection 
conn = boto.connect_s3(
        aws_access_key_id = s3_access_key_id,
        aws_secret_access_key = s3_secret_access_key,
        host = s3_endpoint_url,
        )

response = s3.list_buckets()

# Get a list of all bucket names from the response
buckets = [bucket['Name'] for bucket in response['Buckets']]

# Print out the bucket list
print("Bucket List: %s" % buckets)
print("Trying to make 'MyBucket'")
try:
    s3.create_bucket(Bucket=s3_bucket)
except: 
    print("Bucket " + s3_bucket + " already exists")

#Get all buckets 
buckets = [bucket['Name'] for bucket in response['Buckets']]

# Print out the bucket list
print("Bucket List: %s" % buckets)

print("Upload some demo images to Ceph Object Storage")

#upload Demo files to ceph storage
s3.upload_file("demo-files/pic-1.jpeg", s3_bucket, "demo-pic-1.jpeg")
s3.upload_file("demo-files/pic-2.jpeg", s3_bucket, "demo-pic-2.jpeg")
s3.upload_file("demo-files/pic-3.jpeg", s3_bucket, "demo-pic-3.jpeg")
s3.upload_file("demo-files/pic-4.jpeg", s3_bucket, "demo-pic-4.jpeg")

print("List information about files currently stored in ceph")

bucket = conn.get_bucket(s3_bucket)

for key in bucket.list():
        print(key.name+ " "+key.sizekey + " "+ key.last_modified)

print("Download files from ceph")
s3.download_file(s3_bucket,"demo-pic-1.jpeg","demo-files-out/pic-1.jpeg")
s3.download_file(s3_bucket,"demo-pic-2.jpeg","demo-files-out/pic-2.jpeg")
s3.download_file(s3_bucket,"demo-pic-3.jpeg","demo-files-out/pic-3.jpeg")
s3.download_file(s3_bucket,"demo-pic-4.jpeg","demo-files-out/pic-4.jpeg")

## clean up 
print("Remove bucket from ceph")

s3.delete_bucket(Bucket="MyBucket")

print("Delete downloaded files in `/demo-files-out`? y/n")
delete= input()

if delete == "y":
    files = glob.glob('/demo-files-out')
    for f in files:
        os.remove(f)
else: 
   print("Downloaded files remain in '/demo-files-out' folder")

