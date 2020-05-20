import boto3
import wget
import os  
import base64

s3_endpoint_url = "http://ceph-route-rook-ceph.apps.astoycos-ocp.shiftstack.com"
s3_access_key_id = "NkZXSzY5WDI1MlMyRkU2Szk2RzU="
s3_secret_access_key = "aEVJTjV4aERvRGQzMzh5WjVLQlYzNXk4QnVoWU9Ba3A5aThMMzlVSg=="
s3_bucket = 'mybucket'

# configure boto S3 client
# Low level service access
s3 = boto3.client('s3',
    '',
    use_ssl = False,
    verify = False,
    endpoint_url = s3_endpoint_url,
    aws_access_key_id = base64.decodebytes(bytes(s3_access_key_id,'utf-8')).decode('utf-8'),
    aws_secret_access_key = base64.decodebytes(bytes(s3_secret_access_key, 'utf-8')).decode('utf-8'),
    )

# Configure an S3 Resource 
# Higher level object oriented API 
s3r = boto3.resource('s3',
    '',
    use_ssl = False,
    verify = False,
    endpoint_url = s3_endpoint_url,
    aws_access_key_id = base64.decodebytes(bytes(s3_access_key_id,'utf-8')).decode('utf-8'),
    aws_secret_access_key = base64.decodebytes(bytes(s3_secret_access_key, 'utf-8')).decode('utf-8'),
)

response = s3.list_buckets()
# Get a list of all bucket names from the response
buckets = [bucket['Name'] for bucket in response['Buckets']]

# Print out the bucket list
print("Initial bucket List: %s" % buckets)

#s3r.Bucket("MyBucket").objects.all().delete()
#s3.delete_bucket(Bucket="MyBucket")

print("Trying to make 'mybucket'")
if s3_bucket not in buckets:
    s3.create_bucket(Bucket=s3_bucket)
else: 
    print("Bucket " + s3_bucket + " already exists, deleting and recreating")
    s3r.Bucket(s3_bucket).objects.all().delete()
    s3.delete_bucket(Bucket=s3_bucket)
    s3.create_bucket(Bucket=s3_bucket)

response = s3.list_buckets()
#Get all buckets 
buckets = [bucket['Name'] for bucket in response['Buckets']]

# Print out the bucket list
print("Updated bucket List: %s" % buckets)

print("Upload some demo images to Ceph Object Storage")

#upload Demo files to ceph storage
s3.upload_file("demo-files/pic-1.jpeg", s3_bucket, "demo-pic-1.jpeg")
s3.upload_file("demo-files/pic-2.jpeg", s3_bucket, "demo-pic-2.jpeg")
s3.upload_file("demo-files/pic-3.jpeg", s3_bucket, "demo-pic-3.jpeg")
s3.upload_file("demo-files/pic-4.jpeg", s3_bucket, "demo-pic-4.jpeg")

print("List files currently stored in ceph")

my_bucket = s3r.Bucket(name=s3_bucket)

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object.key + " was last modified at " + str(my_bucket_object.last_modified))

    
print("Download files from ceph")
s3.download_file(s3_bucket,"demo-pic-1.jpeg","demo-files-out/pic-1-out.jpeg")
s3.download_file(s3_bucket,"demo-pic-2.jpeg","demo-files-out/pic-2-out.jpeg")
s3.download_file(s3_bucket,"demo-pic-3.jpeg","demo-files-out/pic-3-out.jpeg")
s3.download_file(s3_bucket,"demo-pic-4.jpeg","demo-files-out/pic-4-out.jpeg")

## clean up 
print("Remove bucket from ceph")

my_bucket.objects.all().delete()
s3.delete_bucket(Bucket=s3_bucket)

print("Downloaded images from ceph can be seen in 'demo-files-out/' directory")

print("Delete downloaded files in `/demo-files-out`? y/n")
delete= input()

if delete == "y":
    mydir = "demo-files-out/"
    filelist = [ f for f in os.listdir(mydir)]
    for f in filelist:
        os.remove(os.path.join(mydir, f))
else: 
   print("Downloaded files remain in '/demo-files-out' folder")

