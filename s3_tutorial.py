import boto3
import uuid


# Create a bucket in s3
def create_bucket_name(bucket_prefix):
    return ''.join([bucket_prefix, str(uuid.uuid4())])

def create_bucket(bucket_prefix, s3_connection):
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': current_region} )
    print(bucket_name, current_region)
    return bucket_name, bucket_response


# Create a text file with random name
def create_temp_file(size, file_name, file_content):
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name
first_file_name = create_temp_file(300, 'secondfile.txt', 'f')   



# Create Bucket and Object instances
first_bucket = s3_resource.Bucket(name=first_bucket_name)
# or
first_bucket = first_object.Bucket()

first_object = s3_resource.Object(bucket_name=first_bucket_name, key=first_file_name)
# or
first_object_again = first_bucket.Object(first_file_name)


# Uploading a File to s3
# From object instance
s3_resource.Object(first_bucket_name, first_file_name).upload_file(Filename=first_file_name)
# or
first_object.upload_file(first_file_name)

# Bucket instance version
s3_resource.Bucket(first_bucket_name).upload_file(Filename=first_file_name, Key=first_file_name)

# Client version
s3_resource.meta.client.upload_file(Filename=first_file_name, Bucket=first_bucket_name, Key=first_file_name)


# List content in a bucket --https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3
def list_content_s3():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('firstpythonbucket40b604f8-232b-42f0-9f66-3f5b5d1aa6d3')
    for file in my_bucket.objects.all():
        print(file.key)

# Downloading a File
s3_resource.Object(first_bucket_name, first_file_name).download_file(f'/tmp/{first_file_name}')


# Copying an object between buckets
def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)

copy_to_bucket(first_bucket_name, second_bucket_name, first_file_name)


# Deleting an object
s3_resource.Object(second_bucket_name, first_file_name).delete()