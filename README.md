# s3_client

**s3_client** - Sample python script to work with Amazon S3. It is proposed to be used for test, not production.

Developed for Python 3.

### Authorization

Authorization is performed using environment variables:

**AWS_ACCESS_KEY_ID** -  Specifies an AWS access key
**AWS_SECRET_ACCESS_KEY** - Specifies the secret key associated with the access key. This is essentially the "password" for the access key.


### Usage:

```console
$ ./s3_client.py
usage: s3_client.py [-h] [--debug] --endpoint ENDPOINT
                    {listbuckets,listobj,metadataobj,upload,download} ...

S3 Client sample script

optional arguments:
  -h, --help            show this help message and exit
  --debug, -d           debug flag
  --endpoint ENDPOINT, -e ENDPOINT
                        S3 endpoint URL

Commands:
  {listbuckets,listobj,metadataobj,upload,download}
    listbuckets         List all buckets
    listobj             List Objects in a bucket
    metadataobj         List Objects Metadata
    upload              Upload files to bucket
    download            Download files from bucket

    Example of use:
        ./s3_client.py -e https://s3.amazonaws.com listbuckets
        ./s3_client.py -e https://s3.amazonaws.com listobj my_bucket -t
        ./s3_client.py -e https://s3.amazonaws.com upload my_bucket -f file1
        ./s3_client.py -e https://s3.amazonaws.com upload my_bucket -d mydir
```

### Example:

```console
$ ./s3_client.py  -e https://s3.amazonaws.com listbuckets
Bucket_Name: test-script1 Creation_Date: 2019-03-22 19:40:36.379000+00:00
Bucket_Name: test-script2 Creation_Date: 2019-03-22 19:50:35.706000+00:00
```

```console
$ ./s3_client.py  -e https://s3.amazonaws.com upload my_bucket -d mydir/
Uploading file mydir/teste11 with key mydir/teste11 Done. Elapsed time (hh:mm:ss.mm) 0:00:00.071952
Uploading file mydir/teste22 with key mydir/teste22 Done. Elapsed time (hh:mm:ss.mm) 0:00:00.046295
Uploading file mydir/internal/teste333 with key mydir/internal/teste333 Done. Elapsed time (hh:mm:ss.mm) 0:00:00.097774
Uploading file mydir/internal/teste444 with key mydir/internal/teste444 Done. Elapsed time (hh:mm:ss.mm) 0:00:00.120079
Uploading file mydir/internal/deep/deep11 with key mydir/internal/deep/deep11 Done. Elapsed time (hh:mm:ss.mm) 0:00:00.048323
```

```console
$ ./s3_client.py  -e https://s3.amazonaws.com listobj my_bucket -t
+----------------------------+------+---------------+----------------------------------+
| Object                     | Size | Storage_Class |          Last_Modified           |
+----------------------------+------+---------------+----------------------------------+
| mydir/internal/deep/deep11 |    4 |    STANDARD   | 2019-03-27 19:52:52.476000+00:00 |
| mydir/internal/teste333    |    4 |    STANDARD   | 2019-03-27 19:52:52.256000+00:00 |
| mydir/internal/teste444    |    4 |    STANDARD   | 2019-03-27 19:52:52.353000+00:00 |
| mydir/teste11              |    4 |    STANDARD   | 2019-03-27 19:52:52.143000+00:00 |
| mydir/teste22              |    0 |    STANDARD   | 2019-03-27 19:52:52.207000+00:00 |
+----------------------------+------+---------------+----------------------------------+
```

```console
$ ./s3_client.py  -e https://s3.amazonaws.com download -h
usage: s3_client.py download [-h] [--localdir LOCALDIR]
                             (--file FILENAME | --prefix PREFIX)
                             bucket

positional arguments:
  bucket                Bucket Name

optional arguments:
  -h, --help            show this help message and exit
  --localdir LOCALDIR, -l LOCALDIR
                        Local directory to save downloaded file. Default
                        current directory
  --file FILENAME, -f FILENAME
                        Download a specific file
  --prefix PREFIX, -p PREFIX
                        Download recursively all files with a prefix.
```

```console
$ ./s3_client.py  -e https://s3.amazonaws.com download my_bucket -f mydir/teste11
Downloading mydir/teste11 to dir '.' Done.
```

```console
$ ./s3_client.py  -e https://s3.amazonaws.com download my_bucket -f mydir/teste11 -l /tmp/Downloading mydir/teste11 to dir '/tmp/' Done.
$
$ ls  /tmp/mydir/teste11
/tmp/mydir/teste11
```
