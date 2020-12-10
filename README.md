# s3-client - Example python script to work with S3

![build](https://github.com/thobiast/s3_client/workflows/build/badge.svg)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/s3-client)
[![codecov](https://codecov.io/gh/thobiast/s3_client/branch/master/graph/badge.svg)](https://codecov.io/gh/thobiast/s3_client)
[![GitHub License](https://img.shields.io/github/license/thobiast/s3_client)](https://github.com/thobiast/s3_client/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


## Installation

```bash
pip install s3-client
```


### Authorization

Authorization is performed using environment variables:

- **AWS_ACCESS_KEY_ID** -  Specifies an AWS access key

- **AWS_SECRET_ACCESS_KEY** - Specifies the secret key associated with the access key. This is essentially the "password" for the access key.


## Usage:

```bash
$ s3-client
usage: s3-client [-h] [-d] [-e ENDPOINT] [-r REGION_NAME] {listbuckets,listobj,deleteobj,metadataobj,upload,download} ...

S3 Client sample script

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           debug flag
  -e ENDPOINT, --endpoint ENDPOINT
                        S3 endpoint URL
  -r REGION_NAME, --region REGION_NAME
                        S3 Region Name

Commands:
  {listbuckets,listobj,deleteobj,metadataobj,upload,download}
    listbuckets         List all buckets
    listobj             List objects in a bucket
    deleteobj           Delete object in a bucket
    metadataobj         List object metadata
    upload              Upload files to bucket
    download            Download files from bucket

    Example of use:
        s3-client listbuckets
        s3-client -r us-east-1 listbuckets
        s3-client -e https://s3.amazonaws.com listobj my_bucket -t
        s3-client -e https://s3.amazonaws.com upload my_bucket -f file1
        s3-client -e https://s3.amazonaws.com upload my_bucket -d mydir
```

### Example:

#### List buckets

```bash
$ s3-client -e https://s3.amazonaws.com listbuckets
Bucket_Name: test-script1 Creation_Date: 2019-03-22 19:40:36.379000+00:00
Bucket_Name: test-script2 Creation_Date: 2019-03-22 19:50:35.706000+00:00
```

#### Upload file(s)

```bash
$ s3-client -e https://s3.amazonaws.com upload my_bucket -f my_file.csv
Uploading file my_file.csv with object name my_file.csv
data transferred: 100%|█████████████████████████████████████████████████████████████| 8.39M/8.39M [00:16<00:00, 520kB/s]
  - Elapsed time 16.1471 seconds
  - Upload completed successfully


$ s3-client -e https://s3.amazonaws.com upload my_bucket -d mydir/ --nopbar
Uploading file mydir/test2 with object name mydir/test2
  - Elapsed time 0.1007 seconds
  - Upload completed successfully
Uploading file mydir/test1 with object name mydir/test1
  - Elapsed time 0.1183 seconds
  - Upload completed successfully
Uploading file mydir/internal/test3 with object name mydir/internal/test3
  - Elapsed time 0.1277 seconds
  - Upload completed successfully
Uploading file mydir/internal/deep/test4 with object name mydir/internal/deep/test4
  - Elapsed time 0.1114 seconds
  - Upload completed successfully
Uploading file mydir/internal/deep/test5 with object name mydir/internal/deep/test5
  - Elapsed time 0.0683 seconds
  - Upload completed successfully
```

#### List objects in a bucket

```bash
$ s3-client listobj -h
usage: s3-client listobj [-h] [--limit LIMIT] [--table] [--prefix PREFIX] bucket

positional arguments:
  bucket                Bucket Name

optional arguments:
  -h, --help            show this help message and exit
  --limit LIMIT, -l LIMIT
                        Limit the number of objects returned
  --table, -t           Show output as table
  --prefix PREFIX, -p PREFIX
                        Only objects with specific prefix
```


```bash
$ s3-client -e https://s3.amazonaws.com listobj my_bucket
key: mydir/internal/deep/test4 size: 10 storage_class: STANDARD e_tag: "d41d8cd98f00b204e9800998ecf8427e" last_modified: 2020-08-21 16:40:42.790000+00:00
key: mydir/internal/deep/test5 size: 203 storage_class: STANDARD e_tag: "7c41d8cd98f00b204e9800998ecf8427" last_modified: 2020-08-21 16:40:42.894000+00:00
key: mydir/internal/test3 size: 42 storage_class: STANDARD e_tag: "9acbdfd98f00b204e9100998ecf8423a" last_modified: 2020-08-21 16:40:42.655000+00:00
key: mydir/test1 size: 1031 storage_class: STANDARD e_tag: "8acffca98f00b204e98a0821ecf8447e" last_modified: 2020-08-21 16:40:42.538000+00:00
key: mydir/test2 size: 121 storage_class: STANDARD e_tag: "d3ab64d98f00b20401800998ecf8438b" last_modified: 2020-08-21 16:40:42.429000+00:00


$ s3-client -e https://s3.amazonaws.com listobj my_bucket -t
| key                       |   size | storage_class   | e_tag                              | last_modified                    |
|---------------------------|--------|-----------------|------------------------------------|----------------------------------|
| mydir/internal/deep/test4 |     10 | STANDARD        | "d41d8cd98f00b204e9800998ecf8427e" | 2020-08-21 16:40:42.790000+00:00 |
| mydir/internal/deep/test5 |    203 | STANDARD        | "7c41d8cd98f00b204e9800998ecf8427" | 2020-08-21 16:40:42.894000+00:00 |
| mydir/internal/test3      |     42 | STANDARD        | "9acbdfd98f00b204e9100998ecf8423a" | 2020-08-21 16:40:42.655000+00:00 |
| mydir/test1               |   1031 | STANDARD        | "8acffca98f00b204e98a0821ecf8447e" | 2020-08-21 16:40:42.538000+00:00 |
| mydir/test2               |    121 | STANDARD        | "d3ab64d98f00b20401800998ecf8438b" | 2020-08-21 16:40:42.429000+00:00 |
```

#### Download objects

```bash
$ s3-client -e https://s3.amazonaws.com download -h
usage: s3-client download [-h] [--nopbar] [-l LOCALDIR] [-o] (-f FILENAME | -p PREFIX) bucket

positional arguments:
  bucket                Bucket Name

optional arguments:
  -h, --help            show this help message and exit
  --nopbar              Disable progress bar
  -l LOCALDIR, --localdir LOCALDIR
                        Local directory to save downloaded file. Default current directory
  -o, --overwrite       Overwrite local destination file if it exists. Default false
  -f FILENAME, --file FILENAME
                        Download a specific file
  -p PREFIX, --prefix PREFIX
                        Download recursively all files with a prefix.
```

```bash
$ s3-client -e https://s3.amazonaws.com download my_bucket -f mydir/test1
Error: File ./mydir/test1 exist. Remove it from local drive to download.

$ s3-client -e https://s3.amazonaws.com download my_bucket -f mydir/test1 --overwrite
Downloading object mydir/test1 to path ./mydir/test1
data transferred: 100%|███████████████████████████████████████████████| 5.24M/5.24M [00:11<00:00, 468kB/s]
  - Elapsed time 11.3103 seconds
  - Download completed successfully
```

```bash
$ s3-client -e https://s3.amazonaws.com download my_bucket -f mydir/test1 -l /tmp/
Downloading object mydir/test1 to path /tmp/mydir/test1
data transferred: 100%|███████████████████████████████████████████████| 5.24M/5.24M [00:11<00:00, 468kB/s]
  - Elapsed time 11.3103 seconds
  - Download completed successfully

$ ls /tmp/mydir/test1
/tmp/mydir/test1
```
