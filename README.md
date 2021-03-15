Based of StackOverflow Post: https://stackoverflow.com/questions/45429556/how-list-amazon-s3-bucket-contents-by-modified-date


# Find Objects in a bucket within certain timeframes, then print them or save to a file

## Example

s3findbydate.py -b mr-b1 -p "trash" -d 7 -c rm -e http://<device ip> -f deleteme.txt

Contents of file look like:

rm s3://mr-b1/trash/t984
rm s3://mr-b1/trash/t985
rm s3://mr-b1/trash/t986
rm s3://mr-b1/trash/t987
rm s3://mr-b1/trash/t988


Then you can use something like s5cmd to delete the object in Parallel
i.e.
#cat deleteme.txt | s5cmd --endpoint-url http://<device ip> run

```
usage: s3findbydate.py [-h] [-b BUCKET] [-p PREFIXES [PREFIXES ...]] [-s [SUFFIXES ...]] [-n LAST_MODIFIED_MIN] [-x LAST_MODIFIED_MAX] [-f FILE] [-c CMD] [-d DAYS]
                       [-e ENDPOINT_URL] [-v]

List keys in S3 bucket for prefix

optional arguments:
  -h, --help            show this help message and exit
  -b BUCKET, --bucket BUCKET
                        S3 Bucket
  -p PREFIXES [PREFIXES ...], --prefixes PREFIXES [PREFIXES ...]
                        Filter s3 keys by a set of prefixes, warning recursive
  -s [SUFFIXES ...], --suffixes [SUFFIXES ...]
                        Filter s3 keys by a set of suffixes
  -n LAST_MODIFIED_MIN, --last_modified_min LAST_MODIFIED_MIN
                        Filter s3 content by minimum last modified date
  -x LAST_MODIFIED_MAX, --last_modified_max LAST_MODIFIED_MAX
                        Filter s3 content by maximum last modified date
  -f FILE, --file FILE  Optional: file to write keys to.
  -c CMD, --cmd CMD     prefix a cmd in front of object in the file or output i.e. rm
  -d DAYS, --days DAYS  Set last_modified_max to x number of days ago.
  -e ENDPOINT_URL, --endpoint-url ENDPOINT_URL
                        Use a different endpoint if not talking to AWS, i.e. FlashBlade
  -v, --verbose         Show Debug logging
```
