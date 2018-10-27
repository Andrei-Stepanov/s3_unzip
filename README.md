# Info

Python 3 unzip script with next characteristics:

1. Can be run locally or triggered by AWS lambda.
2. Supports ZIP64.
3. Can unzip big files in a few GB size with low memory consuming.
4. Unzips local zip file and store files locally.
5. Unzips local zip file and store extracted files at AWS S3 bucket.
6. Unzip S3 .zip file and store .zip contents at S3.

Lambda and the S3 should be in the same region for best performance.

Could be timeout issue at around 4GB when the s3 is even in a nearby region,
but it disappears with a wide margin once everything is in the same region.
There's just a limit on how quickly AWS can copy files to a different S3
region.

# S3 bucket settings

Big files are uploaded with multi part technique. If upload fails, chunks will
stays in a bucket. You will be charged even for partially uploaded files.  Make
sure that you set: life cycle policy on the bucket to clean out chunks after x
days.
