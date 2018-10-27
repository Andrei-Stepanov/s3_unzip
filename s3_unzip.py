#!/usr/bin/env python3

# The MIT License (MIT)
#
# Copyright (c) 2018 Andrei Stepanov <andrusha@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
#
# Notice
# ======
#
# Big files are uploaded with multipart technique. If upload fails, chunks will
# stays in a bucket. You will be charged even for partially uploaded files.
# Make sure that you set:
#
#       lifecycle policy on the bucket to clean out chunks after x days
#
# Env vars
# ========
#
#       ARG_BUCKET_OUT, default is input bucket
#       ARG_KEY_PREFIX_OUT, default is 'extracted'
#       ARG_LOG_LEVEL, default is warn. Choose from: warn, info, debug
#

import io
import os
import abc
import sys
import zlib
import json
import errno
import base64
import shutil
import urllib
import hashlib
import zipfile
import logging
import argparse

logger = logging.getLogger(__name__)


try:
    import boto3
    import botocore
except ImportError:
    logger.error("To work with AWS S3 install boto3 module")


def le2int64(bytes):
    # little-endian to integer
    r = 0
    shift = 0
    for i in bytes:
        r += int(i) << shift
        shift += 8
    return r


class ZipFileFactory(object):

    def __new__(cls, *args, **kwargs):
        """Creates an object that follows ZipFile interface.

        Parameters
        ----------
        args : list
            Passes positial arguments as an input to corresponding constructor
            implementation.
        kwargs : dict
            Passes named arguments as an input to corresponding constructor
            implementation.
        """
        if kwargs.get('bucket', None) and kwargs.get('key', None):
            return S3File(kwargs['bucket'], kwargs['key'])
        if kwargs.get('zipfile', None):
            return LocalFile(kwargs['zipfile'])
        if kwargs.get('zipfile', None):
            return LocalFile(kwargs['zipfile'])
        else:
            logger.error('Use -h for help.')
            sys.exit()


class ZipFile(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        """Common constructor for ZipFile objects.

        Initializes pointers to specific fields in .ZIP file.

        For ZIP file format check:

            https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT

        Creates a fake truncated ZIP file, that can be read by Python zipfile
        module.

        """
        # end-of-central-directory record
        self.size = self.get_fsize()
        eocd_start = self.size - 22
        eocd_size = 22
        self.eocd = self.get_data(eocd_start, eocd_size)
        self.eocdl64 = b''
        self.eocdr64 = b''
        if self.eocd[16:20] == b'\xff\xff\xff\xff':
            logger.debug('Use zip64')
            # zip64 end of central directory locator
            eocdl64_start = self.size - 42
            eocdl64_size = 4 + 4 + 8 + 4
            self.eocdl64 = self.get_data(eocdl64_start, eocdl64_size)
            logger.debug("eocdl64_start: %d eocdl64_size: %d", eocdl64_start, eocdl64_size)
            # zip64 end of central directory record
            eocdr64_start = self.size - 98
            eocdr64_size = 4 + 8 + 2 + 2 + 4 + 4 + 8 + 8 + 8 + 8
            self.eocdr64 = self.get_data(eocdr64_start, eocdr64_size)
            logger.debug("eocdr64_start: %d eocdr64_size: %d", eocdr64_start, eocdr64_size)
            self.cd_size = le2int64(self.eocdr64[40:48])
            self.cd_start = le2int64(self.eocdr64[48:56])
        else:
            self.cd_size = le2int64(self.eocd[12:16])
            self.cd_start = le2int64(self.eocd[16:20])
        logger.debug("cd_start: %d cd_size: %d", self.cd_start, self.cd_size)
        self.cd = self.get_data(self.cd_start, self.cd_size)
        bstream = io.BytesIO(self.cd + self.eocdr64 + self.eocdl64 + self.eocd)
        self.mock_zip = zipfile.ZipFile(bstream, allowZip64=True)

    @abc.abstractmethod
    def get_data(self, start, size):
        """Sets requirements for classes that implement ZipFile interface.  All
        ZipFile implementations must implement this method.
        """
        return None

    @abc.abstractmethod
    def get_fsize(self, start, size):
        """Sets requirements for classes that implement ZipFile interface.  All
        ZipFile implementations must implement this method.
        """
        return None


class S3File(ZipFile):

    cache_size = 50 * 1024 * 1024

    def __init__(self, bucket, key):
        """This implementation for ZipFile interface that works with ZIP files
        stored at AWS S3.

        Parameters
        ----------
        bucket : str
            Unique name for S3 bucket.
        key : str
            File name in S3 bucket.
        """
        self.bucket = bucket
        self.key = key
        config = botocore.client.Config(signature_version=botocore.UNSIGNED)
        #self.s3_client = boto3.client('s3', config=config)
        self.s3_client = boto3.client('s3')
        self.cache = bytearray()
        self.cache_start = 0
        self.cache_end = 0
        super(S3File, self).__init__()

    def get_data(self, start, size):
        """Get file contents from S3 bucket with help of boto3 API.

        Parameters
        ----------
        start : int
            Read data at `start` position.
        size : int
            Read data of `size` bytes.

        Returns
        -------
        bytes
            S3 file contents.
        """
        end = start + size - 1
        logger.debug("Get data start:%s size:%s", start, size)
        if size < self.cache_size:
            # Update cache?
            logger.debug("Via cache.")
            if start < self.cache_start or end > self.cache_end:
                self.cache_start = start
                self.cache_end = self.cache_start + self.cache_size - 1
                rng = "bytes=%s-%s" % (self.cache_start, self.cache_end)
                logger.debug("Get data at range: %s-%s", self.cache_start, self.cache_end)
                obj = self.s3_client.get_object(Bucket=self.bucket, Key=self.key, Range=rng)
                self.cache = bytearray(obj['Body'].read())
                logger.debug("Payload cache data len: %s", len(self.cache))
            cstart = start - self.cache_start
            cend = cstart + size
            data = self.cache[cstart:cend]
            logger.debug("Return data from cache: %s-%s (%sbytes)", cstart, cend, len(data))
            return data
        rng = "bytes=%s-%s" % (start, end)
        logging.debug("Get data at range: %s-%s", start, end)
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=self.key, Range=rng)
        return obj['Body'].read()

    def get_fsize(self):
        """Get file size from S3 bucket with help of boto3 API.

        Returns
        -------
        int
            S3 file size in bytes.
        """
        response = self.s3_client.head_object(Bucket=self.bucket, Key=self.key)
        return response['ContentLength']


class LocalFile(ZipFile):

    def __init__(self, fpath):
        self.fpath = fpath
        super(LocalFile, self).__init__()

    def get_data(self, start, size):
        """Get local file contents.

        Parameters
        ----------
        start : int
            Read data at `start` position.
        size : int
            Read data of `size` bytes.

        Returns
        -------
        bytes
            Local file contents.
        """
        end = start + size - 1
        logging.debug("Get data at range: %s-%s", start, end)
        with open(self.fpath, "rb") as infile:
            infile.seek(start)
            data = infile.read(size)
        return data

    def get_fsize(self):
        """Get local file size.

        Returns
        -------
        int
            File size in bytes.
        """
        return os.path.getsize(self.fpath)


def lambda_handler(event, context):
    """Entry point to this script from AWS.
    This function will be used to run AWS lambda.

    Parameters
    ----------
    event : json
        Message that triggered this lambda function.
    context: dict
        Environment context.

    Returns
    -------
    json
        On success returns 200 HTTP code.
    """
    args = {}
    args['bucket'] = urllib.parse.unquote_plus(event['Records'][0]['s3']['bucket']['name'])
    args['key'] = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    bucket_out = os.environ.get('ARG_BUCKET_OUT', args['bucket'])
    key_prefix_out = os.environ.get('ARG_KEY_PREFIX_OUT', 'extracted')
    log_level = os.environ.get('ARG_LOG_LEVEL', 'info')
    levels = {}
    levels['info'] = logging.INFO
    levels['debug'] = logging.DEBUG
    levels['warn'] = logging.WARN
    lvl = levels.get(log_level, logging.WARN)
    logger.setLevel(level=lvl)
    logger.info("Input bucket: %s", args['bucket'])
    logger.info("Input key: %s", args['key'])
    logger.info("Output bucket: %s", bucket_out)
    logger.info("Output key prefix: %s", key_prefix_out)
    zfile = ZipFileFactory(**args)
    prefix = os.path.join(key_prefix_out, args['key'])
    store = S3Store(bucket=bucket_out, prefix=prefix)
    unzip(zfile, store)
    msg = """
    Unzip done

    From: %s/%s
    To: %s/%s

    """ % (args['bucket'], args['key'], bucket_out, prefix)
    return {"statusCode": 200, "body": json.dumps(msg)}


def get_hash(data):
    """Auxiliary function used in HTTP communication to S3 bucket.

    Parameters
    ----------
    event : bytearray
        Raw data.

    Returns
    -------
    tuple
        md5 hash and the same hash encoded in base64
    """
    md5_hash = hashlib.md5()
    md5_hash.update(data)
    encoded = base64.b64encode(md5_hash.digest()).decode('utf8')
    digest = md5_hash.hexdigest()
    return encoded, digest


class S3Store(object):

    def __init__(self, *args, **kwargs):
        """Stores unzip data at s3 bucket. Object that stores unzip data.

        Parameters
        ----------
        bucket : str
            Store unzip files at specified bucket.
        prefix : str
            Store all unzip filed under common root prefix.
        """
        self.bucket = kwargs.get('bucket')
        self.prefix = kwargs.get('prefix', "")
        self.s3_client = boto3.client('s3')
        self.upload_id = None
        self.part_no = 1  # S3 multipart upload starts from 1
        self.parts = []
        self.opened_file = None
        self.opened_data = None
        self.is_multipart = False

    def close(self):
        """Dump all left data to S3 for opened file.
        """
        logger.debug("%s flush.", self.opened_file)
        self.store("ignore", "ignore")

    def store(self, fname, data):
        """Store unziped data appending it to file at S3.

        Parameters
        ----------
        fname : str
            File name.
        data : bytes
            Data necessary to append to the S3 file.
        """
        dest = os.path.join(self.prefix, fname)
        if not self.opened_file:
            logger.debug("%s: new file to put", dest)
            self.opened_file = dest
            self.opened_data = data
            return
        if self.opened_file and (self.opened_file != dest) and (not self.is_multipart):
            logger.debug("%s: upload file.", self.opened_file)
            md5_encoded, md5 = get_hash(self.opened_data)
            logger.debug("%s md5_encoded: %s, md5: %s", self.opened_file, md5_encoded, md5)
            metadata = {'md5': md5}
            try:
                response = self.s3_client.put_object(Bucket=self.bucket,
                                                     Key=self.opened_file,
                                                     Body=io.BytesIO(self.opened_data),
                                                     ContentMD5=md5_encoded,
                                                     Metadata=metadata)
                logger.debug("Put response %s", repr(response))
            except botocore.client.ClientError as e:
                logger.warn("%s fail upload key %s: %s", self.bucket, dest, repr(e.response))
            self.opened_file = dest
            self.opened_data = data
            return
        if self.opened_file and (self.opened_file == dest):
            if self.part_no == 1:
                logger.info("%s: start multipart upload", dest)
                response = self.s3_client.create_multipart_upload(
                    Bucket=self.bucket, Key=dest)
                self.upload_id = response['UploadId']
            md5_encoded, md5 = get_hash(self.opened_data)
            logger.debug("%s md5_encoded: %s, md5: %s", self.opened_file, md5_encoded, md5)
            logger.info("%s: upload part file, part_no: %s, size: %s",
                        self.opened_file, self.part_no, len(self.opened_data))
            try:
                response = self.s3_client.upload_part(
                    Bucket=self.bucket,
                    Key=self.opened_file,
                    Body=self.opened_data,
                    ContentMD5=md5_encoded,
                    PartNumber=self.part_no,
                    UploadId=self.upload_id)
                self.opened_data = None
                logger.info("%s: done: upload part file, part_no: %s", self.opened_file, self.part_no)
                self.parts.append({'ETag': response['ETag'], 'PartNumber': self.part_no})
            except Exception as err:
                logger.warn("Upload part failed [%s]", repr(err))
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.opened_file,
                    UploadId=self.upload_id)
                sys.exit()
            self.part_no += 1
            self.is_multipart = True
            self.opened_file = dest
            self.opened_data = data
            data = None
            return
        if self.opened_file and (self.opened_file != dest) and self.is_multipart:
            md5_encoded, md5 = get_hash(self.opened_data)
            logger.debug("%s md5_encoded: %s, md5: %s", self.opened_file, md5_encoded, md5)
            logger.info("%s: upload part file, part_no: %s, size: %s",
                        self.opened_file, self.part_no, len(self.opened_data))
            try:
                response = self.s3_client.upload_part(
                    Bucket=self.bucket,
                    Key=self.opened_file,
                    Body=io.BytesIO(self.opened_data),
                    ContentMD5=md5_encoded,
                    PartNumber=self.part_no,
                    UploadId=self.upload_id)
                self.parts.append({'ETag': response['ETag'], 'PartNumber': self.part_no})
            except Exception as err:
                logger.warn("Upload part failed [%s]", repr(err))
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.opened_file,
                    UploadId=self.upload_id)
                sys.exit()
            logger.info("%s: stop multipart upload", self.opened_file)
            response = self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.opened_file,
                MultipartUpload={'Parts': self.parts},
                UploadId=self.upload_id)
            self.part_no = 1
            self.is_multipart = False
            self.opened_file = dest
            self.upload_id = None
            self.parts = []
            self.opened_data = data
            return


class LocalStore(object):

    def __init__(self, *args, **kwargs):
        self.outdir = kwargs.get('outdir', "")
        self.opened_file = None

    def close(self):
        """File is closed each time after appending new data.
        """
        pass

    def store(self, fname, data):
        """Store unziped data appending it to local file.

        Parameters
        ----------
        fname : str
            File name.
        data : bytes
            Data necessary to append to the local file.
        """
        dest = os.path.join(self.outdir, fname)
        try:
            dirname = os.path.dirname(dest)
            if dirname and not os.path.exists(dirname):
                os.makedirs(dirname)
        except (OSError, IOError) as err:
            if err.errno != errno.EEXIST:
                raise
        if os.path.exists(dest) and os.path.isdir(dest):
            return
        if self.opened_file and self.opened_file.name != dest:
            self.opened_file.close()
            self.opened_file = None
        if not self.opened_file:
            self.opened_file = open(dest, 'wb')
        out_data = io.BytesIO(data)
        shutil.copyfileobj(out_data, self.opened_file)
        out_data.close()


def unzip(zfile, store):
    """Unzip one entry from .zip file.

    https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT

    Parameters
    ----------
    zfile : object
        Object that implements ZipFile interface.
    store : object
        Object that has store method.
    """
    for zi in zfile.mock_zip.filelist:
        logger.info("%s: modified=%s arch_size=%d size=%d comment=%s",
                    zi.filename,
                    ("%d-%d-%d %d:%d:%d" % zi.date_time),
                    zi.compress_size,
                    zi.file_size,
                    repr(zi.comment))
        header_start = zfile.cd_start + zi.header_offset + 26
        header_size = 4
        file_head = zfile.get_data(header_start, header_size)
        name_len = file_head[0] + (file_head[1] << 8)
        extra_len = file_head[2] + (file_head[3] << 8)
        data_start = zfile.cd_start + zi.header_offset + 30 + name_len + extra_len
        logger.debug("%s: data start: %s", zi.filename, data_start)
        logger.debug("%s: compress type: %s", zi.filename, zi.compress_type)
        # S3 multipart minimal chunk size is 5 MB: 1025*1024*5 / (32*1024) = 161 min
        chunk_size = (1 << zlib.MAX_WBITS) * 1152
        data_left = zi.compress_size
        chunk = 0
        decoder = zlib.decompressobj(-zlib.MAX_WBITS)
        out_data = bytearray()
        while data_left:
            read_size = min(data_left, chunk_size)
            data_left -= read_size
            logger.debug("%s: reading bytes: %s", zi.filename, read_size)
            logger.debug("%s: data left: %s", zi.filename, data_left)
            data = zfile.get_data(data_start, read_size)
            data_start += read_size
            logger.debug("%s: process chunk: %s", zi.filename, chunk)
            if zi.compress_type == zipfile.ZIP_DEFLATED:
                # A few compressed bytes can expand to gigs of memory. Take very small steps.
                minichnk = (1 << zlib.MAX_WBITS) * 4  # 128 kb compressed data
                for i in range(0, len(data), minichnk):
                    logger.debug("%s: process chunk: %s, part: %s", zi.filename, chunk, i)
                    in_data = data[i:i+minichnk]
                    out_data.extend(decoder.decompress(in_data))
                    if len(out_data) > (256 * 1024 * 1024):
                        logger.info("%s: send uncompressed data %sb", zi.filename, len(out_data))
                        store.store(zi.filename, out_data)
                        out_data = bytearray()
                        os.sync()
            else:
                store.store(zi.filename, data)
                pass
            chunk += 1
        if out_data:
            store.store(zi.filename, out_data)
            pass
    store.close()


def main():
    parser = argparse.ArgumentParser(description='Extract zip files.')
    group = parser.add_mutually_exclusive_group()
    parser.add_argument('-v', '--verbose', action='store_true', default=True,
                        help='verbose output')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='debug output')
    group.add_argument("-f", "--file", metavar='ZIPFILE', dest="zipfile",
                       help="input zip file")
    group.add_argument("-b", "--bucket", metavar='S3BUCKET', dest="bucket",
                       help="public S3 bucket")
    parser.add_argument("-k", "--key", metavar='S3KEY', dest="key",
                        help="public key in the bucket")
    parser.add_argument("-p", "--path", metavar='OUTDIR', default="",
                        dest="outdir", help="extract archive to directory")
    args = vars(parser.parse_args())
    if args['debug']:
        lvl = logging.DEBUG
    elif args['verbose']:
        lvl = logging.INFO
    else:
        lvl = logging.WARN
    logger.setLevel(level=lvl)
    zfile = ZipFileFactory(**args)
    store = LocalStore(**args)
    unzip(zfile, store)

if __name__ == '__main__':
    sys.exit(main())
