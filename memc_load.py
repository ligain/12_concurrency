#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import time

import multiprocessing

import appsinstalled_pb2
# pip install python3-memcached
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(queue, processed, errors, dry_run=False):
    while True:
        item = queue.get()
        if item is None:
          break
        memc_addr, key, packed = item
        try:
            if dry_run:
                logging.debug("%s -> %s" % (memc_addr, key))
            else:
                memc = memcache.Client([memc_addr], socket_timeout=1)
                memc.set(key, packed)
        except Exception as e:
            logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
            with errors.get_lock():
                errors.value += 1
        with processed.get_lock():
            processed.value += 1


def parse_appsinstalled(line):
    line_parts = line.strip().decode().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def generate_appsinstalled(file_descriptor, queue, device_memc, errors):
    for line in file_descriptor:
        line = line.strip()
        if not line:
            continue
        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            with errors.get_lock():
                errors.value += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            with errors.get_lock():
                errors.value += 1
            logging.error("Unknown device type: %s" % appsinstalled.dev_type)
            continue
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        queue.put((memc_addr, key, packed))


def main(options):
    queue = multiprocessing.Queue()
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    for fn in glob.iglob(options.pattern):
        processed = multiprocessing.Value('i', 0)
        errors = multiprocessing.Value('i', 0)
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn)

        produce_appsinstalled = multiprocessing.Process(
            target=generate_appsinstalled,
            args=(fd, queue, device_memc, errors)
        )
        save_appsinstalled = multiprocessing.Process(
            target=insert_appsinstalled,
            args=(queue, processed, errors),
            kwargs=({'dry_run': options.dry})
        )

        produce_appsinstalled.start()
        save_appsinstalled.start()

        produce_appsinstalled.join()
        queue.put(None)         # sentinel
        save_appsinstalled.join()

        err_rate = float(errors.value) / processed.value
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successful load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(processName)s %(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        start = time.perf_counter()
        main(opts)
        end = time.perf_counter()
        logging.info("Script completed in: {} sec".format(end - start))
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
