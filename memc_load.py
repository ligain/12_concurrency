#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import threading
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import time
import multiprocessing
from queue import Queue

import appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


class ProcessFile(multiprocessing.Process):
    def __init__(self, file_descriptor, device_memc, writer_threads=4, dry_run=False):
        multiprocessing.Process.__init__(self)
        self.file_descriptor = file_descriptor
        self.device_memc = device_memc
        self.dry_run = dry_run
        self.writer_threads = writer_threads
        self.queue = Queue()
        self.writers = []
        self.errors = 0
        self.processed = 0
        self.errors_lock = threading.RLock()
        self.processed_lock = threading.RLock()

    def parse_appsinstalled(self, line):
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

    def read_file(self, file_descriptor):
        try:
            for line in file_descriptor:
                line = line.strip()
                if not line:
                    continue
                appsinstalled = self.parse_appsinstalled(line)
                if not appsinstalled:
                    with self.errors_lock:
                        self.errors += 1
                    continue
                memc_addr = self.device_memc.get(appsinstalled.dev_type)
                if not memc_addr:
                    with self.errors_lock:
                        self.errors += 1
                    logging.error("Unknown device type: %s" % appsinstalled.dev_type)
                    continue
                ua = appsinstalled_pb2.UserApps()
                ua.lat = appsinstalled.lat
                ua.lon = appsinstalled.lon
                key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
                ua.apps.extend(appsinstalled.apps)
                packed = ua.SerializeToString()
                yield memc_addr, key, packed
                # self.queue.put((memc_addr, key, packed))
        finally:
            file_descriptor.close()

    def put_data_in_queue(self):
        for item in self.read_file(self.file_descriptor):
            self.queue.put(item)

    def write_data(self):
        while True:
            item = self.queue.get()
            if item is None:
                break
            memc_addr, key, packed = item
            try:
                if self.dry_run:
                    logging.debug("%s -> %s" % (memc_addr, key))
                else:
                    memc = memcache.Client([memc_addr], socket_timeout=1)
                    write_status = memc.set(key, packed)
                    if not write_status:
                        with self.errors_lock:
                            self.errors += 1
                    logging.info("Item %s saved with return code: %s" % (key, write_status))
            except Exception as e:
                logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
                with self.errors_lock:
                    self.errors += 1
            with self.processed_lock:
                self.processed += 1

    def run(self):
        queue_writer = threading.Thread(
            name='queue_writer',
            target=self.put_data_in_queue
        )
        queue_writer.start()

        for writerno in range(self.writer_threads):
            writer = threading.Thread(
                name='writer-thread-{}'.format(writerno),
                target=self.write_data
            )
            writer.start()
            self.writers.append(writer)

        queue_writer.join()
        # stop writers
        for _ in range(self.writer_threads):
            self.queue.put(None)

        for writer in self.writers:
            writer.join()

        err_rate = float(self.errors) / self.processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successful load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        logging.info("Processed lines: %d" % self.processed)


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    processing_files = []
    for file_path in glob.iglob(options.pattern):
        logging.info('Processing %s' % file_path)
        with gzip.open(file_path) as file_descriptor:
            file_process = ProcessFile(
                file_descriptor=file_descriptor,
                device_memc=device_memc,
                dry_run=options.dry
            )
            file_process.start()
            processing_files.append(file_process)
        dot_rename(file_path)

    for file_process in processing_files:
        file_process.join()


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
    logging.basicConfig(filename=opts.log,
                        level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(processName)s %(threadName)s %(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
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
