"""
@brief: This script is used to dump the data from the csv file to the mongo database.
@Author: Hengyi Li
@Date: 2024-09-24
@Version: 1.0
@Copyright: (c) 2024 Hengyi Li. All rights reserved.
"""

import csv
import sys
import logging
import argparse
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Set up the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up the argument parser
parser = argparse.ArgumentParser(description="Dump the data from the csv file to the mongo database.")
parser.add_argument("--csv_file", help="The path to the csv file.")
parser.add_argument("--mongo_host", help="The host of the mongo database.")
parser.add_argument("--mongo_port", help="The port of the mongo database.")
parser.add_argument("--mongo_db", help="The name of the mongo database.")
parser.add_argument("--mongo_collection", help="The name of the mongo collection.")
args = parser.parse_args()

# Check the arguments
if not args.csv_file:
    logger.error("The path to the csv file is required.")
    sys.exit(1)
if not args.mongo_host:
    logger.error("The host of the mongo database is required.")
    sys.exit(1)
if not args.mongo_port:
    logger.error("The port of the mongo database is required.")
    sys.exit(1)
if not args.mongo_db:
    logger.error("The name of the mongo database is required.")
    sys.exit(1)
if not args.mongo_collection:
    logger.error("The name of the mongo collection is required.")
    sys.exit(1)

# Connect to the mongo database
try:
    client = MongoClient(args.mongo_host, int(args.mongo_port))
    db = client[args.mongo_db]
    collection = db[args.mongo_collection]
except ConnectionFailure as e:
    logger.error("Failed to connect to the mongo database: %s" % e)
    sys.exit(1)
except OperationFailure as e:
    logger.error("Failed to connect to the mongo database: %s" % e)
    sys.exit(1)

# Read the data from the csv file
try:
    with open(args.csv_file, "r") as f:
        reader = csv.DictReader(f)
        data = [row for row in reader]
except Exception as e:
    logger.error("Failed to read the data from the csv file: %s" % e)
    sys.exit(1)

# Insert the data into the mongo database
try:
    collection.insert_many(data)
except OperationFailure as e:
    logger.error("Failed to insert the data into the mongo database: %s" % e)
    sys.exit(1)

logger.info("Successfully dumped the data from the csv file to the mongo database.")
