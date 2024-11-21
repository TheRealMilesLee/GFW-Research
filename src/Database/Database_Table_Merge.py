"""
This file is meant to merge the database tables in the Mongodb.
We have two databases, afterDomainChange and beforeDomainChange. In those database, we have three type of the collections:

1. DNSPoisoning
2. IPBlocking
3. GFW Location

The purpose of this file is to merge the tables in the two databases. The merging process is done by the domain name.
So in the end, we wish we only have one database, with three table: DNSPoisoning, IPBlocking, GFW Location.

The key of the merging would be base on domains. If the domain is the same, we will merge the data together.
"""
import concurrent.futures
import csv
import logging
import os
from collections import defaultdict

from DBOperations import ADC_db, BDC_db, MongoDBHandler
