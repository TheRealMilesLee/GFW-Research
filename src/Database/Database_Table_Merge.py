import concurrent.futures
import csv
import logging
import os
from collections import defaultdict

from DBOperations import ADC_db, BDC_db, MongoDBHandler
