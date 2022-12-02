from .config import *

"""These are the types of import we might expect in this file"""
import httplib2
#import oauth2
#import tables
#import mongodb
#import sqlite
import urllib.request
import numpy as np
from shapely.geometry import Point 
import geopandas as gpd
import pandas as pd
import seaborn as sbn
import pandas as pd

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """


def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError

def kexecute(conn, sqlquery =""):
    curfresh = conn.cursor()
    curfresh.execute(sqlquery)
    ret = curfresh.fetchall()
    curfresh.close()
    return ret
    
def create_pp_data(conn):
  cur= conn.cursor()
  cur.execute(f'''
  DROP TABLE IF EXISTS `pp_data`;''')
  cur.execute(f'''
  CREATE TABLE IF NOT EXISTS `pp_data` (
    `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
    `price` int(10) unsigned NOT NULL,
    `date_of_transfer` date NOT NULL,
    `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
    `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
    `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
    `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
    `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
    `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
    `street` tinytext COLLATE utf8_bin NOT NULL,
    `locality` tinytext COLLATE utf8_bin NOT NULL,
    `town_city` tinytext COLLATE utf8_bin NOT NULL,
    `district` tinytext COLLATE utf8_bin NOT NULL,
    `county` tinytext COLLATE utf8_bin NOT NULL,
    `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
    `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
    `db_id` bigint(20) unsigned NOT NULL
  ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;''')
  ret =  cur.fetchall()
  cur.close()
  return ret

def download_prop_data(beg_year=1995, end_year=2022):
  for year in range(beg_year, end_year+1):
    urllib.request.urlretrieve(f'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{year}.csv', f'pp_data{year}.csv')
   
def upload_prop_data(conn, beg_year=1995, end_year=2022):
  for year in range(beg_year, end_year+1):
    print(year)
    kexecute(conn, f'''
    LOAD DATA LOCAL INFILE 'pp_data{year}.csv' INTO TABLE pp_data
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"';
    ''')
  
def sanity_checks(conn):
    """
    Print out some sanity_checks to make sure the database was created succesfully
    :param conn: the Connection object
    """
    print("\n indexes in pp data are:")
    print(kexecute(conn, f'SHOW INDEXES from pp_data;'))
    print("\nsum of all entries:")
    print(kexecute(conn, f'SELECT COUNT(*) FROM `pp_data`')) #this supposed to be around 27mln for data until oct-2022
    print("\nsum of all entries on March 15 2018:")
    print(kexecute(conn, f'SELECT COUNT(*) FROM pp_data WHERE date_of_transfer = "2018-03-15"'))
    print("\nsum of all entries before 2003:")
    print(kexecute(conn, f'SELECT COUNT(*) FROM pp_data WHERE date_of_transfer < "2003-01-01"'))
    print("\n sum of all entries by year:")
    print(kexecute(conn, f'SELECT YEAR(date_of_transfer), COUNT(*) FROM pp_data GROUP BY YEAR(date_of_transfer)')) #correct counts for this below :)
    print("\n# of entries in postcode_data")
    print(kexecute(conn, f'SELECT max(db_id) FROM postcode_data')) #2.6MLN
    print("\n columns of pp_data are:", access.kexecute(conn, f"show columns from pp_data")) #columns
    print("\n columns of postcode_data are:\n", access.kexecute(conn, f"show columns from postcode_data")) #columns
    print("\n the first 5 entries of pp_data are above\n", access.head(conn, "pp_data", 5))
    print("\n the first 5 entries of postcode_data are above\n", access.head(conn, "postcode_data", 5))
    
def select_top(conn, table,  n):
    """
    Query n first rows of the table
    :param conn: the Connection object
    :param table: The table to query
    :param n: Number of rows to query
    """
    curnew = conn.cursor()
    curnew.execute(f'SELECT * FROM {table} LIMIT {n}')

    rows = curnew.fetchall()
    curnew.close()
    return rows

def head(conn, table, n=5):
    """
    Print n first rows of the table
    :param conn: the Connection object
    :param table: The table to query
    :param n: Number of rows to query
    """
    rows = select_top(conn, table, n)
    for r in rows:
      print(r)
    
def bound_box(longitude, latitude, radius):
    """
    get bounding box of a point
    :param longitude: long. of the centre
    :param latitude: lat. of the centre
    :param radius: size of the side of the box
    """
    return (
      longitude - radius/2,
      longitude + radius/2,
      latitude  - radius/2,
      latitude  + radius/2)
#def data_info(tablename = "pp_data", ):
    
def join_to_df(rows):
    """ Convert query results into a DataFrame format.
    :param rows: query results in rows
    :return: query results in a DataFrame with labelled columns
    """
    return pd.DataFrame(rows, columns=['county', 'date_of_transfer', 'db_id', 'district', 'locality',
                                       'new_build_flag', 'postcode','price','property_type',
                                       'tenure_type','town_city','latitude', 'longitude',  'country', 
                                       ])
    
   

