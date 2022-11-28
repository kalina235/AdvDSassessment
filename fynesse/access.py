from .config import *

"""These are the types of import we might expect in this file"""
import httplib2
import oauth2
import tables
import mongodb
import sqlite
import urllib.request

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
   
def upload_prop_data(beg_year=1995, end_year=2022):
  for year in range(beg_year, end_year+1):
    print(year)
    cur.execute(f'''
    LOAD DATA LOCAL INFILE 'pp_data{year}.csv' INTO TABLE pp_data
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"';
    ''')
  
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
  rows = select_top(conn, table, n)
  for r in rows:
      print(r)
    
#def data_info(tablename = "pp_data", ):
    
   

