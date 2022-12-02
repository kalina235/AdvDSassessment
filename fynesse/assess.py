from .config import *

from fynesse import access
import math
from math import radians, cos, sin, asin, sqrt
import geopy.distance
import matplotlib
import datetime
import statsmodels.api as sm
import pygeos
import pgeocode
import datetime
import pandas as pd
import matplotlib
import osmnx as ox
import matplotlib.pyplot as plt
import mlai
import mlai.plot as plot
import rtree
"""These are the types of import we might expect in this file
import pandas
import bokeh
import seaborn
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""


def inner_join(conn, coord = None, dates = None, extra_stuff = None, radius = 0.5):
  return access.inner_join(conn, coord, dates, extra_stuff, radius)

def data():
    """Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names informative, date and times correctly formatted. Return a structured data structure such as a data frame."""
    df = access.data()
    raise NotImplementedError

def query(data):
    """Request user input for some aspect of the data."""
    raise NotImplementedError

def view(data):
    """Provide a view of the data that allows the user to verify some aspect of its quality."""
    raise NotImplementedError

def labelled(data):
    """Provide a labelled set of data ready for supervised learning."""
    raise NotImplementedError

def postcode_to_coord(pcode): 
    """return a (longitude, latitude) pair given a postcode
    param:pcode: string: a postcode to convert into coords"""
    nomi = pgeocode.Nominatim('gb')
    return (nomi.query_postal_code(pcode)['longitude'], nomi.query_postal_code(pcode)['latitude'])    

def plot_price_heatmap(conn, location, df, dates):
    #df['price'] = df.price.apply(math.log)
    beg,end = dates
    df.plot(kind='scatter', x='longitude', y='latitude', alpha=0.7, s=12.2, c=2, cmap=plt.get_cmap('jet'), colorbar=True)
    end = min(end, datetime.date.fromisoformat("2022-11-01"))
    plt.title('Price Map in ' + location + str(beg) + '--' + str(end))
    plt.xlabel('latitude')
    plt.ylabel('longitude')
    plt.show()
    
def plot_heatmaps(conn, location, dates, period_in_days, coord = None, modulo = 100):
    begdate, enddate = dates
    delta = datetime.timedelta(days=period_in_days)
    while(begdate < enddate):
      j = begdate+delta
      dfslice = inner_join(conn, coord, dates = (begdate, j), extra_stuff = f"pp.db_id mod {modulo} = 0")
      plot_price_heatmap(conn, location, dfslice, dates = (begdate,j))
      begdate = j
    
def plot_price_through_date_range(conn, df, info, date_range=356, box_radius=0.02, property_type = " "):
    df['date_of_transfer'] = pd.to_datetime(df['date_of_transfer'])
    if(property_type != " "):
      df = df[df['property_type'] == property_type]
    plt.figure(figsize=(14,6))
    sns.lineplot(data=df, x='date_of_transfer', y='price', color='blue')
    plt.title("Price variation in time for houses in" + info + " " + property_type )
    plt.xlabel('date')
    plt.ylabel('price')
    plt.legend()
    plt.show()

def price_by_year(df, info):
    fig, ax = plt.subplots()
    df = df[df['price'] < 10000000] # removing anomaly
    df['date_of_transfer'] = pd.to_datetime(df['date_of_transfer'])
    to_plot = df.groupby(df.date_of_transfer.dt.year).mean()
    ax.set_title(info)
    ax.plot(to_plot.price)
    
def plot_price_histogram(df, prop_type, info, log = False):
    """
    Plot a histogram of prices of a certain type
    :param df: property price df
    :param prop_type: type of house
    """
    if prop_type != "All":
        df = df.loc[df['property_type'] == prop_type]

    if(log):
      df.price = df.price.apply(math.log)
    if(log):
      plt.xlabel('Log Price')
    else:
      plt.xlabel('Price')
      plt.ylabel('Frequency')
    fig, ax = plt.subplots()
    ax.set_title("Price histogram for " + info+ " property type: " + prop_type)
    ax.hist(df['price'], 50, density=True)

def plot_house_prices(df, info, types=True, log = False):
    """
    Plot house prices
    :param df: property price data
    :param type: plot by type or all
    """
    if types:
        plot_price_histogram(df, "F", info, log)
        plot_price_histogram(df, "S", info, log)
        plot_price_histogram(df, "D", info, log)
        plot_price_histogram(df, "T", info, log)
        plot_price_histogram(df, "O", info, log)
    else:
        plot_price_histogram(df, "All", info, log)
        
def plot_house_types_distributions(conn, info, df, log = False):
    if(log):
      df.price = df.price.apply(math.log)
    by_property = df.groupby('property_type')
    fig, axs = plt.subplots(1,5)
    fig.suptitle('Price distributions in '+  info + " for the different types of houses", fontsize=20)
    property_types = ['D', 'S', 'T', 'F', 'O']
    labels = ['detached', 'semi-detached', 'terraced', 'flat', 'other']
    colors = ['pink', 'purple', 'green', 'orange', 'cyan']
    i = 0
    for pt in property_types:
      df.loc[(df.property_type == 'pt')]
      axs[i].hist(df.loc[(df.property_type == pt)].price, edgecolor=colors.pop(), alpha=.9, linewidth=2)
      i = i + 1

    fig.tight_layout()
    fig.legend(axs, labels=labels, loc="upper right", borderaxespad=0.1)
    fig.subplots_adjust(top=0.85)
    
def haversine_dist(row, coords):
    lon1, lat1 = coords
    longs, lats = row
    lon2 = np.array(longs)
    lat2 = np.array(lats)
    lon1, lat1 = map(radians, [lon1, lat1])
    lon2 = np.array([radians(lon) for lon in lon2])
    lat2 = np.array([radians(lat) for lat in lat2])
    dlon = np.subtract(lon2, lon1) 
    dlat = np.subtract(lat2, lat1)
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a)) 
    km = 6367 * c
    return km

def plot_distance_vs_price(conn, location, coords, pt, radius=0.1):
    df = inner_join(conn, coords, last_year, radius = radius, extra_stuff = f"pp.property_type = '{pt}'")
    df.latitude = df.latitude.astype("float")
    df.longitude = df.longitude.astype("float")
    longitude, latitude = coords
    df['distance'] = haversine_dist((df.longitude, df.latitude), (longitude, latitude))
    plt.figure(figsize=(14,6))
    df.plot(kind='scatter', x='distance', y='price',
            c='distance', cmap=plt.get_cmap('jet'), colorbar=True)
    plt.title('Price vs Distance from cooords ' + str(coords))
    plt.xlabel('distance')
    plt.ylabel('price')
    plt.show()
    
  

