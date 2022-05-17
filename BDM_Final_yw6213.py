from pyspark.sql.session import SparkSession
import sys
import json
from pyspark.sql.functions import col
from pyspark.sql.functions import substring
import os
# def date_is_valid(date_range_start, date_range_end):
#     return (date_range_start >= '2019-03-01' and date_range_end <= '2019-03-31') \
#            or (date_range_start >= '2019-10-01' and date_range_end <= '2019-10-31') \
#            or (date_range_start >= '2020-03-01' and date_range_end <= '2020-03-31') \
#            or (date_range_start >= '2020-10-01' and date_range_end <= '2020-10-31')
def date_is_valid(date_range_start, date_range_end):
    return (date_range_start in ['2019-03','2019-10','2020-03','2020-10'] or date_range_end <= ['2019-03','2019-10','2020-03','2020-10'])


def date_range_flag(date_range_start_flag,date_range_end_flag):
    if(date_range_start_flag == '2019-03' or date_range_end_flag == '2019-03'):
        return '2019-03'
    elif(date_range_start_flag == '2019-10' or date_range_end_flag == '2019-10'):
        return '2019-10'
    elif (date_range_start_flag == '2020-03' or date_range_end_flag == '2020-03'):
        return '2020-03'
    elif (date_range_start_flag == '2020-10' or date_range_end_flag == '2020-10'):
        return '2020-10'
    else: return ''

#calculate the haversine distance
# def haversine(lon1, lat1, lon2, lat2):
#     lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
#     dlon = lon2 - lon1
#     dlat = lat2 - lat1
#     a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
#     c = 2 * np.arcsin(np.sqrt(a))
#     #unit is km
#     return c * 6367

#calculate the haversine distance

def haversine2(lon1, lat1, lon2, lat2):
    import math
    lon1 = math.radians(lon1)
    lat1 = math.radians(lat1)
    lon2 = math.radians(lon2)
    lat2 = math.radians(lat2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat * 0.5) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon * 0.5) ** 2
    # unit is km
    return 2 * math.asin(math.sqrt(a)) * 6371


def parse_vistor_home_cbgs(vistors_home_cbgs,list_nyc_cbg_centroids):
    dict_cbgs = json.loads(vistors_home_cbgs)
    cbgs = list(dict_cbgs.keys())
    if(len(cbgs) == 0):
        return None
    #only consider CBG FIPS for NYC
    cbgs = list(filter(lambda x:x in list_nyc_cbg_centroids,cbgs))
    return (cbgs)

def calulate_distance(cbg_latlong,poi_cbg_latlong):
    return haversine2(poi_cbg_latlong[1],poi_cbg_latlong[0],cbg_latlong[1],cbg_latlong[0])

def distances(x,dict_cbg_latlong):
    poi_cbg_latlong = dict_cbg_latlong.get(x[0][0])
    sum_dis = sum(map(lambda cbg:calulate_distance(dict_cbg_latlong.get(cbg),poi_cbg_latlong),x[1]))
    return (x[0],[sum_dis,len(x[1])])


def list_distance(group):
    cbg_fips = group[0]
    date_distances = dict(group[1])
    return (int(cbg_fips),date_distances.get('2019-03',''),date_distances.get('2019-10',''),date_distances.get('2020-03',''),date_distances.get('2020-10',''))



if __name__ == '__main__':


    output = sys.argv[1]
    spark = SparkSession.builder.appName("BDM_Final_yw6213") \
        .getOrCreate()

    nyc_cbg_centroids = os.getenv('SPARK_YARN_STAGING_DIR') + "/nyc_cbg_centroids.csv"
    nyc_supermarkets = os.getenv('SPARK_YARN_STAGING_DIR') + "/nyc_supermarkets.csv"
    #weekly_patterns = "weekly-pattern-nyc-2019-2020-sample.csv"
    weekly_patterns = "/tmp/bdm/weekly-patterns-nyc-2019-2020/*"


    data_nyc_supermarkets = spark.read \
        .option('header', True) \
        .option('delimiter', ',') \
        .option('inferSchema', True) \
        .csv(nyc_supermarkets) \
        .toDF('place_id', 'latitude', 'longitude', 'name', 'vicinity',
              'rating', 'user_ratings_total', 'community_district',
              'percent_food_insecurity', 'safegraph_placekey', 'safegraph_name')

    data_weekly_patterns = spark.read \
        .option('header', True) \
        .option('delimiter', ',') \
        .option('inferSchema', False) \
        .option('escape','"')\
        .csv(weekly_patterns) \
        .select('placekey', 'poi_cbg', 'visitor_home_cbgs', 'date_range_start','date_range_end')\
         .withColumn('date_range_start_flag',col('date_range_start').substr(0,7))\
         .withColumn('date_range_end_flag',col('date_range_end').substr(0,7))


    data_nyc_cbg_centroids = spark.read \
        .option('header', True) \
        .option('delimiter', ',') \
        .option('inferSchema', True) \
        .csv(nyc_cbg_centroids) \
        .toDF('cbg_fips', 'latitude', 'longitude')
    data_nyc_cbg_centroids.cache()

    # get all safegraph_placekey
    list_safegraph_placekey = data_nyc_supermarkets.select('safegraph_placekey') \
        .rdd \
        .map(lambda row: row[0]) \
        .collect()

    # 1.Use nyc_supermarkets.csv to filter the visits in the weekly patterns data
    data_weekly_patterns = data_weekly_patterns.rdd.filter(lambda row: row['placekey'] in list_safegraph_placekey)

    # 2. Only visit patterns with date_range_start or date_range_end overlaps with the 4
    # months of interests (Mar 2019, Oct 2019, Mar 2020, Oct 2020) will be considered,
    # i.e. either the start or the end date falls within the period.
    data_weekly_patterns = data_weekly_patterns.map(lambda row: [row['poi_cbg'],row['visitor_home_cbgs'],date_range_flag(row['date_range_start_flag'], row['date_range_end_flag'])] )\
        .filter(lambda x:x[2] != '')

    # 3. Use visitor_home_cbgs as the travel origins, and only consider CBG FIPS for NYC (must exist in nyc_cbg_centroids.csv ).
    list_nyc_cbg_centroids = data_nyc_cbg_centroids.select('cbg_fips') \
        .rdd \
        .map(lambda row: str(row[0])) \
        .collect()


    data_weekly_patterns = data_weekly_patterns.map(lambda row:((row[0],row[2]),parse_vistor_home_cbgs(row[1],list_nyc_cbg_centroids)))\
        .filter(lambda x:x[1] != None and len(x[1]) > 0 )
    '''
    4. Travel distances are the distances between each CBG in visitor_home_cbgs with
    poi_cbg . The distance must be computed in miles. To compute the distance
    between coordinates locally to NYC more accurately, please project them to the
    EPSG 2263 first.
    '''
    dict_cbg_latlong = data_nyc_cbg_centroids.rdd\
        .map(lambda row:(str(row['cbg_fips']),[row['latitude'],row['longitude']]))\
        .collectAsMap()



    # 1km * 0.621371 = 1 mile
    result = data_weekly_patterns.map(lambda x:distances(x,dict_cbg_latlong))\
        .reduceByKey(lambda x,y:[x[0]+y[0],x[1]+y[1]])\
        .map(lambda x: (x[0][0],[x[0][1],round(x[1][0] / x[1][1] * 0.621371,2)]))


    #format output
    #cbg_fips 2019-03 2019-10 2020-03 2020-10
    result = result.groupByKey()\
        .map(lambda group:list_distance(group)) \
        .repartition(1) \
        .sortBy(lambda x:x[0])

    result.saveAsTextFile(output)
    print('finished...')
