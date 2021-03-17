import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
from databricks import koalas as ks
import numpy as np
import matplotlib as mpl
from datetime import datetime


def menu() -> int:
    # Printing the options menu
    print("*************************************************************************")
    print("|   1:    How many events were created for each month/year?             |")
    print("|   2:    How do the number of events online compare to in-person?      |")
    print("|   3:    What is the trend of new tech vs. established tech meetings?  |")
    print("|   4:    What city hosted the most tech based events?                  |")
    print("|   5:    Which venue has hosted the most events?                       |")
    print("|   6:    Which state has the most venues?                              |")
    print("|   7:    What are some of the most common meetup topics?               |")
    print("|   8:    What extra topics are associated with tech meetups?           |")
    print("|   9:    What is the most popular time when events are created?        |")
    print("|  10:    Are events with longer durations more popular than shorter?   |")
    print("|  11:    Has there been a change in planning times for events?         |")
    print("|  12:    Which event has the most RSVPs?                               |")
    print("|  13:    How has event capacity changed over time?                     |")
    print("|  14:    What is the preferred payment method for events?              |")
    print("|  15:    How has the average cost of events changed over time?         |")
    print("|  16:    What are the largest tech groups by members?                  |")
    print("|  17:    Run all of the analyses                                       |")
    print("|   0:    Exit the analysis program                                     |")
    print("*************************************************************************\n")

    # Creating a repeating loop to get user input then report it back.
    repeat = True
    result = 0
    while repeat:
        repeat = False
        try:
            result = int(input("Enter your choice:  "))
        except ValueError:
            print("Something went wrong, please try again...")
            repeat = True

    return result


# Analysis runners that ingest the raw data and produce result output (graphs and lists)

# Q1:  How many events were created for each month/year?
def q1(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 1 started...")


# Q2:  How do the number of events online compare to in-person?
def q2(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 2 started...")


# Q3:  What is the trend of new tech vs. established tech meetings?
def q3(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 3 started...")


# Q4:  What city hosted the most tech based events?
def q4(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 4 started...")


# Q5:  Which venue has hosted the most events?
def q5(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 5 started...")


# Q6:  Which state has the most venues?
def q6(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 6 started...")


# Q7:  What are some of the most common meetup topics?
def q7(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 7 started...")


# Q8:  What extra topics are associated with tech meetups?
def q8(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 8 started...")


# This analysis finds the number of mentions of specific technologies invented pre-90's, in the 90's, and >90's
# and returns the data as well as some comparative figures based on that data.
# Q9:  What is the most popular time when events are created?
def q9(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 9 started...")

    # Aux function to find all mentions of a given technology in the descriptions of meetings spanning the given years.
    def tech_mentions(technology, year_start, year_end) -> list[int]:
        interim_1 = df.select('description', 'time').filter(df['description'].contains(technology))
        working_list: list[int] = []
        for year in range(year_start, year_end+1):
            print(f"\t\tcalculating for year {year}...")
            start_millis = (year - 1970) * 31536000000
            end_millis = (year + 1 - 1970) * 31536000000

            result = interim_1                          \
                .filter(start_millis <= df['time'])     \
                .filter(df['time'] <= end_millis)       \
                .count()
            working_list.append(result)

        return working_list

    # Dictionary containing all of the technologies and their era (<, >, =90's)
    tech_dict = {"ada": "<90's",        "android": ">90's",     "clojure": ">90's",     "cobol": "<90's",
                 "dart": ">90's",       "delphi": "=90's",      "fortran": "<90's",     "ios": ">90's",
                 "java": "=90's",       "javascript": "=90's",  "kotlin": ">90's",      "labview": "<90's",
                 "matlab": "<90's",     "pascal": "<90's",      "perl": "<90's",        "php": "90's",
                 "powershell": ">90's", "python": "=90's",      "ruby": "=90's",        "rust": ">90's",
                 "scala": ">90's",      "sql": "<90's",         "typescript": ">90's",  "visual basic ": "=90's",
                 "wolfram": "<90's"}

    # Creating the schema for the results DataFrame
    q9_schema = StructType([
        StructField('Technology', StringType(), True),
        StructField('Era',  StringType(),  True),
        StructField('2003', IntegerType(), False),
        StructField('2004', IntegerType(), False),
        StructField('2005', IntegerType(), False),
        StructField('2006', IntegerType(), False),
        StructField('2007', IntegerType(), False),
        StructField('2008', IntegerType(), False),
        StructField('2009', IntegerType(), False),
        StructField('2010', IntegerType(), False),
        StructField('2011', IntegerType(), False),
        StructField('2012', IntegerType(), False),
        StructField('2013', IntegerType(), False),
        StructField('2014', IntegerType(), False),
        StructField('2015', IntegerType(), False),
        StructField('2016', IntegerType(), False),
        StructField('2017', IntegerType(), False),
        StructField('2018', IntegerType(), False),
        StructField('2019', IntegerType(), False),
        StructField('2020', IntegerType(), False)
    ])

    q9_cols = ['Technology', 'Era', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010',
               '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']

    # Temporary holding pin for the results before being converted to a DataFrame
    q9_data = []
    # Looping through every technology listed in the tech_dictionary
    for tech, era in tech_dict.items():
        temp = [tech, era]
        print(f"Calculating yearly data for {tech}...")
        years = tech_mentions(tech, 2003, 2020)
        for year in years:
            temp.append(year)
        print(temp)
        q9_data += temp

    # Converting the data into an RDD then a Spark DataFrame
    q9_rdd = spark.sparkContext.parallelize(q9_data)
    q9_df = q9_rdd.toDF(q9_cols).orderBy('Era', 'Technology')

    # Printing for quality assurance
    print(q9_df.show(10))

    # Saving the result data to a save file
    q9_df.write.csv("output/q9_tech_mentions.csv", sep="\t", header="true")

    # Creating graphs from the produced data


# Q10:  Are events with longer durations more popular than shorter?
def q10(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 10 started...")


# Q11:  Has there been a change in planning times for events?
def q11(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 11 started...")


# Q12:  Which event has the most RSVPs?
def q12(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 12 started...")


# Q13:  How has event capacity changed over time?
def q13(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 13 started...")


# Q14:  What is the preferred payment method for events?
def q14(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 14 started...")


# Q15:  How has the average cost of events changed over time?
def q15(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 15 started...")


# Q16:  What are the largest tech groups by members?
def q16(spark: SparkSession, df: ps.sql.DataFrame):
    print("Analysis 16 started...")


def main():
    # Setting up the SparkSession for the analysis engine
    spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("MeetUp Trend Analysis") \
        .getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.sparkContext.setLogLevel("ERROR")

    # Create a list of timezone offsets
    time_offset_list = {
        "Addison, TX": -21600000,
        "Ames, IA": -21600000,
        "Akron, OH": -18000000,
        "Amherst, MA": -18000000,
        "Allen Park, MI": -18000000,
        "Albuquerque, NM": -25200000,
        "Anaheim, CA": -28800000,
        "Alexandria, VA": -18000000,
        "Agawam, MA": -18000000,
        "Allentown, PA": -18000000,
        "Agoura Hills, CA": -28800000,
        "Alpharetta, GA": -18000000,
        "Aliso Viejo, CA": -28800000,
        "Anchorage, AK": -32400000,
        "Aiea, HI": -36000000,
        "American Fork, UT": -25200000,
        "Appleton, WI": -21600000,
        "Arlington, TX": -21600000,
        "Aptos, CA": -28800000,
        "Arlington, VA": -18000000,
        "Arlington Heights, IL": -21600000,
        "Armonk, NY": -18000000,
        "Arnold, MO": -21600000,
        "Arlington, MA": -18000000,
        "Ann Arbor, MI": -18000000,
        "Annapolis Junction, MD": -18000000,
        "Apex, NC": -18000000,
        "Apollo Beach, FL": -18000000,
        "Ashburn, VA": -18000000,
        "Astoria, NY": -18000000,
        "Athens, GA": -18000000,
        "Athol, MA": -18000000,
        "Augusta, GA": -18000000,
        "Atlanta, GA": -18000000,
        "Aurora, CO": -25200000,
        "Auburn, AL": -21600000,
        "Austin, TX": -21600000,
        "Auburn, CA": -28800000,
        "Avon, CT": -18000000,
        "Auburn, WA": -28800000,
        "Basking Ridge, NJ": -18000000,
        "Babylon, NY": -18000000,
        "Baton Rouge, LA": -21600000,
        "Bakersfield, CA": -28800000,
        "Bay Shore, NY": -18000000,
        "Baltimore, MD": -18000000,
        "Bayside, NY": -18000000,
        "Bartlesville, OK": -21600000,
        "Beachwood, OH": -18000000,
        "Beaverton, OR": -28800000,
        "Bedford, TX": -21600000,
        "Bellevue, WA": -28800000,
        "Beverly Hills, CA": -28800000,
        "Birmingham, AL": -21600000,
        "Birmingham, MI": -18000000,
        "Bloomfield Hills, MI": -18000000,
        "Bethesda, MD": -18000000,
        "Bethlehem, PA": -18000000,
        "Bethpage, NY": -18000000,
        "Bettendorf, IA": -21600000,
        "Belmont, NC": -18000000,
        "Bend, OR": -28800000,
        "Berkeley, CA": -28800000,
        "Berthoud, CO": -25200000,
        "Bloomington, IL": -21600000,
        "Bloomington, IN": -18000000,
        "Blue Bell, PA": -18000000,
        "Boca Raton, FL": -18000000,
        "Boynton Beach, FL": -18000000,
        "Bradenton, FL": -18000000,
        "Brandon, FL": -18000000,
        "Brandon, MS": -21600000,
        "Boise, ID": -25200000,
        "Bonita Springs, FL": -18000000,
        "Boston, MA": -18000000,
        "Boulder, CO": -25200000,
        "Brecksville, OH": -18000000,
        "Brentwood, TN": -21600000,
        "Bridgeport, CT": -18000000,
        "Bridgewater, NJ": -18000000,
        "Brookfield, WI": -21600000,
        "Brooklet, GA": -18000000,
        "Brooklyn, NY": -18000000,
        "Broomfield, CO": -25200000,
        "Brighton, MI": -18000000,
        "Bristol, CT": -18000000,
        "Bronx, NY": -18000000,
        "Burlington, NC": -18000000,
        "Byron Center, MI": -18000000,
        "Calabasas, CA": -28800000,
        "Camarillo, CA": -28800000,
        "Buckhead, GA": -18000000,
        "Buffalo, NY": -18000000,
        "Burbank, CA": -28800000,
        "Burlington, MA": -18000000,
        "Carmel, IN": -18000000,
        "Carrboro, NC": -18000000,
        "Carrollton, TX": -21600000,
        "Cary, NC": -18000000,
        "Cambridge, MA": -18000000,
        "Camillus, NY": -18000000,
        "Campbell, CA": -28800000,
        "Canton, MI": -18000000,
        "Cedar Park, TX": -21600000,
        "Cedar Rapids, IA": -21600000,
        "Chagrin Falls, OH": -18000000,
        "Canton, OH": -18000000,
        "Cape Coral, FL": -18000000,
        "Capitola, CA": -28800000,
        "Chalfont, PA": -18000000,
        "Carlsbad, CA": -28800000,
        "Chatsworth, CA": -28800000,
        "Chattanooga, TN": -18000000,
        "Cherry Hill, NJ": -18000000,
        "Chesapeake, VA": -18000000,
        "Chicago, IL": -21600000,
        "Chico, CA": -28800000,
        "Chula Vista, CA": -28800000,
        "Cincinnati, OH": -18000000,
        "Chandler, AZ": -25200000,
        "Chapel Hill, NC": -18000000,
        "Charleston, SC": -18000000,
        "Charlotte, NC": -18000000,
        "College Station, TX": -21600000,
        "Colorado Springs, CO": -25200000,
        "Columbia, MD": -18000000,
        "Columbia, MO": -21600000,
        "Chester Springs, PA": -18000000,
        "Chesterfield, MO": -21600000,
        "Chesterfield, VA": -18000000,
        "Cheyenne, WY": -25200000,
        "Claremont, CA": -28800000,
        "Clarksville, TN": -21600000,
        "Clearwater, FL": -18000000,
        "Cleveland, OH": -18000000,
        "Columbia, SC": -18000000,
        "Columbus, GA": -18000000,
        "Columbus, OH": -18000000,
        "Commerce Township, MI": -18000000,
        "Cleveland, TN": -18000000,
        "Clinton Township, MI": -18000000,
        "Cocoa Beach, FL": -18000000,
        "Coeur D Alene, ID": -28800000,
        "Covington, KY": -18000000,
        "Cranbury, NJ": -18000000,
        "Culver City, CA": -28800000,
        "Cumming, GA": -18000000,
        "Cupertino, CA": -28800000,
        "Cuyahoga Falls, OH": -18000000,
        "Dacula, GA": -18000000,
        "Corona, CA": -28800000,
        "Corpus Christi, TX": -21600000,
        "Corvallis, OR": -28800000,
        "Costa Mesa, CA": -28800000,
        "Coppell, TX": -21600000,
        "Coraopolis, PA": -18000000,
        "Cornelius, NC": -18000000,
        "Corona del Mar, CA": -28800000,
        "Concord, CA": -28800000,
        "Concord, NC": -18000000,
        "Conroe, TX": -21600000,
        "Conyers, GA": -18000000,
        "De Pere, WI": -21600000,
        "Dearborn, MI": -18000000,
        "Decatur, GA": -18000000,
        "Deerfield Beach, FL": -18000000,
        "Dallas, TX": -21600000,
        "Danbury, CT": -18000000,
        "Del Mar, CA": -28800000,
        "Darien, CT": -18000000,
        "Delafield, WI": -21600000,
        "Deland, FL": -18000000,
        "Davenport, IA": -21600000,
        "Delray Beach, FL": -18000000,
        "Davidson, NC": -18000000,
        "Davis, CA": -28800000,
        "Dayton, OH": -18000000,
        "Daytona Beach, FL": -18000000,
        "Dunellen, NJ": -18000000,
        "Durham, NC": -18000000,
        "Eagle, ID": -25200000,
        "East Hartford, CT": -18000000,
        "Dublin, CA": -28800000,
        "Dublin, OH": -18000000,
        "Duluth, GA": -18000000,
        "Dunedin, FL": -18000000,
        "Edinburg, TX": -21600000,
        "Edison, NJ": -18000000,
        "Edmond, OK": -21600000,
        "El Dorado Hills, CA": -28800000,
        "Detroit, MI": -18000000,
        "Downers Grove, IL": -21600000,
        "Doylestown, PA": -18000000,
        "Draper, UT": -25200000,
        "Deltona, FL": -18000000,
        "Denton, TX": -21600000,
        "Denver, CO": -25200000,
        "Des Moines, IA": -21600000,
        "Elkin, NC": -18000000,
        "Elyria, OH": -18000000,
        "Emeryville, CA": -28800000,
        "Encinitas, CA": -28800000,
        "East Islip, NY": -18000000,
        "East Lansing, MI": -18000000,
        "East Troy, WI": -21600000,
        "Easton, PA": -18000000,
        "El Paso, TX": -21600000,
        "El Segundo, CA": -28800000,
        "Elizabethtown, KY": -18000000,
        "Elk Grove, CA": -28800000,
        "Encino, CA": -28800000,
        "Enfield, CT": -18000000,
        "Englewood, CO": -25200000,
        "Erlanger, KY": -18000000,
        "Escondido, CA": -28800000,
        "Estero, FL": -18000000,
        "Eugene, OR": -28800000,
        "Evanston, IL": -21600000,
        "Farmington, MI": -18000000,
        "Farmington, UT": -25200000,
        "Fayetteville, NC": -18000000,
        "Feasterville Trevose, PA": -18000000,
        "Ferndale, MI": -18000000,
        "Findlay, OH": -18000000,
        "Fishers, IN": -18000000,
        "Fleming Island, FL": -18000000,
        "Evansville, IN": -18000000,
        "Fairfax, VA": -18000000,
        "Everett, WA": -28800000,
        "Fairfield, CT": -18000000,
        "Exton, PA": -18000000,
        "Falls Church, VA": -18000000,
        "Fargo, ND": -21600000,
        "Fair Oaks, CA": -28800000,
        "Flemington, NJ": -18000000,
        "Flint, MI": -18000000,
        "Florence, OR": -28800000,
        "Fort Myers, FL": -18000000,
        "Fort Stewart, GA": -18000000,
        "Fort Wayne, IN": -18000000,
        "Fort Worth, TX": -21600000,
        "Framingham, MA": -18000000,
        "Franklin, TN": -21600000,
        "Frederick, MD": -18000000,
        "Fredericksburg, VA": -18000000,
        "Flower Mound, TX": -21600000,
        "Flushing, NY": -18000000,
        "Fogelsville, PA": -18000000,
        "Forest Hills, NY": -18000000,
        "Fremont, CA": -28800000,
        "Fremont, NE": -21600000,
        "Fresno, CA": -28800000,
        "Frisco, TX": -21600000,
        "Garden Grove, CA": -28800000,
        "Garland, TX": -21600000,
        "Geneva, IL": -21600000,
        "Georgetown, TX": -21600000,
        "Forest Park, IL": -21600000,
        "Fort Collins, CO": -25200000,
        "Fort Lauderdale, FL": -18000000,
        "Fort Mill, SC": -18000000,
        "Fullerton, CA": -28800000,
        "Gainesville, FL": -18000000,
        "Gainesville, GA": -18000000,
        "Garden City, NY": -18000000,
        "Goodlettsville, TN": -21600000,
        "Granada Hills, CA": -28800000,
        "Grand Rapids, MI": -18000000,
        "Granger, IN": -18000000,
        "Grapevine, TX": -21600000,
        "Grass Valley, CA": -28800000,
        "Greeley, CO": -25200000,
        "Green Bay, WI": -21600000,
        "Grimsby, ON": -18000000,
        "Glendale, AZ": -25200000,
        "Gurnee, IL": -21600000,
        "Glendale, CA": -28800000,
        "Hackensack, NJ": -18000000,
        "Glenshaw, PA": -18000000,
        "Hamilton, ON": -18000000,
        "Goleta, CA": -28800000,
        "Henderson, NV": -28800000,
        "Hendersonville, TN": -21600000,
        "Henrico, VA": -18000000,
        "Herndon, VA": -18000000,
        "Greenfield, MA": -18000000,
        "Greensboro, NC": -18000000,
        "Greenvale, NY": -18000000,
        "Gilbert, AZ": -25200000,
        "Glassboro, NJ": -18000000,
        "Greenwich, CT": -18000000,
        "Glastonbury, CT": -18000000,
        "Glen Allen, VA": -18000000,
        "Herriman, UT": -25200000,
        "Hialeah, FL": -18000000,
        "Hickory, NC": -18000000,
        "Hillsboro, OR": -28800000,
        "Huntington Beach, CA": -28800000,
        "Huntington, NY": -18000000,
        "Huntsville, AL": -21600000,
        "Incline Village, NV": -28800000,
        "Independence, OH": -18000000,
        "Independence, OR": -28800000,
        "Indianapolis, IN": -18000000,
        "Harrisville, RI": -18000000,
        "Hartford, CT": -18000000,
        "Hayward, CA": -28800000,
        "Hempstead, NY": -18000000,
        "Honolulu, HI": -36000000,
        "Hopkins, MN": -21600000,
        "Houston, TX": -21600000,
        "Hudson, OH": -18000000,
        "Issaquah, WA": -28800000,
        "Italy, TX": -21600000,
        "Ithaca, NY": -18000000,
        "Jackson, MI": -18000000,
        "Jackson, MS": -21600000,
        "Jacksonville Beach, FL": -18000000,
        "Jacksonville, FL": -18000000,
        "Janesville, WI": -21600000,
        "Kailua, HI": -36000000,
        "Kalamazoo, MI": -18000000,
        "Kansas City, KS": -21600000,
        "Kansas City, MO": -21600000,
        "Hoboken, NJ": -18000000,
        "Holly Springs, NC": -18000000,
        "Hollywood, FL": -18000000,
        "Holmdel, NJ": -18000000,
        "Jefferson City, TN": -18000000,
        "Jeffersonville, IN": -18000000,
        "Jericho, NY": -18000000,
        "Jupiter, FL": -18000000,
        "Iowa City, IA": -21600000,
        "Irvine, CA": -28800000,
        "Irving, TX": -21600000,
        "Iselin, NJ": -18000000,
        "Lafayette, CO": -25200000,
        "Lafayette, IN": -18000000,
        "Lafayette, LA": -21600000,
        "Laguna Niguel, CA": -28800000,
        "Knoxville, TN": -18000000,
        "Kokomo, IN": -18000000,
        "Kilgore, TX": -21600000,
        "La Jolla, CA": -28800000,
        "King of Prussia, PA": -18000000,
        "Kirkland, WA": -28800000,
        "Kissimmee, FL": -18000000,
        "La Mirada, CA": -28800000,
        "Lawrence Township, NJ": -18000000,
        "Lawrence, KS": -21600000,
        "Lawrenceville, GA": -18000000,
        "Lawton, OK": -21600000,
        "Layton, UT": -25200000,
        "Leawood, KS": -21600000,
        "Lehi, UT": -25200000,
        "Lenexa, KS": -21600000,
        "Lewis Center, OH": -18000000,
        "Lexington, KY": -18000000,
        "Liberty Lake, WA": -28800000,
        "Lilburn, GA": -18000000,
        "Lancaster, CA": -28800000,
        "Lansdale, PA": -18000000,
        "Lansing, MI": -18000000,
        "Las Vegas, NV": -28800000,
        "Lincoln, NE": -21600000,
        "Linthicum Heights, MD": -18000000,
        "Little Rock, AR": -21600000,
        "Littleton, CO": -25200000,
        "Kapolei, HI": -36000000,
        "Katy, TX": -21600000,
        "Kennesaw, GA": -18000000,
        "Kent, OH": -18000000,
        "Los Angeles, CA": -28800000,
        "Lake Forest, CA": -28800000,
        "Los Gatos, CA": -28800000,
        "Lake Oswego, OR": -28800000,
        "Lake Worth, FL": -18000000,
        "Louisville, CO": -25200000,
        "Long Beach, NY": -18000000,
        "Long Island City, NY": -18000000,
        "Longmont, CO": -25200000,
        "Louisville, KY": -18000000,
        "Lakeland, FL": -18000000,
        "Longwood, FL": -18000000,
        "Malvern, PA": -18000000,
        "Manchester, CT": -18000000,
        "Manchester, NH": -18000000,
        "Loveland, CO": -25200000,
        "Lowell, MA": -18000000,
        "Lubbock, TX": -21600000,
        "Mandeville, LA": -21600000,
        "Macon, GA": -18000000,
        "Madison Heights, MI": -18000000,
        "Madison, AL": -21600000,
        "Madison, WI": -21600000,
        "Manhattan Beach, CA": -28800000,
        "Maricopa, AZ": -25200000,
        "Marietta, GA": -18000000,
        "Marina del Rey, CA": -28800000,
        "Livermore, CA": -28800000,
        "Livonia, MI": -18000000,
        "Lodi, CA": -28800000,
        "Long Beach, CA": -28800000,
        "Melbourne, FL": -18000000,
        "Memphis, TN": -21600000,
        "Merced, CA": -28800000,
        "Meriden, CT": -18000000,
        "Miami, FL": -18000000,
        "Miamisburg, OH": -18000000,
        "Midland, TX": -21600000,
        "Midlothian, VA": -18000000,
        "Mc Lean, VA": -18000000,
        "McAllen, TX": -21600000,
        "McHenry, IL": -21600000,
        "McKinney, TX": -21600000,
        "Midvale, UT": -25200000,
        "Milford, CT": -18000000,
        "Milpitas, CA": -28800000,
        "Milwaukee, WI": -21600000,
        "Meridian, ID": -25200000,
        "Merritt Island, FL": -18000000,
        "Mesa, AZ": -25200000,
        "Miami Beach, FL": -18000000,
        "Monmouth Junction, NJ": -18000000,
        "Montclair, NJ": -18000000,
        "Marina, CA": -28800000,
        "Monterey Park, CA": -28800000,
        "Maryland Heights, MO": -21600000,
        "Maryville, TN": -18000000,
        "Monterey, CA": -28800000,
        "Mason, OH": -18000000,
        "Morristown, TN": -18000000,
        "Morrisville, NC": -18000000,
        "Mount Gilead, NC": -18000000,
        "Mount Juliet, TN": -21600000,
        "Montgomery, AL": -21600000,
        "Moorestown, NJ": -18000000,
        "Moorpark, CA": -28800000,
        "Morgan Hill, CA": -28800000,
        "Mineola, NY": -18000000,
        "Minneapolis, MN": -21600000,
        "Mishawaka, IN": -18000000,
        "Mission Viejo, CA": -28800000,
        "Mission, KS": -21600000,
        "Mobile, AL": -21600000,
        "Modesto, CA": -28800000,
        "Moline, IL": -21600000,
        "Murphy, ID": -25200000,
        "Murrieta, CA": -28800000,
        "Muscatine, IA": -21600000,
        "Muskegon, MI": -18000000,
        "Mount Pleasant, SC": -18000000,
        "Mountain View, CA": -28800000,
        "Muncie, IN": -18000000,
        "Murfreesboro, TN": -21600000 ,
        "Naperville, IL": -21600000,
        "Naples, FL": -18000000,
        "Neenah, WI": -21600000,
        "Nashville, TN": -21600000,
        "Neptune Beach, FL": -18000000,
        "Nevada City, CA": -28800000,
        "Nederland, TX": -21600000,
        "New Albany, OH": -18000000,
        "New Britain, CT": -18000000,
        "New Brunswick, NJ": -18000000,
        "New Haven, CT": -18000000,
        "New Hope, PA": -18000000,
        "New Orleans, LA": -21600000,
        "New Philadelphia, OH": -18000000,
        "New Port Richey, FL": -18000000,
        "New Rochelle, NY": -18000000,
        "Muskogee, OK": -21600000,
        "Nampa, ID": -25200000,
        "Nanuet, NY": -18000000,
        "Napa, CA": -28800000,
        "Newbury Park, CA": -28800000,
        "Newington, CT": -18000000,
        "Newport Beach, CA": -28800000,
        "Newport News, VA": -18000000,
        "Normal, IL": -21600000,
        "North Brunswick, NJ": -18000000,
        "North Canton, OH": -18000000,
        "North Charleston, SC": -18000000,
        "New Hyde Park, NY": -18000000,
        "New London, CT": -18000000,
        "New Milford, CT": -18000000,
        "New Smyrna Beach, FL": -18000000,
        "New York, NY": -18000000,
        "Newark, DE": -18000000,
        "Newark, NJ": -18000000,
        "North Hollywood, CA": -28800000,
        "North Little Rock, AR": -21600000,
        "North Miami Beach, FL": -18000000,
        "North Palm Beach, FL": -18000000,
        "North Richland Hills, TX": -21600000,
        "Northampton, MA": -18000000,
        "Northport, NY": -18000000,
        "Northridge, CA": -28800000,
        "Newton, MA": -18000000,
        "Noblesville, IN": -18000000,
        "Norcross, GA": -18000000,
        "Norfolk, VA": -18000000,
        "Oak Brook, IL": -21600000,
        "Oak Ridge, NC": -18000000,
        "Oakland, CA": -28800000,
        "Ocala, FL": -18000000,
        "Oceanside, CA": -28800000,
        "Offutt A F B, NE": -21600000,
        "Oklahoma City, OK": -21600000,
        "Ogden, UT": -25200000,
        "Olathe, KS": -21600000,
        "Old Westbury, NY": -18000000,
        "Okemos, MI": -18000000,
        "Olympia, WA": -28800000,
        "Omaha, NE": -21600000,
        "Ontario, CA": -28800000,
        "Orange Park, FL": -18000000,
        "Orange, CA": -28800000,
        "Orem, UT": -25200000,
        "Orlando, FL": -18000000,
        "Overland Park, KS": -21600000,
        "Oxford, OH": -18000000,
        "Norwalk, CT": -18000000,
        "Oviedo, FL": -18000000,
        "Oxnard, CA": -28800000,
        "Norwich, CT": -18000000,
        "Pacific Palisades, CA": -28800000,
        "Novi, MI": -18000000,
        "Palm Bay, FL": -18000000,
        "O Fallon, MO": -21600000,
        "Palmdale, CA": -28800000,
        "Palo Alto, CA": -28800000,
        "Parker, CO": -25200000,
        "Paramus, NJ": -18000000,
        "Pasadena, CA": -28800000,
        "Pawtucket, RI": -18000000,
        "Park City, UT": -25200000,
        "Pella, IA": -21600000,
        "Pembroke Pines, FL": -18000000,
        "Penfield, NY": -18000000,
        "Peoria, IL": -21600000,
        "Palm Beach Gardens, FL": -18000000,
        "Palm Beach, FL": -18000000,
        "Palm Harbor, FL": -18000000,
        "Perrysburg, OH": -18000000,
        "Palm Springs, CA": -28800000,
        "Pinellas Park, FL": -18000000,
        "Pittsburg, CA": -28800000,
        "Pleasant Grove, UT": -25200000,
        "Pleasant Ridge, MI": -18000000,
        "Pewaukee, WI": -21600000,
        "Pittsburgh, PA": -18000000,
        "Pleasanton, CA": -28800000,
        "Pflugerville, TX": -21600000,
        "Philadelphia, PA": -18000000,
        "Phoenix, AZ": -25200000,
        "Pomona, CA": -28800000,
        "Potomac, MD": -18000000,
        "Pottstown, PA": -18000000,
        "Poughkeepsie, NY": -18000000,
        "Poway, CA": -28800000,
        "Pompano Beach, FL": -18000000,
        "Ponte Vedra, FL": -18000000,
        "Pontiac, MI": -18000000,
        "Poquoson, VA": -18000000,
        "Princeton Junction, NJ": -18000000,
        "Princeton, NJ": -18000000,
        "Providence, RI": -18000000,
        "Provo, UT": -25200000,
        "Pueblo, CO": -25200000,
        "Queen Creek, AZ": -25200000,
        "Racine, WI": -21600000,
        "Raleigh, NC": -18000000,
        "Placentia, CA": -28800000,
        "Plainview, NY": -18000000,
        "Plano, TX": -21600000,
        "Playa del Rey, CA": -28800000,
        "Rancho Cordova, CA": -28800000,
        "Rancho Cucamonga, CA": -28800000,
        "Red Wing, MN": -21600000,
        "Redmond, WA": -28800000,
        "Redwood City, CA": -28800000,
        "Reno, NV": -28800000,
        "Redlands, CA": -28800000,
        "Renton, WA": -28800000,
        "Ridgeland, SC": -18000000,
        "Ridgewood, NJ": -18000000,
        "Riverside, CA": -28800000,
        "Rochester, MN": -21600000,
        "Rochester, NY": -18000000,
        "Rock Hill, SC": -18000000,
        "Rochester, MI": -18000000,
        "Rockford, IL": -21600000,
        "Port Saint Lucie, FL": -18000000,
        "Port Washington, NY": -18000000,
        "Portage, MI": -18000000,
        "Portland, OR": -28800000,
        "Rolling Meadows, IL": -21600000,
        "Roseville, CA": -28800000,
        "Roswell, GA": -18000000,
        "Rockledge, FL": -18000000,
        "Rocklin, CA": -28800000,
        "Rockville, MD": -18000000,
        "Round Rock, TX": -21600000,
        "Rocky Hill, CT": -18000000,
        "Royal Oak, MI": -18000000,
        "Ruther Glen, VA": -18000000,
        "Sacramento, CA": -28800000,
        "Saint Augustine, FL": -18000000,
        "Saint Charles, IL": -21600000,
        "Saint Charles, MO": -21600000,
        "Saint Louis, MO": -21600000,
        "Saint Petersburg, FL": -18000000,
        "Salem, OR": -28800000,
        "Salt Lake City, UT": -25200000,
        "Saint Paul, MN": -21600000,
        "Sammamish, WA": -28800000,
        "Reston, VA": -18000000,
        "Rialto, CA": -28800000,
        "Richardson, TX": -21600000,
        "Richmond, VA": -18000000,
        "San Jose, CA": -28800000,
        "San Juan Capistrano, CA": -28800000,
        "San Luis Obispo, CA": -28800000,
        "San Marcos, CA": -28800000,
        "Santa Ana, CA": -28800000,
        "Santa Barbara, CA": -28800000,
        "Santa Clara, CA": -28800000,
        "Santa Cruz, CA": -28800000,
        "Santa Fe, NM": -25200000,
        "Santa Maria, CA": -28800000,
        "Santa Monica, CA": -28800000,
        "Sarasota, FL": -18000000,
        "Saratoga, CA": -28800000,
        "Savannah, GA": -18000000,
        "Schaumburg, IL": -21600000,
        "San Mateo, CA": -28800000,
        "San Ramon, CA": -28800000,
        "Sandy, UT": -25200000,
        "Sanford, FL": -18000000,
        "Scotch Plains, NJ": -18000000,
        "Scottsdale, AZ": -25200000,
        "Seaside, CA": -28800000,
        "Seattle, WA": -28800000,
        "Seminole, FL": -18000000,
        "Shawnee, KS": -21600000,
        "Sheboygan, WI": -21600000,
        "Sherman Oaks, CA": -28800000,
        "Shreveport, LA": -21600000,
        "Sioux Falls, SD": -21600000,
        "Smyrna, GA": -18000000,
        "Snellville, GA": -18000000,
        "Solon, OH": -18000000,
        "Somerset, NJ": -18000000,
        "Somerville, MA": -18000000,
        "Somerville, NJ": -18000000,
        "Sonora, CA": -28800000,
        "South Amboy, NJ": -18000000,
        "South Bend, IN": -18000000,
        "South Jordan, UT": -25200000,
        "South Lake Tahoe, CA": -28800000,
        "South Ozone Park, NY": -18000000,
        "South Pasadena, CA": -28800000,
        "South San Francisco, CA": -28800000,
        "Southampton, NY": -18000000,
        "Southfield, MI": -18000000,
        "Southington, CT": -18000000,
        "Sparks, NV": -28800000,
        "San Antonio, TX": -21600000,
        "San Bernardino, CA": -28800000,
        "San Diego, CA": -28800000,
        "Spokane, WA": -28800000,
        "Spring, TX": -21600000,
        "Springfield, IL": -21600000,
        "San Francisco, CA": -28800000,
        "Springfield, MA": -18000000,
        "Springfield, MO": -21600000,
        "Springfield, OH": -18000000,
        "Springfield, OR": -28800000,
        "St Catharines, ON": -18000000,
        "Stamford, CT": -18000000,
        "Stanford, CA": -28800000,
        "Sterling Heights, MI": -18000000,
        "Sterling, VA": -18000000,
        "Stockton, CA": -28800000,
        "Stony Brook, NY": -18000000,
        "Strongsville, OH": -18000000,
        "Studio City, CA": -28800000,
        "Sturgeon Bay, WI": -21600000,
        "Sunnyvale, CA": -28800000,
        "Stuart, FL": -18000000,
        "Surprise, AZ": -25200000,
        "Tallahassee, FL": -18000000,
        "Tampa, FL": -18000000,
        "Tarrytown, NY": -18000000,
        "Temecula, CA": -28800000,
        "Tempe, AZ": -25200000,
        "Temple, TX": -21600000,
        "Tenafly, NJ": -18000000,
        "Texas City, TX": -21600000,
        "Tomball, TX": -21600000,
        "Topeka, KS": -21600000,
        "Torrance, CA": -28800000,
        "Tracy, CA": -28800000,
        "Suwanee, GA": -18000000,
        "Syosset, NY": -18000000,
        "Syracuse, NY": -18000000,
        "Tacoma, WA": -28800000,
        "Trenton, NJ": -18000000,
        "Troy, MI": -18000000,
        "Tucson, AZ": -25200000,
        "Tulsa, OK": -21600000,
        "Tuscaloosa, AL": -21600000,
        "Tustin, CA": -28800000,
        "Tyler, TX": -21600000,
        "Upland, CA": -28800000,
        "Utica, MI": -18000000,
        "Utica, NY": -18000000,
        "Vacaville, CA": -28800000,
        "Valencia, CA": -28800000,
        "Valparaiso, IN": -18000000,
        "Van Nuys, CA": -28800000,
        "Vancouver, WA": -28800000,
        "Thousand Oaks, CA": -28800000,
        "Tijuana, Mexico": -28800000,
        "Titusville, FL": -18000000,
        "Toledo, OH": -18000000,
        "Victorville, CA": -28800000,
        "Vienna, VA": -18000000,
        "Virginia Beach, VA": -18000000,
        "Visalia, CA": -28800000,
        "Vista, CA": -28800000,
        "Waco, TX": -21600000,
        "Walnut Creek, CA": -28800000,
        "Warner Robins, GA": -18000000,
        "Venice, CA": -28800000,
        "Ventura, CA": -28800000,
        "Vero Beach, FL": -18000000,
        "Vicksburg, MS": -21600000,
        "Warsaw, IN": -18000000,
        "Washington, DC": -18000000,
        "Watertown, CT": -18000000,
        "Watertown, WI": -21600000,
        "Watsonville, CA": -28800000,
        "Waukegan, IL": -21600000,
        "Waukesha, WI": -21600000,
        "Wayne, PA": -18000000,
        "Webster, NY": -18000000,
        "West Chester, OH": -18000000,
        "West Des Moines, IA": -21600000,
        "West Hartford, CT": -18000000,
        "West Haven, CT": -18000000,
        "West Hills, CA": -28800000,
        "West Hollywood, CA": -28800000,
        "West Point, NY": -18000000,
        "Westbury, NY": -18000000,
        "Westerville, OH": -18000000,
        "Westlake Village, CA": -28800000,
        "West Palm Beach, FL": -18000000,
        "Whittier, CA": -28800000,
        "Wichita, KS": -21600000,
        "Wilkes Barre, PA": -18000000,
        "Williamsburg, VA": -18000000,
        "Westlake, OH": -18000000,
        "Westminster, CO": -25200000,
        "Westwood, NJ": -18000000,
        "White Plains, NY": -18000000,
        "Woodbury, NY": -18000000,
        "Woodland Hills, CA": -28800000,
        "Woodside, NY": -18000000,
        "Woodstock, GA": -18000000,
        "Winston Salem, NC": -18000000,
        "Winter Garden, FL": -18000000,
        "Winter Park, FL": -18000000,
        "Woodbridge, NJ": -18000000,
        "Windermere, FL": -18000000,
        "Windsor, CO": -25200000,
        "Windsor, ON": -18000000,
        "Winnetka, CA": -28800000,
        "Willoughby, OH": -18000000,
        "Willow Grove, PA": -18000000,
        "Wilmington, DE": -18000000,
        "Wilmington, NC": -18000000,
        "Youngstown, OH": -18000000,
        "Ypsilanti, MI": -18000000,
        "Zionsville, IN": -18000000,
        "Woodstock, IL": -21600000,
        "Worcester, MA": -18000000,
        "Yonkers, NY": -18000000,
        "Yorba Linda, CA": -28800000
    }

    # Creating DataFrame UDFs to properly type each column
    def date_parser(date_string: str):
        return datetime.strptime(date_string, "%Y-%m-%d")

    date_parser_udf = F.udf(date_parser, DateType())

    def tz_parser(city: str):
        return time_offset_list[city]

    tz_parser_udf = F.udf(tz_parser, LongType())

    # def cat_ids_parser(cat_string: str):
    #     hold: list[int] = []
    #     if len(cat_string) == 0:
    #         return hold
    #
    #     ids = cat_string.replace(" ", "").split(',')
    #     for i in ids:
    #         hold.append(int(i))
    #     return hold
    #
    # cat_parser_udf = F.udf(cat_ids_parser, ArrayType(IntegerType()))

    def status_parser(status: str) -> bool:
        if status == "past":
            return True
        else:
            return False

    status_parser_udf = F.udf(status_parser, BooleanType())

    # Creating a list of column names for the resulting DataFrame for reference
    # df_cols = ["id", "name", "group_name", "urlname", "v_id", "v_name", "local_date", "date_month", "local_time",
    #            "localized_location", "is_online_event", "status", "cat_ids", "duration", "time", "created",
    #            "yes_rsvp_count", "rsvp_limit", "accepts", "amount", "description"]
    base_df = spark.read.load('all_cities.tsv', format='csv', header='True', sep='\t', inferSchema="true")

    print(base_df.show(10))
    print("\n\n")

    base_df.printSchema()

    # Modifying the column data types to match data
    base_df = base_df.withColumn('id', F.col('id').cast(LongType()))                             \
                     .withColumn('v_id', F.col('v_id').cast(LongType()))                         \
                     .withColumn('local_date', date_parser_udf('local_date'))                       \
                     .withColumn('time_offset', tz_parser_udf('localized_location'))                \
                     .withColumn('adj_creation_time', F.col('created') + F.col('time_offset'))      \
                     .withColumn('past', status_parser_udf('status'))                               \
                     .withColumn('is_online_event', F.col('is_online_event').cast(BooleanType()))
    # .withColumn('cat_ids', cat_parser_udf('cat_ids'))

    new_cols_list = ["time_offset", "adj_creation_time", "past"]

    base_df.printSchema()

    print(base_df.show(10))

    while True:
        user_choice = menu()
        if user_choice == 1:
            q1(spark, base_df)
        elif user_choice == 2:
            q2(spark, base_df)
        elif user_choice == 3:
            q3(spark, base_df)
        elif user_choice == 4:
            q4(spark, base_df)
        elif user_choice == 5:
            q5(spark, base_df)
        elif user_choice == 6:
            q6(spark, base_df)
        elif user_choice == 7:
            q7(spark, base_df)
        elif user_choice == 8:
            q8(spark, base_df)
        elif user_choice == 9:
            q9(spark, base_df)
        elif user_choice == 10:
            q10(spark, base_df)
        elif user_choice == 11:
            q11(spark, base_df)
        elif user_choice == 12:
            q12(spark, base_df)
        elif user_choice == 13:
            q13(spark, base_df)
        elif user_choice == 14:
            q14(spark, base_df)
        elif user_choice == 15:
            q15(spark, base_df)
        elif user_choice == 16:
            q16(spark, base_df)
        elif user_choice == 17:
            q1(spark, base_df)
            q2(spark, base_df)
            q3(spark, base_df)
            q4(spark, base_df)
            q5(spark, base_df)
            q6(spark, base_df)
            q7(spark, base_df)
            q8(spark, base_df)
            q9(spark, base_df)
            q10(spark, base_df)
            q11(spark, base_df)
            q12(spark, base_df)
            q13(spark, base_df)
            q14(spark, base_df)
            q15(spark, base_df)
            q16(spark, base_df)
        elif user_choice == 0:
            break
        else:
            print("Invalid option, please choose from the available options.")


if __name__ == "__main__":
    main()
