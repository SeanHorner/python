# The common aliases used for Pandas, NumPy, and MatPlotLib are pd, np, and plt, respectively.
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# Spacer function to put some space between different queries.
def spacer():
    print("\n\n")


# Read the CSV file into pandas to create a dataframe.
# There are many read functions in pandas, including:
# read_json(), read_html(), read_sql_table()
nba = pd.read_csv("nba_all_elo.csv")
# Shows that nba is indeed a dataframe.
print(type(nba))
#     <class 'pandas.core.frame.DataFrame'>

# The length of nba counts the number of rows in the dataframe.
print(len(nba))
#     126314
# The shape function gives the dimensions of the dataframe, i.e. rows and columns.
print(nba.shape)
#     (126314, 23)


# The head() function prints the first 5 rows of the dataframe.
print(nba.head())
#        gameorder          game_id      lg_id   ...  game_result  forecast   notes
#     0          1     194611010TRH        NBA   ...            L  0.640065     NaN
#     1          1     194611010TRH        NBA   ...            W  0.359935     NaN
#     2          2     194611020CHS        NBA   ...            W  0.631101     NaN
#     3          2     194611020CHS        NBA   ...            L  0.368899     NaN
#     4          3     194611020DTF        NBA   ...            L  0.640065     NaN
#
#     [5 rows x 23 columns]


# The set_option() function is used to change various pandas options.
# This option, display.precision, determines the number of decimal spaces to display.
pd.set_option("display.precision", 2)

# The tail() function prints the last 5 rows of the dataframe by default, but if given a
# value it will print that many rows from the end of the df (in this example 3).
print(nba.tail(3))
#             gameorder       game_id lg_id  ...  game_result  forecast notes
#     126311      63156  201506140GSW   NBA  ...            L      0.23   NaN
#     126312      63157  201506170CLE   NBA  ...            L      0.48   NaN
#     126313      63157  201506170CLE   NBA  ...            W      0.52   NaN
#
#     [3 rows x 23 columns]


# The info() function prints out information about the df such as: number of entries,
# number of columns, column data for each column (name, entries, nullability, datatype),
# count of each datatype used, and memory used by the df.
print(nba.info())
#     <class 'pandas.core.frame.DataFrame'>
#     RangeIndex: 126314 entries, 0 to 126313
#     Data columns (total 23 columns):
#      #   Column         Non-Null Count   Dtype
#     ---  ------         --------------   -----
#      0   gameorder      126314 non-null  int64
#      1   game_id        126314 non-null  object
#      2   lg_id          126314 non-null  object
#      3   _iscopy        126314 non-null  int64
#      4   year_id        126314 non-null  int64
#      5   date_game      126314 non-null  object
#      6   seasongame     126314 non-null  int64
#      7   is_playoffs    126314 non-null  int64
#      8   team_id        126314 non-null  object
#      9   fran_id        126314 non-null  object
#      10  pts            126314 non-null  int64
#      11  elo_i          126314 non-null  float64
#      12  elo_n          126314 non-null  float64
#      13  win_equiv      126314 non-null  float64
#      14  opp_id         126314 non-null  object
#      15  opp_fran       126314 non-null  object
#      16  opp_pts        126314 non-null  int64
#      17  opp_elo_i      126314 non-null  float64
#      18  opp_elo_n      126314 non-null  float64
#      19  game_location  126314 non-null  object
#      20  game_result    126314 non-null  object
#      21  forecast       126314 non-null  float64
#      22  notes          5424 non-null    object
#     dtypes: float64(6), int64(7), object(10)
#     memory usage: 22.2+ MB


# The describe() function provides some very interesting data about the df. It gives various
# metrics like: count, mean value, standard deviation, minimum value, 25% value, 50% value,
# 75% value, and maximum value. It gives these values only for numerical columns.
print(nba.describe())
#            gameorder   _iscopy    year_id  ...  opp_elo_i  opp_elo_n   forecast
#     count  126314.00  126314.0  126314.00  ...  126314.00  126314.00  126314.00
#     mean    31579.00       0.5    1988.20  ...    1495.24    1495.24       0.50
#     std     18231.93       0.5      17.58  ...     112.14     112.46       0.22
#     min         1.00       0.0    1947.00  ...    1091.64    1085.77       0.02
#     25%     15790.00       0.0    1975.00  ...    1417.24    1416.99       0.33
#     50%     31579.00       0.5    1990.00  ...    1500.95    1500.95       0.50
#     75%     47368.00       1.0    2003.00  ...    1576.06    1576.29       0.67
#     max     63157.00       1.0    2015.00  ...    1853.10    1853.10       0.98
#
#     [8 rows x 13 columns]
# The include keyword adds options to the describe() function. Here it's used to filter to
# only the object datatype from the numpy package, essentially columns of strings.
print(nba.describe(include=np.object))
#                  game_id   lg_id  ... game_result           notes
#     count         126314  126314  ...      126314            5424
#     unique         63157       2  ...           2             231
#     top     197011240DET     NBA  ...           W  at New York NY
#     freq               2  118016  ...       63157             440
#
#     [4 rows x 10 columns]


# The value_counts() function counts up the occurrences of each distinct value in the column,
# for example, in this query it will count up how many times each team ID occurs in the df.
print(nba["team_id"].value_counts())
#     BOS    5997
#     NYK    5769
#     LAL    5078
#     DET    4985
#     PHI    4533
#           ...
#     PIT      60
#     DTF      60
#     INJ      60
#     TRH      60
#     SDS      11
#     Name: team_id, Length: 104, dtype: int64

# The next query counts up the occurrences of different franchises in the df.
print(nba["fran_id"].value_counts())
#     Lakers          6024
#     Celtics         5997
#     Knicks          5769
#     Warriors        5657
#     Pistons         5650
#     Sixers          5644
#     Hawks           5572
#     Kings           5475
#     Wizards         4582
#     Spurs           4309
#       ...
#     Name: fran_id, dtype: int64

# Running these queries something interesting surfaces: the Lakers franchise occurs 6024 times
# in the df, but the team ID LAL (Los Angeles Lakers) only appears 5078 times. To figure out
# what's happening another query can be run, this time using the data selection function loc,
# short for locate.
print(nba.loc[nba["fran_id"] == "Lakers", "team_id"].value_counts())
#     LAL    5078
#     MNL     946
#     Name: team_id, dtype: int64

# That query shows us that two teams share the franchise ID Lakers, the LA Lakers and the
# Minneapolis Lakers. Never heard of the Minneapolis Lakers? This next query will shows when
# their games were played. First, their most recent game:
print("MNL latest game: " + nba.loc[nba["team_id"] == "MNL", "date_game"].max())
#     MNL latest game: 4/9/1959
# Then their earliest game in the data set:
print("MNL earliest game: " + nba.loc[nba["team_id"] == "MNL", "date_game"].min() + "\n")
#     MNL earliest game: 1/1/1949
# Since both queries are looking at the same initial data and just getting the min and max, both
# can be run simultaneously, like so:
print("MNL Earliest and Latest Game:")
print(nba.loc[nba["team_id"] == "MNL", "date_game"].agg(("min", "max")))
#     MNL Earliest and Latest Game:
#     min    1/1/1949
#     max    4/9/1959
#     Name: date_game, dtype: object

# These queries show that the Minneapolis Lakers played between 1949 and 1959, making them not
# very well-known to current basketball fans.


# The next goal is to determine the total number of points scored by the Boston Celtics, the
# team with the most games in the data set, across the entire data set.
print("Total BOS points scored: ")
print(nba.loc[nba["team_id"] == "BOS", "pts"].sum())
#     Total BOS points scored:
#     626484


# The following command builds the most basic form of a pandas dataframe, the Series() function
# which turns a Python list into a dataframe.
revenues = pd.Series([5555, 7000, 1980])
print(revenues)
#     0     5555
#     1     7000
#     2     1980
# Series are composed of two components: a sequence of values and a sequence of identifiers, aka
# the index, which can be accessed like so:
print(revenues.values)
#     array([5555, 7000, 1980])
print(revenues.index)
#     RangeIndex(start=0, stop=3, step=1)

# As an interesting side-note, the values in a Series object are a NumPy n-dimensional array.
print(type(revenues.values))
#     <class 'numpy.ndarray'>

# It is also possible to replace the numeric index values with strings or any other data type:
city_revenues = pd.Series(
    [4200, 8000, 6500],
    index=["Amsterdam", "Toronto", "Tokyo"]
)
print(city_revenues)
#     Amsterdam     4200
#     Toronto       8000
#     Tokyo         6500
#     dtype: int64

# Indexed this way dataframes are very much like dictionaries. In fact, a dictionary can be
# passed into pandas to create the same effect:
city_employee_count = pd.Series({"Amsterdam": 5, "Tokyo": 8})
print(city_employee_count)
#     Amsterdam     5
#     Tokyo         8
#     dtype: int64
# The dictionary's keys become the index for the Series values. The .keys() method and the in
# keyword can be used to check for values in the index:
print(city_employee_count.keys())
#     Index(['Amsterdam', 'Tokyo'], dtype='object')
print("Tokyo" in city_employee_count)
#     True
print("New York" in city_employee_count)
#     False

# Series objects can only hold one attribute per key, so to create larger, more complex data sets
# Series can be combined together to make DataFrames, which are akin to SQL tables.
# This is done by using the DataFrame method:
city_data = pd.DataFrame({
    "revenue": city_revenues,
    "employee_count": city_employee_count
})
print(city_data)
#                revenue    employee_count
#     Amsterdam     4200               5.0
#     Tokyo         6500               8.0
#     Toronto       8000               NaN
# Notice that since Toronto doesn't appear in the city_employee_count Series, it's value in that
# column is NaN for "Not a Number", the generic null value for Pandas DataFrames.
# The DataFrame's index is a union of the two Series' indices, that is any value that shows up
# in at least one of the Series as an index value will show up in the DataFrame.

# The two dimensions of the DataFrame can be accessed using the axes property of the DataFrame:
print(city_data.axes)
#     [Index(['Amsterdam', 'Tokyo', 'Toronto'], dtype='object'),
#      Index(['revenue', 'employee_count'], dtype='object')]
print(city_data.axes[0])
#     Index(['Amsterdam', 'Tokyo', 'Toronto'], dtype='object')
print(city_data.axes[1])
#     Index(['Amsterdam', 'Tokyo', 'Toronto'], dtype='object')
# The first axis, axes[0], corresponds to the index of the DataFrame, here the cities, and the
# second axis, axes[1], corresponds to the column index, here the data categories.

# DataFrames also act similarly to dictionaries, so they support the .keys() method and the in
# keyword, but for DataFrames these relate to the columns, not the index. For example:
print("Amsterdam" in city_data)
#     False
print("revenue" in city_data)
#     True
# Trying this with the nba dataset:
print(nba.index)
print(nba.axes[0])
print(nba.axes[1])

# Series data can be accessed by both the row's positional index number, the implicit index,
# or it's label, the explicit index, like so:
print(city_revenues["Toronto"])
#     8000
print(city_revenues[1])
#     8000
# It is also possible to use negative indices and slices, just like with lists:
print(city_revenues[-1])
#     6500
print(city_revenues[1:])
#     Toronto    8000
#     Tokyo      6500
#     dtype: int64
print(city_revenues["Toronto":])
#     Toronto    8000
#     Tokyo      6500
#     dtype: int64

# A problem can arise when the label indices are also numbers, for example:
colors = pd.Series(
    ["red", "purple", "blue", "green", "yellow"],
    index=[1, 2, 3, 5, 8]
)
print(colors)
#     1       red
#     2    purple
#     3      blue
#     5     green
#     8    yellow
#     dtype: object
# To avoid confusion in such situations, Pandas provides two data access methods:
# 1: .loc[]     which refers to the label index
# 2: .iloc[]    which refers to the positional index
# Using these two methods makes data access much more intelligible:
print(colors.loc[1])
#     'red'
print(colors.iloc[1])
#     'purple'
# In production code it is preferable to use .loc[] and .iloc[] over the regular indexing
# operator, both for readability and because there is a performance drawback.
# One more important thing to note is that the .iloc[] method will exclude the closing
# element in a slice, but the .loc[] method will include it. For example:
print(colors.iloc[1:3])
#     2    purple
#     3      blue
#     dtype: object
print(colors.loc[3:8])
#     3      blue
#     5     green
#     8    yellow
#     dtype: object
# In this way, using the .iloc[] method on a series is similar to using [] on a list, while
# .loc[] is similar to using [] on a dictionary.

# Accessing data inside a DataFrame is fairly similar. Each column can be thought of as a Series.
# Columns can be accessed both by using the normal indexing operator with the column name passed as
# a string as well as using member access notation (the . operator) with the column name:
print(city_data["revenue"])
#     Amsterdam    4200
#     Tokyo        6500
#     Toronto      8000
#     Name: revenue, dtype: int64
print(city_data.revenue)
#     Amsterdam    4200
#     Tokyo        6500
#     Toronto      8000
#     Name: revenue, dtype: int64
# The two methods produce equivalent outputs. The only time that dot notation might fail is when a
# column name coincides with a Pandas DataFrame attribute, for example:
toys = pd.DataFrame([
    {"name": "ball", "shape": "sphere"},
    {"name": "Rubik's cube", "shape": "cube"}
    ])
print(toys["shape"])
#     0    sphere
#     1      cube
#     Name: shape, dtype: object
print(toys.shape)
#     (2, 2)

# DataFrames also provide the .loc[] and .iloc[] data access methods. For example:
print(city_data.loc["Amsterdam"])
#     revenue           4200.0
#     employee_count       5.0
#     Name: Amsterdam, dtype: float64
print(city_data.loc["Tokyo": "Toronto"])
#             revenue employee_count
#     Tokyo   6500    8.0
#     Toronto 8000    NaN
print(city_data.iloc[1])
#     revenue           6500.0
#     employee_count       8.0
#     Name: Tokyo, dtype: float64
# Here, using .loc[] with a single city name gives the data for that city as a Series, using .loc[]
# with a range of city names gives data from all the cities in the slice (inclusive) in a DataFrame,
# and using .iloc[] with an index number returns the values for the city at that position.

# Practicing this knowledge on the nba dataset to get the second to last row:
print(nba.iloc[-2])
#     gameorder               63157
#     game_id          201506170CLE
#     lg_id                     NBA
#     _iscopy                     0
#     year_id                  2015
#     date_game           6/16/2015
#     seasongame                102
#     is_playoffs                 1
#     team_id                   CLE
#     fran_id             Cavaliers
#     pts                        97
#     elo_i                 1.7e+03
#     elo_n                 1.7e+03
#     win_equiv                  59
#     opp_id                    GSW
#     opp_fran             Warriors
#     opp_pts                   105
#     opp_elo_i             1.8e+03
#     opp_elo_n             1.8e+03
#     game_location               H
#     game_result                 L
#     forecast                 0.48
#     notes                     NaN
#     Name: 126312, dtype: object

# The .loc[] data access method also has another quirk when used on a DataFrame, it can be passed a
# second parameter, specifically a list of the columns desired from the DataFrame to create a subset:
print(city_data.loc["Amsterdam": "Tokyo", "revenue"])
#     Amsterdam    4200
#     Tokyo        6500
#     Name: revenue, dtype: int64
# Using the larger nba dataset for illustration of multiple rows:
print(nba.loc[5555:5559, ["fran_id", "opp_fran", "pts", "opp_pts"]])
#           fran_id  opp_fran  pts  opp_pts
#     5555  Pistons  Warriors   83       56
#     5556  Celtics    Knicks   95       74
#     5557   Knicks   Celtics   74       95
#     5558    Kings    Sixers   81       86
#     5559   Sixers     Kings   86       81

# The next step in understanding a dataset is being able to make queries. Often queries are used to
# create new, smaller DataFrames of selected data, like so:
current_decade = nba[nba["year_id"] > 2010]
print(current_decade.shape)
#     (12658, 23)
# This smaller DataFrame now contains all of the game data only for games played in this decade.
# It is also possible to use the .notnull() method to return only rows where a certain field is not null:
games_with_notes = nba[nba["notes"].notnull()]
print(games_with_notes.shape)
#     (5424, 23)
# Note: the same result can be achieved by using the .notna() method, it also returns any non-null values.

# Another useful method in Pandas is the .str access operator which allows results to be treated as
# strings, i.e. apply string methods to applicable fields in a DataFrame. For example:
ers_teams = nba[nba["fran_id"].str.endswith("ers")]
print(ers_teams.shape)
#     (27797, 23)
# The above assignment creates a new DataFrame of all games where the home teams name ends in "ers".

# It's possible to combine or chain logical expressions when filtering a DataFrame, using a single pipe
# ( | ) for "or" and a single ampersand ( & ) for "and", and giving each condition its own enclosing
# parentheses. The following filter finds games played by Baltimore where both teams scored over 100
# points. Notice the first condition ensures there are no duplicates in the filtered set.
high_score_baltimore = nba[
    (nba["_iscopy"] == 0) &
    (nba["pts"] > 100) &
    (nba["opp_pts"] > 100) &
    (nba["team_id"] == "BLB")
    ]
print(high_score_baltimore.shape)
#     (5, 23)

# Another challenge would be to find the two games played in 1992 in which both of Los Angeles' teams
# had to play home games at another court.
la_home_away_play = nba[
    (nba["_iscopy"] == 0) &
    (nba["year_id"] == 1992) &
    (nba["fran_id"].str.startswith("LA")) &
    (nba["notes"].notnull())
]

# Another important set of methods when dealing with dataframes is the aggregate functions, that is
# processes like summing or averaging a column, or finding the maximum or minimum value in a column.
# Any method that can be used on a series in Pandas can be used on a column since they are, effectively
# just a set of series. Some examples:
print(nba["pts"].sum())
#     12976235
print(nba["win_equiv"].mean())
#     41.70788868917144

# Because DataFrames have multiple columns its also possible to use grouping to aggregate a column
# together by combining like results in another column. For example:
print(nba.groupby("fran_id", sort=False)["pts"].sum())
#     fran_id
#     Huskies           3995
#     Knicks          582497
#     Stags            20398
#     Falcons           3797
#     Capitols         22387
#     ...
# Note: By default Pandas sorts by the given key when using .groupby(). Passing the sort=False flag
# stops the sorting from happening.

# Combining multiple filter conditions and grouping by column can give some very interesting and
# useful results. For example:
recent_spurs_win_loss = nba[
    (nba["fran_id"] == "Spurs") &
    (nba["year_id"] > 2010)
].groupby(["year_id", "game_result"])["game_id"].count()
print(recent_spurs_win_loss)
#     2011     L              25
#              W              63
#     2012     L              20
#              W              60
#     2013     L              30
#              W              73
#     2014     L              27
#              W              78
#     2015     L              31
#              W              58
#     Name: game_id, dtype: int64

# More practice would be to find the Golden State Warriors' 2014-2015 season win-loss record divided
# between the regular and post-season. Like so:
gsw_2015_wl = nba[
    (nba["fran_id"] == "Warriors") &
    (nba["year_id"] == 2015)
].groupby(["is_playoffs", "game_result"])["game_id"].count()
print(gsw_2015_wl)
#     is_playoffs  game_result
#     0            L              15
#                  W              67
#     1            L               5
#                  W              16
#     Name: game_id, dtype: int64

# Column manipulation is another important aspect of using Pandas. Columns can be added to or dropped
# from a DataFrame to either clarify or clean data, as needed.
# Note: this step creates a copy of the nba DataFrame so as to leave nba unaltered.
df = nba.copy()
print(df.shape)
#     (126314, 23)
#  New columns can be defined like so:
df["difference"] = df.pts - df.opp_pts
print(df.shape)
#     (126314, 24)
# This creates a new column named "difference" which measures the difference between the home and opposing
# teams points. Notice that the column count for the DataFrame is now one more than previously.
# This column could prove useful for determining interesting stats. For example, the largest gap between
# team scores:
print(df["difference"].max())
#     68              ## (What a blowout!)

# It's also possible to rename columns in a dataframe. For example, the game_result and game_location
# column names seem pretty verbose. They can be renamed like so:
# Note: here the DataFrame is assigned to a new DataFrame, but this is just for convenience sake. To
# modify the current DataFrame it is possible to just pass it's name as the assignment or use the rename()
# method's inplace parameter set to True (e.g. rename(..., inplace=True)).
renamed_df = df.rename(
    columns={"game_result": "result", "game_location": "location"}
)
#     print(renamed_df.info())
#     <class 'pandas.core.frame.DataFrame'>
#     RangeIndex: 126314 entries, 0 to 126313
#     Data columns (total 24 columns):
#     gameorder      126314 non-null int64
#     ...
#     location       126314 non-null object
#     result         126314 non-null object
#     forecast       126314 non-null float64
#     notes          5424 non-null object
#     difference     126314 non-null int64
#     dtypes: float64(6), int64(8), object(10)
#     memory usage: 23.1+ MB

# Removing columns can be useful for cleaning up DataFrames and removing any unnecessary or
# cumbersome data. This is done by utilizing the drop() method parameterized with a single column name
# or, if multiple columns are to be removed, an array of column names, along with which axis they are
# on (drop can also be used with rows). The following example will also use the inplace flag to modify
# the DataFrame directly.
print(df.shape)
#     (126314, 24)
elo_columns = ["elo_i", "elo_n", "opp_elo_i", "opp_elo_n"]
df.drop(elo_columns, inplace=True, axis=1)
print(df.shape)
#     (126314, 20)

# While Pandas will automatically choose which data type it thinks is best whenever a Series or DataFrame
# is created or read in from a file. Usually it makes fine assumptions, but it can often help code to be
# faster an more efficient by giving the specific data types for data in the data set. For example, in
# the nba (and therefore df) DataFrames, the date_game column is considered a string (object, in Pandas)
# but Pandas has a good data type for dates, the datetime data type, and it's possible to convert the
# date_game column to this datatype by using the to_datetime() method, like so:
df["date_game"] = pd.to_datetime(df["date_game"])
# Another column that is a candidate for type fixing is the game_location column, which only has 3 possible
# values. This can be checked like so:
print(df["game_location"].nunique())
#     3
print(df["game_location"].value_counts())
#     A     63138
#     H     63138
#     N     38
#     Name: game_location, dtype: int64

# For a single character with very few options to choose from Pandas provides the categorical data type,
# which is similar to the enum type in relational databases. The column can be converted like so:
df["game_location"] = pd.Categorical(df["game_location"])
print(df["game_location"].dtype)
#     category

# Running df.info() again it's possible to see that the data type for these columns has changed. These new
# types help save on memory, as can be seen by the lower DataFrame memory usage: 18.4 MB vs 23.1
print(df.info())
#     <class 'pandas.core.frame.DataFrame'>
#     RangeIndex: 126314 entries, 0 to 126313
#     Data columns (total 20 columns):
#      #   Column         Non-Null Count   Dtype
#     ---  ------         --------------   -----
#      0   gameorder      126314 non-null  int64
#      1   game_id        126314 non-null  object
#      2   lg_id          126314 non-null  object
#      3   _iscopy        126314 non-null  int64
#      4   year_id        126314 non-null  int64
#      5   date_game      126314 non-null  datetime64[ns]
#      6   seasongame     126314 non-null  int64
#      7   is_playoffs    126314 non-null  int64
#      8   team_id        126314 non-null  object
#      9   fran_id        126314 non-null  object
#      10  pts            126314 non-null  int64
#      11  win_equiv      126314 non-null  float64
#      12  opp_id         126314 non-null  object
#      13  opp_fran       126314 non-null  object
#      14  opp_pts        126314 non-null  int64
#      15  game_location  126314 non-null  category
#      16  game_result    126314 non-null  object
#      17  forecast       126314 non-null  float64
#      18  notes          5424 non-null    object
#      19  difference     126314 non-null  int64
#     dtypes: category(1), datetime64[ns](1), float64(2), int64(8), object(8)
#     memory usage: 18.4+ MB

# Another column that could be re-typed is the game_result column, which only has two possible values,
# W or L, so it can be re-typed to categorical as well:
print(df["game_result"].nunique())
#    2
print(df["game_result"].value_counts())
#     W    63157
#     L    63157
#     Name: game_result, dtype: int64
df["game_result"] = pd.Categorical(df["game_result"])
print(df["game_result"].dtype)
#     category
print(df.info())
#     ...
#     dtypes: category(2), datetime64[ns](1), float64(2), int64(8), object(7)
#     memory usage: 17.6+ MB

# There are other data pruning techniques that are useful when dealing with large datasets. For example,
# removing rows that contain null values, since they represent incomplete data. In the nba dataset, most
# null values come from the notes column (in fact only 4.3% of the rows have a non-null value for
# comment). To remove all rows from the DataFrame that contain a null value, the dropna() method is used:
rows_without_missing_data = nba.dropna()
print(rows_without_missing_data.shape)
#     (5424, 23)
# It is also possible to use the dropna() method to remove problematic columns (i.e. ones with null
# values) from the data set by passing the axis=1 parameter:
data_without_missing_columns = nba.dropna(axis=1)
print(data_without_missing_columns.shape)
#     (126314, 22)

# It is also possible to replace null values with a meaningful default value by using the fillna() method.
# This method takes a parameter, value, and copies it in place of every null in the provided column. To
# show that this method also has an inplace flag (and to not mess up the nba DataFrame) the following
# example will copy nba to a new DataFrame to modify:
data_with_default_notes = nba.copy()
data_with_default_notes["notes"].fillna(
    value="no notes at all",
    inplace=True
)
print(data_with_default_notes["notes"].describe())
#     count              126314
#     unique                232
#     top       no notes at all
#     freq               120890
#     Name: notes, dtype: object

# Another type of data that may need to be pruned is invalid data, which can especially be a problem if the
# data was entered manually or collected from a program/form that didn't do appropriate error checking. One
# of the best ways to determine if there are any invalid data points in the data set is to use the
# describe() method. For example, in the nba data set, describe shows that the year_id varies between 1947
# and 2015. That seems pretty reasonable. Looking at pts however, the range is between 186 and 0. 0 seems a
# little odd. To find the rows where the home team scored zero points, the following query is run:
print(nba[nba["pts"] == 0])
#            gameorder       game_id  ... forecast                           notes
#     26684      13343  197210260VIR  ...     0.33  at Richmond VA; forfeit to VIR
#
#     [1 rows x 23 columns]
# The query produces one game, in which the Denver Nuggets forfeited to the Virginia Squires. That's
# reasonable, although certain analyses might be thrown off by such an outlier so removing it from the
# dataset might be necessary.

# Yet another type of bad data that can affect a dataset is inconsistent values. These are data that don't
# make logical sense. For example, in the nba DataFrame it wouldn't make sense if the home team has more
# points than their opponent but the game is recorded as a loss. Conversely, it wouldn't make sense if the
# if the opponent scored more points than the home team but the game was recorded as a win. Fortunately
# these conditions like these are very easy to check:
print(nba[(nba["pts"] > nba["opp_pts"]) & (nba["game_result"] != 'W')].empty)
#     True
print(nba[(nba["pts"] < nba["opp_pts"]) & (nba["game_result"] != 'L')].empty)
#     True
# Since both of these conditions are true, there aren't any games that have been incorrectly recorded.

# Data cleaning is a common issue when dealing with real world data sets as is combining multiple datasets
# into one. For that purpose Pandas provides the concat() method. For example, the following is a new
# DataFrame with more city data in it:
further_city_data = pd.DataFrame(
    {"revenue": [7000, 3400], "employee_count": [2, 2]},
    index=["New York", "Barcelona"]
)
# These new data can be added together with the existing cities_data DataFrame into a new DataFrame:
all_cities_data = pd.concat([city_data, further_city_data], sort=False)
print(all_cities_data)
#     Amsterdam   4200    5.0
#     Tokyo       6500    8.0
#     Toronto     8000    NaN
#     New York    7000    2.0
#     Barcelona   3400    2.0
# Note: Here the sort keyword has been used to ensure that the data doesn't get sorted. The default has
# changed over versions (from default True to default False) it's a good idea to include the keyword so
# that there are no issues dependent on which version of Pandas is on the machine running the code.

# By default the concat() method appends along axis=0, that is it appends the rows of the second DataFrame
# to the first. It is also possible to use concat to append on axis=1, that is to append columns from the
# second DataFrame to the first. For example:
city_countries = pd.DataFrame({
    "country": ["Holland", "Japan", "Holland", "Canada", "Spain"],
    "capital": [1, 1, 0, 0, 0]},
    index=["Amsterdam", "Tokyo", "Rotterdam", "Toronto", "Barcelona"]
)
cities = pd.concat([all_cities_data, city_countries], axis=1, sort=False)
print(cities)
#                revenue  employee_count  country  capital
#     Amsterdam   4200.0             5.0  Holland      1.0
#     Tokyo       6500.0             8.0    Japan      1.0
#     Toronto     8000.0             NaN   Canada      0.0
#     New York    7000.0             2.0      NaN      NaN
#     Barcelona   3400.0             2.0    Spain      0.0
#     Rotterdam      NaN             NaN  Holland      0.0

# Pandas places nulls in positions that don't have corresponding data (e.g. New York's country and capital
# status). To leave out any rows that have no corresponding data in the opposite table (an inner join) can
# be achieved by passing the join keyword with the value inner, like so:
print(pd.concat([all_cities_data, city_countries], axis=1, join="inner"))
#                revenue  employee_count  country  capital
#     Amsterdam     4200             5.0  Holland        1
#     Tokyo         6500             8.0    Japan        1
#     Toronto       8000             NaN   Canada        0
#     Barcelona     3400             2.0    Spain        0
# Note: Toronto is still in the table and still has it's null value because it carried that null into the
# join operation. Since there was data in the second table for Toronto it is included in the inner join,
# regardless of the null.

# DataFrames can also be combined together using Pandas' merge() function:
countries = pd.DataFrame({
    "population_millions": [17, 127, 37],
    "continent": ["Europe", "Asia", "North America"]
}, index=["Holland", "Japan", "Canada"])
print(pd.merge(cities, countries, left_on="country", right_index=True))
#                revenue  employee_count  country    capital    population_millions     continent
#     Amsterdam     4200             5.0  Holland        1.0                    17         Europe
#     Rotterdam      NaN             NaN  Holland        0.0                    17         Europe
#     Tokyo         6500             8.0    Japan        1.0                   127           Asia
#     Toronto       8000             NaN   Canada        0.0                    37  North America

# By default merge() does an inner join, but it can be used to do other types of joins (left, right, outer)
# by using the how keyword:
print(pd.merge(
    cities,
    countries,
    left_on="country",
    right_index=True,
    how="left"
))
#                revenue  employee_count  country    capital    population_millions     continent
#     Amsterdam     4200             5.0  Holland        1.0                    17         Europe
#     Tokyo         6500             8.0    Japan        1.0                   127           Asia
#     Toronto       8000             NaN   Canada        0.0                    37  North America
#     New York      7000             2.0      NaN        NaN                   NaN            NaN
#     Barcelona     3400             2.0    Spain        0.0                   NaN            NaN
#     Rotterdam      NaN             NaN  Holland        0.0                    17         Europe


# DataFrame visualization is very important for creating understandable output from analyses. For this
# purpose MatPlotLib is one of the best libraries to use.

# A chart is created by calling the plot() method, passing into it either functions or data Series
# (x- then y-values), along with a format string which tells which color and type of line/points to use.

# The format string is given in the format "<letter><symbol>", e.g. "r-" or "b^". There are many colors
# available, including: r = red, b = blue, g = green, p = purple, etc. There are many line/point styles
# available, including: - = line, ^ = triangles, o = circles, s = squares, -- = dashed line, etc.

# It is also possible to determine the range of axes by using the axis([x_min, x_max, y_min, y_max]) method.

# The axes can also be labeled using the xlabel(str) and ylabel(str) methods.

# Another important method in the MatLibPlot library is subplot(). It is used to block together multiple
# charts into one figure. The subplot() method is called with the syntax:
# subplot([rows_of_charts][cols_of_charts][chart_number])
# For example: subplot(212) would be a chart in a figure that is two charts tall (row_of_charts = 2) and
# one column of charts wide (cols_of_charts = 1) and it is the second chart (chart_number = 2) so it is on
# the bottom of the two. If there are two or more columns and two or more rows, the numbering goes the same
# as script, left to right, top to bottom.

# Some plotting examples follow that show these properties:

# Example 1:
plt.plot([1, 2, 3, 4])
plt.ylabel("random numbers")
plt.savefig("example_1.png")
plt.close()
# Note: it is possible to pass in a single series to the plot function and it will assume that they are
# the y-values that are to be plotted, giving them arbitrary x-values starting with 0 and increasing by 1.
# Also note that no format string was given, so MatPlotLib will default to "b-" or blue line.
# Also, this example introduces the savefig(str) method, which is used to save a figure at the specified path.

# Example 2:
plt.plot([1, 2, 3, 4], [1, 4, 9, 16], 'ro')
plt.axis([0, 6, 0, 20])
plt.savefig('example_2.png')
plt.close()
# This example includes two Series, a format string, and the axis range modifying method.

# Example 3:
t = np.arange(0., 5., 0.2)
plt.plot(t, t, 'r--', t, t**2, 'bs', t, t**3, 'g^')
plt.savefig('example_3.png')
plt.close()
# This example shows the use of functions and plotting multiple lines on one chart using varying
# format strings.


# Example 4:
def f(t):
    return np.exp(-t) * np.cos(2*np.pi*t)


t1 = np.arange(0.0, 5.0, 0.1)
t2 = np.arange(0.0, 5.0, 0.02)

plt.figure(1)
plt.subplot(211)
plt.plot(t1, f(t1), 'bo', t2, f(t2), 'k')

plt.subplot(212)
plt.plot(t2, np.cos(2*np.pi*t2), 'r--')
plt.savefig('example_4.png')
plt.close(1)
# This example shows the use of the subplot() method to create two plots that share a single figure. It also
# has the figure() method which is used to distinguish and augment certain aspects of the figure (here it is
# used only to number the figure). The final method call releases the figure, since it is maintained if named.

# Example 5:
# np.random.seed(19680801)
#
# mu, sigma = 100, 15
# x = mu + sigma * np.random.randn(10000)
#
# n, bins, patches = plt.hist(x, 50, normed=1, facecolor='g', alpha=0.75)
#
# plt.xlabel("Smarts")
# plt.ylabel("Probability")
# plt.title("Histogram of IQ")
# plt.text(60, 0.25, r'$\mu=100,\ \sigma=15$')
# plt.axis([40, 160, 0, 0.03])
# plt.grid(True)
# plt.savefig("example_5.png")
# This example is somewhat complex, but the most important are the text methods. The xlabel() method provides
# a label to the x-axis. The ylabel() method does the same for the y-axis. The title() method gives the chart
# a title. The text() method has the syntax: text([x_pos], [y_pos], [text_str]). Another notable method is the
# grid() method, which, as implied, draws a major grid on the chart.
# Another notable item in this example is the use of a TeX string to use Greek symbols (such as mu and sigma).
# TeX strings are created with the r'${str}$'. Note the r in front which tells Python to treat it as a raw
# string, thereby ignoring escape slashes. The same string could be built without the raw designation as:
# "$\\mu=100,\\ \\sigma=15$" or """$\mu=100,\ \sigma=15$""".