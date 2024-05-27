import datetime
import pytz

from d22_data_format.dataset_accessor import DatasetAccessor

# dataset_accessor_WL_nc4 = DatasetAccessor("./dataset_DK_test.nc4")
dataset_accessor_WL_nc4 = DatasetAccessor("./WL_oil_platforms.nc4")

list_station_ids = dataset_accessor_WL_nc4.station_ids
print(list_station_ids)

# how to plot the location of the stations
dataset_accessor_WL_nc4.visualize_station_positions()

datetime_start = datetime.datetime(2005, 11, 2, 1, 0, 0, tzinfo=pytz.utc)
datetime_end = datetime.datetime(2005, 11, 2, 12, 0, 0, tzinfo=pytz.utc)

# how to access a range of data from the station
for crrt_station in list_station_ids:
    accessed_data = dataset_accessor_WL_nc4.get_data(crrt_station, datetime_start, datetime_end)
    print("querying station {} for data gave:".format(crrt_station))
    print(accessed_data)


datetime_start = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
datetime_end = datetime.datetime(2019, 12, 1, 0, 0, 0, tzinfo=pytz.utc)

for crrt_station in list_station_ids:
    dataset_accessor_WL_nc4.visualize_single_station(crrt_station, datetime_start, datetime_end)
