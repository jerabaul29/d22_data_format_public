import pytz

import datetime

import matplotlib.pyplot as plt

import cartopy
import cartopy.crs as ccrs
import cartopy.feature as cfeature

import netCDF4 as nc4

from raise_assert import ras
from external_water_level_stations.helper.datetimes import assert_is_utc_datetime

from external_water_level_stations.helper.arrays import find_index_first_greater_or_equal


class DatasetAccessor():
    def __init__(self, path_to_NetCDF):
        self.path_to_NetCDF = path_to_NetCDF
        self.dict_metadata = self.get_dict_stations_metadata()

    def get_dict_stations_metadata(self):
        dict_stations_metadata = {}

        with nc4.Dataset(self.path_to_NetCDF, "r", format="NETCDF4") as nc4_fh:
            self.station_ids = nc4_fh["stationid"][:]
            self.number_of_stations = len(self.station_ids)

            for crrt_ind in range(self.number_of_stations):
                crrt_station_id = nc4_fh["stationid"][crrt_ind]
                crrt_lat = nc4_fh["latitude"][crrt_ind]
                crrt_lon = nc4_fh["longitude"][crrt_ind]

                crrt_dict_metadata = {}
                crrt_dict_metadata["station_index"] = crrt_ind
                crrt_dict_metadata["nc4_dump_index"] = crrt_ind
                crrt_dict_metadata["latitude"] = crrt_lat
                crrt_dict_metadata["longitude"] = crrt_lon

                dict_stations_metadata[crrt_station_id] = crrt_dict_metadata

        return dict_stations_metadata

    def visualize_available_times(self, date_start=None, date_end=None):
        # TODO: implement
        pass

    def visualize_station_positions(self):
        """Visualize the position of the stations."""

        # The data to plot are defined in lat/lon coordinate system, so PlateCarree()
        # is the appropriate choice of coordinate reference system:
        _ = ccrs.PlateCarree()

        # the map projection properties.
        proj = ccrs.LambertConformal(central_latitude=65.0,
                                     central_longitude=15.0,
                                     standard_parallels=(52.5, 75.0))

        plt.figure(figsize=(15, 18))
        ax = plt.axes(projection=proj)

        resol = '50m'
        bodr = cartopy.feature.NaturalEarthFeature(category='cultural',
                                                   name='admin_0_boundary_lines_land',
                                                   scale=resol, facecolor='none', alpha=0.7)
        land = cartopy.feature.NaturalEarthFeature('physical', 'land',
                                                   scale=resol, edgecolor='k', facecolor=cfeature.COLORS['land'])
        ocean = cartopy.feature.NaturalEarthFeature('physical', 'ocean',
                                                    scale=resol, edgecolor='none', facecolor=cfeature.COLORS['water'])
        lakes = cartopy.feature.NaturalEarthFeature('physical', 'lakes',
                                                    scale=resol, edgecolor='b', facecolor=cfeature.COLORS['water'])
        rivers = cartopy.feature.NaturalEarthFeature('physical', 'rivers_lake_centerlines',
                                                     scale=resol, edgecolor='b', facecolor='none')

        ax.add_feature(land, zorder=0)
        ax.add_feature(ocean, linewidth=0.2, zorder=0)
        ax.add_feature(lakes, zorder=1)
        ax.add_feature(rivers, linewidth=0.5, zorder=1)
        ax.add_feature(bodr, linestyle='--', edgecolor='k', alpha=1, zorder=2)

        list_lats = []
        list_lons = []
        list_names = []

        for crrt_station_id in self.station_ids:
            list_lats.append(self.dict_metadata[crrt_station_id]["latitude"])
            list_lons.append(self.dict_metadata[crrt_station_id]["longitude"])
            list_names.append(crrt_station_id)

        ax.scatter(list_lons, list_lats, transform=ccrs.PlateCarree(), color="red", zorder=3)

        transform = ccrs.PlateCarree()._as_mpl_transform(ax)
        for crrt_station_index in range(self.number_of_stations):
            ax.annotate("#{}{}".format(crrt_station_index, list_names[crrt_station_index]),
                        xy=(list_lons[crrt_station_index], list_lats[crrt_station_index]),
                        xycoords=transform,
                        xytext=(5, 5), textcoords="offset points", color="red"
                        )

        ax.set_extent([-3.5, 32.5, 50.5, 82.5])

        plt.show()

    def visualize_single_station(self, station_id, datetime_start, datetime_end):
        """Show the data for both observation and prediction for a specific station over
        a specific time interval.
        Input:
            - station_id: the station to look at
            - datetime_start: the start of the plot
            - datetime_end: the end of the plot
        """

        print("start netCDF4 data query...")
        datetime_timestamps, observation = self.get_data(station_id, datetime_start, datetime_end)

        print("start plotting...")
        plt.figure()

        plt.plot(datetime_timestamps, observation, label="obs")

        plt.ylim([-1000.0, 1000.0])
        plt.title("station: {}".format(station_id))

        plt.ylabel("water heigth [cm]")

        plt.legend()
        plt.xticks(rotation=45)

        plt.tight_layout()

        plt.show()

    def get_data(self, station_id, datetime_start, datetime_end):
        """Get the data contained in the netcdf4 dump about stations_id, that
        is between times datetime_start and datetime_end (both included).
        Input:
            - sation_id: the station ID, for example 'OSL'
            - datetime_start, datetime_end: the limits of the extracted data.
        Output:
            - data_timestamps: the timestamps of the data.
            - data_observation: the observation.
            - data_prediction: the prediction.
        """

        ras(station_id in self.station_ids)
        assert_is_utc_datetime(datetime_start)
        assert_is_utc_datetime(datetime_end)
        ras(datetime_start <= datetime_end)

        nc4_index = self.dict_metadata[station_id]["station_index"]

        timestamp_start = datetime_start.timestamp()
        timestamp_end = datetime_end.timestamp()

        with nc4.Dataset(self.path_to_NetCDF, "r", format="NETCDF4") as nc4_fh:
            data_timestamp_full = nc4_fh["timestamps"][:]
            data_observation_full = nc4_fh["observation"][nc4_index][:]

        first_index = find_index_first_greater_or_equal(data_timestamp_full, timestamp_start)
        last_index = find_index_first_greater_or_equal(data_timestamp_full, timestamp_end) + 1

        data_datetime = [datetime.datetime.fromtimestamp(crrt_timestamp, pytz.utc) for
                         crrt_timestamp in data_timestamp_full[first_index:last_index]]
        data_observation = data_observation_full[first_index:last_index]

        return(data_datetime, data_observation)
