from d22_data_format.name_lookups import dict_name_to_position

import matplotlib.pyplot as plt

import cartopy
import cartopy.crs as ccrs
import cartopy.feature as cfeature

print("This script queries detailed map information;")
print("this may take a few seconds, be patient...")

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

station_ids = list(dict_name_to_position.keys())

def robust_truncate(str_in, max):
    if len(str_in) < max:
        return str_in
    else:
        return str_in[0:max]

short_station_ids = [robust_truncate(str_in, 4) for str_in in station_ids]

for crrt_station_id in station_ids:
    list_lats.append(dict_name_to_position[crrt_station_id][0])
    list_lons.append(dict_name_to_position[crrt_station_id][1])
    list_names.append(crrt_station_id)

ax.scatter(list_lons, list_lats, transform=ccrs.PlateCarree(), color="red", zorder=3)

transform = ccrs.PlateCarree()._as_mpl_transform(ax)
for crrt_station_index in range(len(station_ids)):
    ax.annotate("{}".format(short_station_ids[crrt_station_index]),
                xy=(list_lons[crrt_station_index], list_lats[crrt_station_index]),
                xycoords=transform,
                xytext=(5, 5), textcoords="offset points", color="red"
                )

ax.set_extent([-3.5, 32.5, 50.5, 82.5])

plt.show()
