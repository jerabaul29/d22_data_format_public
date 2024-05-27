import logging

from pathlib import Path

from pprint import PrettyPrinter

import pickle

import itertools

from d22_data_format.exploration_tools import d22_station_generator
from d22_data_format.helpers.folders_navigation import get_sorted_subfolders
from d22_data_format.helpers.readfile import FileLinesYielder


def generate_dict_folder_to_id(quick=False):
    """Go through all the data, and create a lookup dict of the different "station IDs",
    really data package titles, used in each station folder."""
    path_root_data = "/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/"

    dict_folder_to_id = {}

    stations_subfolders = get_sorted_subfolders(path_root_data)

    for crrt_station_path in stations_subfolders:
        logging.info("explore station path {}".format(crrt_station_path))

        crrt_folder = crrt_station_path.split("/")[-1]
        logging.info("crrt folder is: {}".format(crrt_folder))

        d22_path = crrt_station_path + "/d22/"

        if not Path(d22_path).is_dir():
            logging.warning("seems like {} has no d22!".format(crrt_station_path))
            continue

        list_subfolders = list(d22_station_generator(d22_path))

        dict_folder_to_id[crrt_folder] = []

        for crrt_subfolder in list_subfolders:
            logging.info("looking at the subfolder: {}".format(crrt_subfolder))

            files = sorted([str(x) for x in Path(crrt_subfolder).glob("*") if x.is_file()])

            for crrt_file in files:
                try:
                    logging.info("looking at the file: {}".format(crrt_file))

                    file_lines_yielder_instance = FileLinesYielder()
                    file_lines_yielder = file_lines_yielder_instance.file_lines_yielder
                    line_yielder = file_lines_yielder(crrt_file,
                                                      automatic_gzip_recognition=True,
                                                      encoding="latin-1")

                    while True:
                        # find the next data package start
                        while True:
                            crrt_line = next(line_yielder)
                            if crrt_line == "!!!!\n" or crrt_line == "!!!!\r\n":
                                break

                        # get the title
                        _ = next(line_yielder)
                        line_3 = next(line_yielder)

                        crrt_station_id = line_3[:-2].rstrip()

                        # add the title to the summarizing dict
                        if crrt_station_id not in [tpl[0] for tpl in dict_folder_to_id[crrt_folder]]:
                            logging.warning("add id {} for station {} based on file {}".
                                            format(crrt_station_id, crrt_folder, crrt_file))
                            dict_folder_to_id[crrt_folder].append((crrt_station_id, crrt_file))

                        # if we do a quick test, only look at the first title in each file
                        if quick:
                            break
                except:
                    pass

    return dict_folder_to_id


dict_name_lookups_hand_cleaned = \
    {   'aastahansteen': [   (   'AASTA',
                                 '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/aastahansteen/d22/2019/20190918.d22')],
        'asgardb': [   (   'Åsgard B',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/asgardb/d22/2013/20130806.d22')],
        'brage': [   (   '1021',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/brage/d22/2013/20130806.d22'),
                     (   'Brage',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/brage/d22/2014/20141220.d22')],
        'draugen': [   (   'Draugen',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/draugen/d22/1998/19981201.d22.gz'),
                       (   'DRAUGEN',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/draugen/d22/2008/20080618.d22'),
                       (   '1022',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/draugen/d22/2018/20181010.d22')],
        'ekofisk': [   (   'Ekofisk',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/ekofisk/d22/1997/19970226.d22.gz')],
        'ekofiskL': [   (   'EKO-L',
                            '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/ekofiskL/d22/2014/20140506.d22')],
        'gjoa': [   (   'GJOA 1076',
                        '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/gjoa/d22/2012/20120213.d22')],
        'goliat': [   (   '1096',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/goliat/d22/2015/20150909.d22')],
        'grane': [   (   '1046',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/grane/d22/2013/20130806.d22')],
        'gudrun': [   (   '1094',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/gudrun/d22/2015/20151015.d22')],
        'gullfaksc': [   (   'Gullfaks C',
                             '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/gullfaksc/d22/1999/19990518.d22.gz')],
        'heidrun': [   (   '6538/A-HDR',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/heidrun/d22/1996/19960201.heidrun.gz'),
                       (   'Heidrun',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/heidrun/d22/1998/19980311.d22.gz'),
                       (   'HDR',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/heidrun/d22/2003/20030622.d22.gz')],
        'heimdal': [   (   'Heimdal',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/heimdal/d22/2002/20020806.d22.gz')],
        'huldra': [   (   '1043',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/huldra/d22/2013/20130806.d22')],
        'johansverdrup': [   (   'JOH',
                                 '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/johansverdrup/d22/2020/20200313.d22')],
        'kristin': [   (   '1054',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/kristin/d22/2013/20130806.d22')],
        'kvitebjorn': [   (   'kvb',
                              '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/kvitebjorn/d22/2013/20130806.d22')],
        'leiveiriksson': [   (   'LeivEiriksson',
                                 '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/leiveiriksson/d22/2008/20080624.d22')],
        'njorda': [   (   'Njord A',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/njorda/d22/2004/20040817.d22.gz'),
                      (   'NRA',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/njorda/d22/2006/20060403.d22'),
                      (   '1044',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/njorda/d22/2013/20130514.d22')],
        'nobleglobet2': [   (   '1609_Noble_Globetrotter_II (SHELL',
                                '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/nobleglobet2/d22/2013/20130812.d22')],
        'norne': [   (   'Norne EMS System - V3.10',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/1997/19970901.d22.gz'),
                     (   'Norne EMS System - V3.11',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/1997/19971009.d22.gz'),
                     (   'Norne EMS System - V3.12',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/1998/19980318.d22.gz'),
                     (   'Norne EMS System - V3.13',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/1998/19980514.d22.gz'),
                     (   'Norne EMS System - V3.14',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/1998/19980926.d22.gz'),
                     (   'Norne EMS System - V3.1',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/1999/19991209.d22.gz'),
                     (   'Norne EMS System - V3.15',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/2002/20020725.d22.gz'),
                     (   'Demonstration - V3.15',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/2009/20091029.d22'),
                     (   'NOR',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/2009/20091203.d22'),
                     (   'Norne',
                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/norne/d22/2009/20091203.d22')],
        'ormenlange': [   (   'West Navigator',
                              '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/ormenlange/d22/2007/20070912.d22'),
                          (   'WNG',
                              '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/ormenlange/d22/2010/20100107.d22')],
        'oseberg': [   (   'Oseberg feltsenter',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/oseberg/d22/2003/20030612.d22.gz')],
        'osebergc': [   (   '1020',
                            '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/osebergc/d22/2013/20130806.d22'),
                        (   'Oseberg C',
                            '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/osebergc/d22/2013/20130808.d22')],
        'osebergost': [   (   '1028',
                              '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/osebergost/d22/2013/20130806.d22')],
        'osebergsyd': [   (   '1034',
                              '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/osebergsyd/d22/2013/20130806.d22')],
        'petrojarlknarr': [   (   '1095',
                                  '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/petrojarlknarr/d22/2015/20150428.d22')],
        'scarabeo8': [   (   '1823',
                             '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/scarabeo8/d22/2015/20150115.d22')],
        'sleipner': [   (   '6511-A/SLA',
                            '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/sleipner/d22/1995/19950223.sleipner.gz'),
                        (   'Sleipner',
                            '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/sleipner/d22/1998/19980210.d22.gz'),
                        (   'Sleipne',
                            '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/sleipner/d22/1998/19980211.d22.gz'),
                        (   'Sleipner A',
                            '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/sleipner/d22/2004/20041105.d22.gz'),
                        (   '1017',
                            '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/sleipner/d22/2014/20141110.d22'),
                        (   'SleipnerA',
                            '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/sleipner/d22/2014/20141124.d22')],
        'sleipnerb': [   (   '1059',
                             '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/sleipnerb/d22/2013/20130806.d22')],
        'snorrea': [   (   '1036',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/snorrea/d22/2013/20130806.d22')],
        'snorreb': [   (   '1039',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/snorreb/d22/2013/20130806.d22')],
        'stafjorda': [   (   'Statjord A',
                             '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/stafjorda/d22/2004/20040817.d22.gz'),
                         (   'Statfjord A',
                             '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/stafjorda/d22/2010/20100407.d22')],
        'statfjordb': [   (   '1068',
                              '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/statfjordb/d22/2013/20130806.d22'),
                          (   'Statfjord C',
                              '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/statfjordb/d22/2013/20130806.d22')],
        'transoceanspitsbergen': [   (   '1831',
                                         '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/transoceanspitsbergen/d22/2014/20141030.d22')],
        'trolla': [   (   'Troll A',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/trolla/d22/1998/19980319.d22.gz')],
        'trollb': [   (   'TROLL B',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/trollb/d22/2004/20041213.d22'),
                      (   'TRB',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/trollb/d22/2013/20130218.d22')],
        'trollc': [   (   '1000',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/trollc/d22/2013/20130806.d22'),
                      (   '1032',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/trollc/d22/2015/20150331.d22')],
        'ula': [   (   'ULA - V3.15',
                       '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/ula/d22/2007/20071105.d22'),
                   (   'ULA',
                       '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/ula/d22/2011/20110908.d22')],
        'valhall': [   (   'Valhall QP EMS System - V3.15',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/valhall/d22/2007/20070330.d22'),
                       (   '1081',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/valhall/d22/2011/20110909.d22')],
        'veslefrikka': [   (   '1016',
                               '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/veslefrikka/d22/2013/20130806.d22')],
        'veslefrikkb': [   (   '1063',
                               '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/veslefrikkb/d22/2013/20130806.d22')],
        'visund': [   (   '1027',
                          '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/visund/d22/2013/20130806.d22')],
        'westnav': [   (   'WNG',
                           '/lustre/storeB/immutable/archive/projects/metproduction/DNMI_OFFSHORE/westnav/d22/2013/20130821.d22')]}

dict_name_lookups = {}

for crrt_key in dict_name_lookups_hand_cleaned:
    dict_name_lookups[crrt_key] = [entry[0] for entry in dict_name_lookups_hand_cleaned[crrt_key]]


list_station_names = list(dict_name_lookups.keys())
list_stations_ids = list(itertools.chain.from_iterable([dict_name_lookups[crrt_key] for crrt_key in dict_name_lookups]))

dict_ids_lookup = {}

for crrt_station in dict_name_lookups:
    crrt_ids = dict_name_lookups[crrt_station]

    for crrt_id in crrt_ids:
        dict_ids_lookup[crrt_id] = crrt_station

for crrt_key_1 in dict_name_lookups:
    for crrt_key_2 in dict_name_lookups:
        if crrt_key_1 != crrt_key_2:
            for crrt_entry in dict_name_lookups[crrt_key_1]:
                if crrt_entry in dict_name_lookups[crrt_key_2]:
                    logging.warning("found {} in {} and {}".format(crrt_entry, crrt_key_1, crrt_key_2))


dict_name_to_position = {
    "ekofisk": (56.50, 3.20),
    "ekofiskL": (56.50, 3.20),
    "sleipner": (58.37, 1.91),
    "heimdal": (59.57, 2.24),
    "trollc": (60.88, 3.61),
    "snorreb": (61.53, 2.21),
    "asgardb": (65.09, 6.81),
    "heidrun": (65.33, 7.33),
    "draugen": (64.353172, 7.782606), # unsure position
    "trollb": (60.88, 3.61),  # unsure position
    "veslefrikka": (60.766667, 2.883333), # unsure position
    "veslefrikkb": (60.766667, 2.883333), # unsure position
    "veslefrikk": (60.766667, 2.883333), # unsure position
    "valhall": (56.278164, 3.395331),
    "ula": (57.111431, 2.847331),
}


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)

    pp = PrettyPrinter(indent=4).pprint

    # show the lookup dict generated from the files
    if False:
        logging.basicConfig(level=logging.INFO)
        dict_folder_to_id = generate_dict_folder_to_id()

        pp(dict_folder_to_id)

        with open("./dict_data_lookup.pkl", "wb") as pkl_handle:
            pickle.dump(dict_folder_to_id, pkl_handle)

    if True:
        pp(dict_name_lookups)
        pp(dict_ids_lookup)
        pp(list_station_names)
        pp(list_stations_ids)
