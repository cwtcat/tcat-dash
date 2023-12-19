import sys
from pathlib import Path

import pandas as pd
import numpy as np
import geopandas as gp
# import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta
from collections import defaultdict

import re
import wslPath

from dotenv import load_dotenv
load_dotenv()

'''
from IPython.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))
display(HTML("<style>.output_result { max-width:100% !important; }</style>"))
pd.set_option('display.max_rows', 500)
pd.set_option('display.min_rows', None)
pd.set_option('display.max_columns', None)
'''

#import service_day
#import csv

from dotenv import load_dotenv
load_dotenv()

gf_input_path = f"{os.getenv('GF_INPUT_PATH')}"
gf_input_path = wslPath.toPosix(gf_input_path)

gf_output_path = f"{os.getenv('GF_OUTPUT_PATH')}"
gf_output_path = wslPath.toPosix(gf_output_path)

dropfile_name = f"{os.getenv('DROPFILE')}"

def process_batch(file_path,file_list):

    """ with the list of files that we got, this is the entry point -
    make this into a batch-processor class

    All this does is take the list of files from genfare_process.py
    and pass each file to the class DayHistory for further analysis.

    accepts:
      file_path
    """
    #header_file_path = f"{os.getenv('HEADER_FILE_PATH')}"
    #header_path = wslPath.toPosix(header_file_path)
    day_dict_list = []
    all_days_miles = []
    types = {'vehicle': 'string', 'time': 'object', 'lat': 'string', \
        'long': 'string', 'speed': 'int64', 'direction': 'int64', \
        'offRouteStatus': 'int64', 'commStatus': 'int64', \
        'operationalStatus': 'int64', 'headwayStatus': 'int64', \
        'actualHeadway': 'int64', 'Server_Time': 'object', 'Route': 'string', \
        'Trip': 'string', 'Inbound_Outbound': 'int64', 'Deviation': 'int64', \
        'Onboard': 'int64', 'Vehicle_Name': 'string', 'Run_Id': 'string', \
        'Run_Name': 'string', 'Stop_Name': 'string', 'Operator_Record_Id': 'int64', \
        'Route_Name': 'string', 'Stop_Report': 'int64', 'Scheduled_Headway': 'int64', \
        'Target_Headway': 'int64', 'Alarm_State': 'int64', 'GPSStatus': 'int64', \
        'Boards': 'int64', 'Alights': 'int64', 'Confidence_Level': 'int64', \
        'Message_Type_Id': 'int64', 'Stop_Dwell_Time': 'int64', 'PTV_Health_Alert': 'int64', \
        'Stop_Id': 'string', 'StationaryStatus': 'int64', 'StationaryDuration': 'int64', \
        'VehicleStatusID': 'int64', 'Veh_Type_Id': 'int64', 'Block_Farebox_Id': 'string', \
        'OdometerValue': 'string', 'MDTFlags': 'int64', 'Previous_Stop_Id': 'string', \
        'Distance': 'string', 'Door_Cycle_Count': 'string', 'Departure_Time': 'object', \
        'Block_External_Id': 'int64', 'Property_Name': 'string'}

    day_miles_dict = {}
    df_list = []
    genfare_all = pd.DataFrame


    for file in file_list:

        response_data = {"msg": "Operation completed successfully."}
        fileyearmonth = file[0:4]
        posix_path_string = gf_input_path
        full_file_path = f"{posix_path_string}/{fileyearmonth}ATD/{file}"
        file_date = file[0:6]
        # try:

        # apply the header
        header_file = f"{posix_path_string}{'atd_header.csv'}"
        card_event_header = pd.read_csv((header_file))
        card_event_col = card_event_header.columns.values

        try:
            genfare = pd.read_csv( \
                full_file_path, \
                skiprows=[0], \
                names=card_event_col, \
                header=None, \
                dtype='string', \
                parse_dates=[3,29], \
                keep_default_na=True, \
                low_memory=False)
        except FileNotFoundError:
            genfare = card_event_header

        #display(genfare.to_string())
        # apply the header
        #header_file = f"{header_path}{'genfare_header.csv'}"

        #genfare_column_names = pd.read_csv(header_file)
        #genfare.columns=genfare_column_names.columns.values
        print(f'total rows for {file}: {len(genfare)}')
        df_list.append(genfare)
    genfare_all = pd.concat(df_list)
    print(f'total rows for genfare_all: {len(genfare_all)}')
    return(genfare_all)


def get_dropfile_data(dropfile):
    dropfile_file_path = f"{gf_input_path}{dropfile_name}"

    # get header data
    dropfile_header_file = f"{gf_input_path}{'cu_dropfile_header.csv'}"
    dropfile_header = pd.read_csv( \
        dropfile_header_file, \
        dtype='string', \
        keep_default_na=False, \
        low_memory=False)

    dropfile_col = dropfile_header.columns.values
    print(dropfile_col)

    cu_dropfile_df = pd.read_csv( \
        dropfile_file_path, \
        names=dropfile_col, \
        dtype='string', \
        keep_default_na=False, \
        low_memory=False)

    # apply the header
    # header_file = f"{gf_input_path}{'cu_dropfile_header.csv'}"
    # dropfile_header = pd.read_csv(header_file)
    # cu_dropfile_df.columns = dropfile_header.columns.values
    return(cu_dropfile_df)


def count_genfare_riders(genfare_result,cu_dropfile):
    all_cu_rider_events = genfare_result.merge(cu_dropfile, left_on='trk2', right_on='Track2')
    all_cu_rider_events['date'] = pd.to_datetime(all_cu_rider_events['ts']).dt.date
    cu_rider_events_by_param = all_cu_rider_events.groupby(['param']).count()
    cu_rider_by_param = cu_rider_events_by_param['loc_n']
    print(cu_rider_by_param.to_string())


    exit()
        # now, we have the entire day file in genfare
        # this could be a class

        # make modifications to genfare
        # add compound ids
        # genfare["OdometerValue"]=genfare.loc[genfare["OdometerValue"] == "NULL", "OdometerValue"] = 0
        # genfare['OdometerValue'] = genfare['OdometerValue'].astype('float64')
'''

        try:
            genfare['OdometerValue'] = genfare['OdometerValue'].apply(lambda x: float(x.replace('NULL', "0.0")))
        except AttributeError:
            pass

        genfare['vehicle'] = genfare['vehicle'].apply(str)
        genfare['Block_Farebox_Id'] = genfare['Block_Farebox_Id'].apply(str)
        genfare['Block_Farebox_Id'] = genfare['Block_Farebox_Id'].str.slice(0, 4)
        genfare['trip_id'] = genfare['Trip'].astype('string')
        genfare['trip_id'] = genfare['trip_id'].str.zfill(4)
        genfare['trip_stop_cid'] = genfare['trip_id'] + "-" + genfare['Stop_Id'].astype('string')
        genfare['block_trip_stop_cid'] = genfare['Block_Farebox_Id'].astype('string') + "-" + genfare['trip_id'] + "-" + genfare['Stop_Id'].astype('string')
        genfare['block_trip_cid'] = genfare['Block_Farebox_Id'].astype('string') + "-" + genfare['trip_id']
        genfare.drop(genfare.loc[genfare['Route_Name']!="Route 30"].index, inplace=True)
        genfare.drop(genfare.loc[genfare['Stop_Id']=="0"].index, inplace=True)
        genfare = genfare[['Route_Name','vehicle','Server_Time','Trip','Stop_Id','Boards','Alights']]
        genfare['Date'] = pd.to_datetime(genfare['Server_Time']).dt.date
        # genfare_data = genfare.copy(deep=True)
        df_list.append(genfare)
    genfare_all = pd.concat(df_list)

        # genfare.to_csv(f"{csv_path}{file_date}_genfare.csv")
        # genfare is now the dataframe for all events in the day
        # chop up the day into useful chunks
    print(genfare_all)

    # genfare_all.drop(genfare_all[genfare_all['Route_Name'] != "Route 30"].index, inplace = True)
    # genfare_all.drop(genfare_all[genfare_all['Stop_Id'] == "0"].index, inplace = True)

    route30_stops  = [["100","Commons-Seneca",1,1], ["112","Albany@SalvArmy",2,1], ["165","Commons-GreenSt",3,1],["1701","State@Stewart",4,1],\
                    ["1703","State@Quarry",5,1],["1711","College@Mitchel",6,1],["1715","Collegtwn Crsng",7,1],["1719","College @ Oak",8,1],\
                    ["1531","Carpenter Hall",9,1],["1533","Statler Hall",10,1],["1525","Rockefeller Hl",11,1],["1325","Thurston@Balch",12,1],\
                    ["1355","Jessup@Trphamr",13,1],["1343","RPCC-Jessup",14,1],["1345","Jessup@PleasGrv",15,1],["3711","PlstGrv@Hanshaw",16,2],\
                    ["3515","Tripham@Texas",17,2],["3519","Triphmr@Winthro",18,2],["3521","Trphmr@Kendal",19,2],["3526","Triphammer Mrlt",20,2],\
                    ["3579","Trphamr@CayuMal",21,2],["3584","YMCA",22,2],["3588","Ithaca Mall",23,2],["3593","Tops Lansing",24,3],\
                    ["3525","LansingWestApts",25,3],["3522","Trphmr@Kendal",26,3],["3520","Triphmr@Winthro",27,3],["3516","Tripham@Spruce",28,3],\
                    ["3712","PlstGrv@Hanshaw",29,3],["1346","Jessup@PleasGrv",30,4],["1344","RPCC-Jessup",31,4],["1356","Jessup@Trphamr",32,4],\
                    ["1326","Risley-Shelter",33,4],["1524","GoldwinSmithHl",34,4],["1534","Sage Hall",35,4],["1716","SchwrtzPrfArCtr",36,4],\
                    ["1710","College@Mitchel",37,4],["1704","State@Quarry",38,4],["1702","State@Stewart",39,4]]
    stops_df = pd.DataFrame(route30_stops)
    stops_df_column_names = ["Stop_Id","Stop_Name","Stop_Sort","Stop_Segment"]
    stops_df.columns=stops_df_column_names
    print(stops_df)
    stops_counts_df = pd.merge(genfare_all,stops_df,how ='inner', on =['Stop_Id'])
    # stops_counts_df.merge(stops_df, left_on='Stop_Id', right_on='stop_id')
    pd.set_option('display.max_rows', None)
    print(stops_counts_df.sort_values(by=["Trip","vehicle"]))
    print(len(stops_counts_df.index))
    stops_counts_df.to_csv(f'rt_30_stops.csv')
    exit()
    day_dict = {}
    service = service_day.ServiceDay(genfare,file_date)
    total_boards = service.vboards()
    total_alights = service.valights()
    alight_error = total_boards-total_alights
    vblock_df, block_list, bus_list = service.vblocks()

    print (f"total number of boards for {service.date_id}: {total_boards}")

    print (f"total number of alights for {service.date_id}: {total_alights}")
    print (f"difference of pax boards-pax_alights: {alight_error}")
    print (f"there are {len(vblock_df.index)} block rows for this day.")
    print (f"there are {len(block_list)} unique blocks for this day.")
    print (f"the service type today is {service.service_type} - type: {type(service.service_type)}")
    print (f"Here's a list of all the blocks in this day:")
    print (block_list)
    missing_block_list = []
    missing_bus_list = []
    inservice_bus_list = []
    if service.service_type == 1:
        service_blocks = ["7001","7002","7003","7004","7005","7006","7007","7008", \
                            "7009","7010","7011","7012","7013","7014","7015","7016", \
                            "7017","7018","7019","7020","7021","7022","7023","7024", \
                            "7025","7026","7027","7028","7029","7030","7031","7032", \
                            "7033","7034","7035","7036","7037","7038","7039","7040"]
    elif service.service_type == 2:
        service_blocks = ["8001","8002","8003","8004","8005","8006","8007","8008", \
                            "8009","8010","8001","8011","8012","8013","8014","8015","8016","8017"]


    else:
        service_blocks = ["9001","9002","9003","9004","9005","9006","9007","9008", \
                            "9009","9010","9001","9011","9012","9013","9015","9014","9016"]
    # compare the day's blocks to the block_list
    for block in service_blocks:
        if block not in block_list:
            missing_block_list.append(block)
    print (f"Here's a list of all the blocks that are missing from Avail:")
    print (missing_block_list)

    print ("Let's look at each block:")
    all_bus_list = []
    for block_id in block_list:
        block_bus_list = service.get_block_detail(block_id)
        print(f"block: {block_id} bus_list: {block_bus_list}")
        for bus in block_bus_list:
            all_bus_list.append(bus)

    all_bus_list = [*set(all_bus_list)]
    buses_not_in_service = [bus_id for bus_id in all_bus_list if bus_id not in bus_list]
    bus_count = len(all_bus_list)
    master_bus_list = ["1105","1106","1108","1109","1110","1111","1112","1113","1114", \
                        "1116","1118","1501","1502","1601","1602","1603","1604","1605", \
                        "1801","1802","1803","1804","1805","1806","1807","1808","1809", \
                        "1810","1811","1901","1902","1903","1904","1905","1906","1907", \
                        "1908","1909","1910","1911","1912","2506"]
    # ADJUSTED FOR DIESEL ONLY
    # what buses are not in the daily list
    for master_bus in master_bus_list:
        if master_bus not in all_bus_list:
            missing_bus_list.append(master_bus)
        if master_bus in all_bus_list:
            inservice_bus_list.append(master_bus)


    missing_bus_count = len(missing_bus_list)
    print (f"Here's a list of all {missing_bus_count} buses that are not listed on blocks for the day:")
    print (missing_bus_list)
    print(f"Here's a list of buses that were out of service for the day:")
    print(buses_not_in_service)
    print("")
    inservice_bus_count = len(inservice_bus_list)
    print (f"Here's a list of all {inservice_bus_count} inservice DIESEL buses that are listed on blocks for the day:")
    print (inservice_bus_list)
    # print (f"total pmt for {service.date_id}: {service.pmt()}")
    print (f"Here's a list of all {bus_count} buses that were out on blocks today.")
    print (all_bus_list)
    print ("Let's display some individual bus stats:")

    day_actual_miles = 0.0
    day_actual_hours = 0.0
    # modified to inservice_bus_list to get diesel
    # TODO new master bust lists with attributes
    for bus in inservice_bus_list:
        actual_bus_miles, actual_bus_hours = service.get_bus_detail(bus)
        print(f"bus: {bus}, actual_miles: {actual_bus_miles}, actual_hours: {actual_bus_hours}")
        day_actual_miles = day_actual_miles + actual_bus_miles
        day_actual_hours = day_actual_hours + actual_bus_hours
    print(f"all buses traveled a total of {day_actual_miles} miles.")
    day_miles_dict[service.date_id]=day_actual_miles
    print(f"together the {bus_count} buses were in service for a total of {day_actual_hours} hours.")
    ### print (f"Let's pass the output csv path to the ServiceDay class and get some results.")
    ### daily_pmt, daily_dh_pmt, daily_true_pmt, daily_dh_miles, daily_all_miles, daily_rev_miles, daily_bus_time, daily_dh_time, daily_rev_time = service.get_daily_pmt(csv_path)

    ### print(f"all buses traveled a total of {day_actual_miles} miles.")
    ### print(f"does this match values from the pmt function? {daily_all_miles} miles.")

    ### try:
    ###    print(f"pmt/actual_miles is {daily_pmt/daily_all_miles} miles per passenger?.")
    ###except Exception as err:
    ###    pass
    ### print(f"UPT: {total_boards}, PMT: {daily_pmt}, PMT/UPT:  {daily_pmt/total_boards}")
    ### print(f"Revenue miles: {daily_rev_miles}")

    # make a JSON line for the day
    ### day_dict["date_id"] = service.date_id
    ### day_dict["service_type"] = service.service_type
    ### day_dict["daily_upt"] = total_boards
    ### day_dict["daily_pmt"] = daily_pmt
    ### day_dict["daily_dh_pmt"] = daily_dh_pmt
    ### day_dict["daily_true_pmt"] = daily_true_pmt
    ### day_dict["daily_dh_miles"] = daily_dh_miles
    ### day_dict["daily_all_miles"] = daily_all_miles
    ### day_dict["daily_rev_miles"] = daily_rev_miles
    ### day_dict["day_actual_hours"] = day_actual_hours
    ### day_dict["bus_count"] = bus_count
    ### day_dict["daily_bus_time"] = daily_bus_time
    ### day_dict["daily_dh_time"] = daily_dh_time
    ### day_dict["daily_rev_time"] = daily_rev_time
    ### day_dict["total_boards"] = total_boards
    ### day_dict["total_alights"] = total_alights
    ### day_dict["alight_error"] = alight_error
    ### day_dict_list.append(day_dict)

    field_names = ['date_id', 'service_type','daily_upt','daily_pmt', \
                    'daily_dh_pmt','daily_true_pmt','daily_dh_miles', \
                    'daily_all_miles','daily_rev_miles','day_actual_hours', \
                    'bus_count','daily_bus_time','daily_dh_time','daily_rev_time', \
                    'total_boards', 'total_alights',  'alight_error']
    with open('my_batch.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(day_dict_list)
    response_data = "Success."
    print(day_miles_dict)
    print(all_days_miles)
    return response_data

'''