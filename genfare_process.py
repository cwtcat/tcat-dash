import os
import sys
import wslPath
from datetime import datetime, timedelta
import argparse
from get_genfare_data import *

from dotenv import load_dotenv
load_dotenv()


def get_datetime(datecode):
    """ this utility function makes a real datetime from a 6-digit yymmdd datecode """

    date_format = "%y%m%d"
    date_object = datetime.strptime(datecode, date_format)
    return(date_object)

def get_file_list(args):
    """if there is a list of files, parse it into a value_list"""

    fail_msg = "Got a file list- stop."
    # if input_file, read and populate value_list
    try:
        value_list = args.input_file.read().strip().replace("\n", "").split(",")
    except AttributeError:
        fail_msg = "No input file list.  Go to the next function. "
    return (value_list, fail_msg)

def get_file_values(args):
    """ if the args include comma separated filenames, parse them to a value list"""

    fail_msg = "Got file values- stop."
    value_list = []
    date_list = []
    try:
        if "," in args.values:
            date_list = args.values.strip().replace(",", ",").split(",")
        # if there is only one value, put it in a list (of one item)
        else:
            date_list = [args.values]

        for date_string in date_list:
            value_list.append(f"{date_string}ATD.csv")

    except AttributeError:
       fail_msg = "No value list."


    return value_list, fail_msg

def get_file_in_daterange(args):
    """ if a date range is provided, find all the dates and make a list of files"""

    fail_msg = "Got a date range- stop."
    value_list = []
    print(f"here's the begin date: {args.begin_date}")
    try:

        begin_datetime = get_datetime(args.begin_date)
        end_datetime = get_datetime(args.end_date)
        print(f'begin: {begin_datetime}, end: {end_datetime}')
        day_delta = end_datetime - begin_datetime
        print(f'total range: {day_delta}')

        for i in range(day_delta.days + 1):
            thisday = begin_datetime + timedelta(days=i)
            day_string = (str(thisday).replace("-",""))[2:]
            date_string = day_string[0:6]
            file_string = (f"{date_string}ATD.csv")
            value_list.append(file_string)

    except ValueError:
        fail_msg = "No value list."
    except TypeError:
        fail_msg = "bad start_date or end_date?"
    return (value_list, fail_msg)

def get_file_path(args):
    """ get file path from args or .env """
    genfare_path = f"{os.getenv('GF_INPUT_PATH')}"
    print(f"path from env: {genfare_path}")
    args_path = args.file_path
    print(f"path from args: {args_path}")
    if args_path:
        genfare_path = args_path
    DIR = wslPath.toPosix(genfare_path)
    print(f"path: {DIR}")

    # GENFARE_FILE=f"{GENFARE_PATH}/{filename}"
    return DIR


def get_file_names(args):
    try:
        value_list = None
        if value_list == None:
            value_list, fail_msg = get_file_list(args)
    except:
        try:
            if value_list == None:
                value_list, fail_msg = get_file_values(args)
        except:
            try:
                if value_list == None:
                    value_list, fail_msg = get_file_in_daterange(args)
                if value_list == None:
                    # end of the line
                    # if no values, return an empty list
                    value_list = []

            except Exception as err:
                print(f'error {err}')

    return (value_list)


def main(argv):
    """ handle args and get filenames to pass to class"""
    # set default arg values and process args
    file_list = []
    value_list = []
    #begin_date = ""
    #end_date = ""
    fail_msg = ""
    dropfile = ""

    args = get_args()
    file_names = get_file_names(args)
    file_count = len(file_names)
    file_path = get_file_path(args)
    print(f"process {file_count} files?")
    print(f'filenames: {file_names}')
    print(f'filepath: {file_path}')
    genfare_result = process_batch(file_path, file_names)
    cu_dropfile = get_dropfile_data(dropfile)
    count_cornell = count_genfare_riders(genfare_result,cu_dropfile)

    print(f'total rows for count_cornell: {len(count_cornell)}')




def get_args():
    parser = argparse.ArgumentParser(
        description="select genfare files for processing"
    )
    parser.add_argument(
        "-i",
        "--input_file",
        type=argparse.FileType("r"),
        # default=sys.stdin,
        help="filename with list of genfare file names to be processes",
    )
    parser.add_argument(
        "-v",
        "--values",
        help="comma separated list of values to be submitted to Dimensions",
    )
    parser.add_argument(
        "-p", "--file_path", help="path to data files"
    )
    parser.add_argument(
        "-b", "--begin_date", help="begin date (format: yymmdd), default 230101"
    )
    parser.add_argument(
        "-e", "--end_date", help="end date (format: yymmdd), default 231231"
    )
    args = parser.parse_args()
    print(f"arguments: {args}")
    return args


if __name__ == "__main__":
    main(sys.argv[1:])