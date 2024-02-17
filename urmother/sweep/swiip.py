import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir
import inkript.enkrypt as encrypt
import functools
import boto3

##additional modules
from googleapiclient.discovery import build
from google.oauth2 import service_account
import sqlite3
import json
import webbrowser
import shutil
import sqlalchemy as sa
import numpy as npy
import io
import dask
from dask import persist
import dask.bag as dskb
import dask.dataframe as dsk
import pandas as pda
pda.set_option( "display.max_columns", None )
pda.set_option( "display.width", None )
import csv
import fastparquet
import chardet
import glob
import dateutil.relativedelta as relativedelta
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
import streamlit as strmlt
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
import random
import requests
from requests.exceptions import RequestException
import rapidfuzz
import multiprocessing as mp
import jsonlines
import re
import pprint
import math


##=====================================
##  SET VARIABLES HERE
##=====================================

compression_dict = {
        "json" : {
             "default_value" : None
            , "original_default_value" : None
            , "values" : [ "infer", None, "bz2", "gzip", "tar", "xz", "zip", "zstd" ]
        }
    , "parquet" : {
             "default_value" : "snappy"
            , "original_default_value" : "snappy"
            , "values": [ "brotli", "gzip", "lz4", "lz4_raw", "snappy", "uncompressed", "zstd" ]
        }
    , "csv" : {
             "default_value" : "infer"
            , "original_default_value" : "infer"
            , "values" : [ "gz", "bz2", "zip", "xz", "zst", "tar", "tar.gz", "tar.xz" or "tar.bz2" ]
        }
}



##=====================================
##  SET DECORATORS HERE
##=====================================

def timeit( func ):
    @functools.wraps( func )
    def timeit_wrapper( *args, **kwargs ):
        start_time = time.perf_counter( )
        result = func( *args, **kwargs )
        end_time = time.perf_counter( )
        total_time = end_time - start_time
        print( f"""Function {func.__name__}{args} {kwargs} took {total_time:4f} seconds""" )
        return result
    return timeit_wrapper


##NOT TESTED
def retry( max_attempts, delay = 1 ):
    def decorator( func ):
        def wrapper( *args, **kwargs ):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func( *args, **kwargs )
                except Exception as exception:
                    attempts += 1
                    raise f"""Attempt {attempts} failed: {exception}"""
                    time.sleep( delay )
        return wrapper
    return decorator



##=====================================
##  INSERT FUNCTION HERE
##=====================================

def calc_memory_usage( df ):
    '''Returns real memory usage of a DataFrame including object types'''

    memory_usage = df.memory_usage( deep = True )

    return memory_usage


def capture_py_script(
         py_script_abs_path
        , save_folderpath
        , save_filename
        , save_fileformat = "py"
    ):
    """
    py_script_abs_path : written as "os.path.abspath(__file__)" in the executing python file
    """

    ##READ CONTENT OF EXECUTING PYTHON SCRIPT
    with open( py_script_abs_path, "r" ) as read_py_file:
        script_content = read_py_file.read( )
    
    ##SAVE SCRIPT CONTENT TO OUTPUT FILE
    capture_pyscript_folder = \
        otomkdir.otomkdir.auto_create_folder(
             default_path = save_folderpath
            , folder_extend = "capture_pyscript_folder"
        )
    with open( Path( capture_pyscript_folder, f"""capture--{save_filename} {datetime.datetime.strftime( datetime.datetime.now( ), "%Y%m%d-%H%M" )}.{save_fileformat}""" ), "w" ) as output_file:
        output_file.write( script_content )


def file_backup(
         folderpath
        , filename
        , fileformat
        , keep_ori_file = True
    ):

    if os.path.exists( Path( folderpath, f"""{filename}.{fileformat}""" ) ):
        shutil.copy(
             Path( folderpath, f"""{filename}.{fileformat}""" )
            , Path( folderpath, f"""{filename} backup {datetime.datetime.strftime( datetime.datetime.now( ), "%Y%m%d-%H%M" )}.{fileformat}""" )
        )

        if keep_ori_file:
            pass
        else:
            os.remove( Path( folderpath, f"""{filename}.{fileformat}""" ) )


def get_delimiter_in_file(
         full_file_path
        , bytes = 4096
    ):
    sniffer = csv.Sniffer( )
    data = open( full_file_path, "r" ).read( bytes )
    delimiter = sniffer.sniff( data ).delimiter
    
    return delimiter


def detect_char_encode_in_file(
         full_file_path
        , bytes = 1024
    ):
    with open( full_file_path, "rb" ) as f:
        enc = chardet.detect( f.read( bytes ) )
    
    return enc


def remove_specchar(
         string
        , except_specchar_list = [ ]
    ):
    pattern = r"[^\w\s"

    ##add exceptions to pattern
    for except_char in except_specchar_list:
        pattern += re.escape( except_char )
    
    ##close pattern with ']'
    pattern += "]"

    clean_string = re.sub( pattern, "", str( string ) )
    return clean_string


def remove_letters(
         string
        , except_letters_list = [ ]
    ):
    clean_string = "".join( char for char in str( string ) if not char.isalpha( ) or char in except_letters_list )

    return clean_string


def get_folder_file_list( folder_path ):
    # List all files and folders in the specified folder
    folder_contents = os.listdir(folder_path)

    # Separate files and folders
    folder_file_dict = { }
    files = []
    folders = []

    for item in folder_contents:
        item_path = os.path.join(folder_path, item)
        if os.path.isfile(item_path):
            files.append(item)
            folder_file_dict.update( { "files" : files } )
        elif os.path.isdir(item_path):
            folders.append(item)
            folder_file_dict.update( { "folders" : folders } )
    return folder_file_dict


def isfloat( num ):
    if num is None:
        num = "None"
    try:
        float( num )
        return True
    except ValueError:
        return False


def rounding(
         value
        , decimal_place
        , round_type = "normal"
    ):
    '''
    round_type: [ None, "up", "down" ]
    '''
    if round_type.lower( ) == "up":
        multiplier = 10 ** decimal_place
        return math.ceil( value * multiplier ) / multiplier
    elif round_type.lower( ) == "down":
        multiplier = 10 ** decimal_place
        return math.floor( value * multiplier ) / multiplier
    else:
        return round( value, decimal_place )


def clean_contact_num(
         country = None
        , contact_num = None
        , is_country_short = 1
    ):
    country_list_0 = {
         "australia"        : [ "AU", "61" ]
        , "china"           : [ "CN", "86" ]
        , "indonesia"       : [ "ID", "62" ]
        , "malaysia"        : [ "MY", "60" ]
        , "philippines"     : [ "PH", "63" ]
        , "singapore"       : [ "SG", "65" ]
        , "thailand"        : [ "TH", "66" ]
        , "united kingdom"  : [ "UK", "44" ]
        , "united states"   : [ "US", "1" ]
        , "vietnam"         : [ "VN", "84" ]
    }

    contact_num = remove_specchar( contact_num, [ "." ] )

    ##CLEAN CONTACT NUMBER
    if pda.isnull( contact_num ) or contact_num == 'nan':
        contact_num = None
    elif isfloat( contact_num ):
        contact_num = str( float( contact_num ) )
        if contact_num[ -2: ] == ".0":
            contact_num = contact_num.replace( contact_num[ -2: ], "" )
        else:
            contact_num = contact_num
        contact_num = contact_num.replace( ".", "" )
    else:
        contact_num = "invalid number"


    if is_country_short == 0:
        country_list = country_list_0
    elif is_country_short == 1:
        country_list = { }
        for x in country_list_0:
            country_list[ country_list_0[ x ][ 0 ].lower( ) ] = [ country_list_0[ x ][ 0 ], country_list_0[ x ][ 1 ] ]

    if pda.isnull( country ) | pda.isnull( contact_num ):
        contact_num_clean = "no number"
    elif str( contact_num ).lower( ) == "invalid number":
        contact_num_clean = "invalid number"
    elif country.lower( ) not in( [ x for x in country_list ] ):
        contact_num_clean = "country not covered"
    else:
        country_short = country_list[ country.lower( ) ][ 0 ]
        country_code = country_list[ country.lower( ) ][ 1 ]
            
        country_code_list = [ country_list[ x ][ 1 ] for x in country_list ]
        country_code_list.remove( country_code )
        country_code_list = { }
        for x in country_list:
            country_code_list[ country_list[ x ][ 1 ] ] = country_list[ x ][ 0 ]


        # if pda.isnull( contact_num ):
        #     contact_num_clean = npy.nan
        # elif ( contact_num[ 0:2 ] != country_code ) & ( contact_num[ 0:2 ] in country_code_list ):
        #     contact_num_clean = f"""{country_code_list[ contact_num[ 0:2 ] ]};{contact_num}"""
        if country_short.lower( ) in [ "au", "cn", "id", "my", "ph", "sg", "th", "uk", "us", "vn" ]:
            if contact_num[ 0:2 ] != country_code:
                if contact_num[ 0 ] == "0":
                    contact_num_clean = f"""{country_short};{country_code}{contact_num[ 1: ]}"""
                elif contact_num[ 0:3 ] == f"""{country_code}0""":
                    contact_num_clean = f"""{country_short};{country_code}{contact_num[ 3: ]}"""
                else:
                    contact_num_clean = f"""{country_short};{country_code}{contact_num}"""
            else:
                contact_num_clean = f"""{country_short};{contact_num}"""

    return contact_num_clean


def json_prettify(
         json_data
        , indent        = 4
        , width         = 350
        , depth         = None
        , sort_dicts    = False
        , compact       = False
    ):

    json_prettified = \
        pprint.PrettyPrinter(
             indent         = indent
            , width         = width
            , depth         = depth
            , sort_dicts    = sort_dicts
            , compact       = compact
        ).pprint( json_data )
    
    return json_prettified


def pygsapi_update(
         service_account_type
        , key_filename
        , encrypted_filename
        , spreadsheet_id
        , gsheet_range
        , df_apiload = None
        , data_upload_list = None
    ):
    '''
    service_account_type    : method used to set credentials --> using either "file" or "file_content" to generate credentials
    key_filename            : filename that stores the decrypt key
    encrypted_filename      : filename that contains encrypted details
    spreadsheet_id          : ID (aka file code) of the gsheet
    gsheet_range            : specify sheetname and cell range that needs to be updated e.g. "sheet 1!D20:D30"
    df_apiload              : specify the dataframe that needs to be uploaded --> if this option is selected, "data_upload_list" is not needed
    data_upload_list        : converted dataset in list format --> if this option is selected, "df_apiload" is not needed
    '''

    ##prepare and transform dataframe for apiload
    if df_apiload is not None:
        df_apiload = df_apiload.fillna( "--None" )
        df_apiload = df_apiload.astype( str )
        df_columnlist = [ df_apiload.columns.tolist( ) ]
        df_list = df_apiload.values.tolist( )
        df_columnlist.extend( df_list )


    a = encrypt.file_decrypt( key_filename, encrypted_filename, ".json" )[ 0 ]

    scopes = [ "https://www.googleapis.com/auth/spreadsheets" ]
    key = [ service_account_type, json.loads( a ) ]    ##[ "file", "<< file_path >>" ] or [ "file_content", "<< file_content >>" ]
    if key[ 0 ] == "file":
        credentials = \
            service_account.Credentials.from_service_account_file(
                 key[ 1 ]
                , scopes = scopes
            )
    elif key[ 0 ] == "file_content":
        credentials = \
            service_account.Credentials.from_service_account_info(
                 key[ 1 ]
                , scopes = scopes
            )

    # The ID and range of a sample spreadsheet.
    spreadsheet_id = spreadsheet_id
    service = build( "sheets", "v4", credentials = credentials )
    sheet = service.spreadsheets( )

    ##CLEAR SHEET
    clear_gsheet = \
        sheet.values( ).clear(
            spreadsheetId = spreadsheet_id
            , range = gsheet_range.split( "!" )[ 0 ]
            , body = { }
        ).execute( )

    ##WRITE SHEET
    write_gsheet = \
        sheet.values( ).update(
            spreadsheetId = spreadsheet_id
            , range = gsheet_range
            , valueInputOption = "USER_ENTERED"
            , body = { "values" : df_columnlist if df_apiload is not None else data_upload_list }
        ).execute( )


def get_random_useragent( ):
    software_names = [ SoftwareName.CHROME.value ]
    operating_systems = [ OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value ]
    user_agent_rotator = UserAgent( software_names = software_names, operating_systems = operating_systems, limit = 100 )
    user_agent = user_agent_rotator.get_user_agents( )
    user_agent = user_agent_rotator.get_random_user_agent( )

    return user_agent


def get_random_managed_headers(
         scrapeops_apikey
    ):
    ##scrapeops_apikey = Courtesy of 'https://scrapeops.io'

    output_type = "browser-headers"     ##{"options" : [ "browser-headers", "user-agents"] }
    url_link = f"""https://headers.scrapeops.io/v1/{output_type}"""
    num_headers = 1

    r = \
        requests.get(
            url = url_link
            , params = {
                 "api_key"          : scrapeops_apikey
                , "num_headers"     : num_headers
            }
        )

    response_list = r.json( )[ "result" ]
    items_in_list = len( response_list )
    sequenced_list = [ x for x in range( 0, items_in_list ) ]
    get_random_position = random.sample( sequenced_list, 1 )[ 0 ]

    return response_list[ get_random_position ]


def enhance_read_csv(
         full_filepath
        , header_names = None
        , separator = ","
        , header_row_index = "infer"
    ):

    get_encoding = detect_char_encode_in_file( full_filepath )[ "encoding" ]

    default_encoding_list = [ "utf-8", "latin1", "iso-8859-1", "cp1252" ]
    encoding_list = [ get_encoding ] + default_encoding_list

    for encoding in encoding_list:
        try:
            if header_names is not None:
                df = pda.read_csv( full_filepath, sep = separator, encoding = encoding, header = 0, names = header_names, on_bad_lines = "warn" )
            else:
                df = pda.read_csv( full_filepath, sep = separator, encoding = encoding, header = header_row_index, on_bad_lines = "warn" )
            break
        except:
            continue
    return df


def glob_compile_list(
         scan_folder_path = None
        , scan_file_regex_string = None
        , scan_file_format = None
    ):

    file_list = \
        glob.glob(
            str(
                Path(
                     scan_folder_path
                    , f"""{scan_file_regex_string}.{scan_file_format}"""
                )
            )
        )
    
    return file_list


def glob_compile_df(
         file_list = None
        , tag_batch = True
        , header_mapping = None
        , header_names = None
        , separator = ","
        , header_row_index = "infer"
        , save_folder_path = None
        , save_compiled_filename = None
    ):
    ##file_list : refer to "glob_compile_list" function

    for idx, file in enumerate( file_list ):
        filename = file.split( "\\" )[ -1 ].split( "." )[ 0 ]

        # get_encoding = detect_char_encode_in_file( file )[ "encoding" ]

        df_0 = enhance_read_csv( file, header_names = header_names, separator = separator, header_row_index = header_row_index )
        # if header_names is not None:
        #     df_0 = pda.read_csv( file, sep = separator, encoding = get_encoding, header = 0, names = header_names )
        # else:
        #     df_0 = pda.read_csv( file, sep = separator, encoding = get_encoding, header = header_row_index )

        if tag_batch is True:
            df_0.loc[ :, "filename" ] = filename
        else:
            pass
        
        if header_mapping is not None:
            df_0 = df_0.rename( columns = header_mapping )
        else:
            pass

        if idx == 0:
            df = df_0
        else:
            df_append = df_0
            df = pda.concat( [ df, df_append ], ignore_index = True )
    if save_folder_path is not None and save_compiled_filename is not None:
        df.to_csv( Path( save_folder_path, f"""{save_compiled_filename}.csv""" ), index = False )
    else:
        pass

    return df


def readchunk_csv(
         full_file_path
        , rows_per_chunk = 5000000
        , get_samplerows = False
        , dtype = None
        , **kwargs
    ):
    with pda.read_csv( full_file_path, chunksize = rows_per_chunk if get_samplerows is False else rows_per_chunk / 10, dtype = dtype, **kwargs ) as df_chunks:
        start_end_time_list = [ ]
        start_end_time_list_cols = [
             "index"
            , "start_timestamp"
            , "end_timestamp"
        ]
        if get_samplerows is True:
            for idx, chunk in enumerate( df_chunks ):
                start_time = datetime.datetime.now( )
                print( f""" Processing index {idx}: {datetime.datetime.now( )}""" )
                if idx == 0:
                    df = chunk.copy( )
                elif idx in list( x for x in range( 1, 4 ) ):
                    df_append = chunk.copy( )
                    df = pda.concat( [ df, df_append ], ignore_index = True )
                else:
                    break
                end_time = datetime.datetime.now( )
                start_end_time_list.append( [ idx, start_time, end_time ] )
        else:
            for idx, chunk in enumerate( df_chunks ):
                start_time = datetime.datetime.now( )
                print( f""" Processing index {idx}: {datetime.datetime.now( )}""" )
                if idx == 0:
                    df = chunk.copy( )
                else:
                    df_append = chunk.copy( )
                    df = pda.concat( [ df, df_append ], ignore_index = True )
                # else:
                #     break
                end_time = datetime.datetime.now( )
                start_end_time_list.append( [ idx, start_time, end_time ] )

        df_duration_analysis = pda.DataFrame( start_end_time_list, columns = start_end_time_list_cols )
        df_duration_analysis.loc[ :, "duration" ] = ( df_duration_analysis[ "end_timestamp" ] - df_duration_analysis[ "start_timestamp" ] ).dt.total_seconds( )
        duration_list = df_duration_analysis[ "duration" ].values.tolist( )
        average_duration = sum( duration_list ) / len( duration_list )

    return df


def create_calendar_df( start_date, end_date ):
    '''
    Create Dimension Date in Pandas
    '''
    from pandas.tseries.offsets import MonthBegin, MonthEnd, QuarterBegin, QuarterEnd

    # Construct DIM Date Dataframe
    df_date = pda.DataFrame( { "calendar_date": pda.date_range( start =f'{start_date}', end=f'{end_date}', freq='D' ) } )

    def get_start_of_month( pd_date ):
        if pd_date.is_month_start == True:
            return pd_date
        else:
            return pd_date - MonthBegin( 1 )

    def get_end_of_month( pd_date ):
        if pd_date.is_month_end == True:
            return pd_date
        else:
            return pd_date + MonthEnd( 1 )

    def get_end_of_quarter( pd_date ):
        if pd_date.is_quarter_end == True:
            return pd_date
        else:
            return pd_date + QuarterEnd( 1 )

    # Add in attributes
    df_date[ "day_name" ] = df_date[ "calendar_date" ].apply( lambda x : datetime.datetime.strftime( x, "%a" ) )
    df_date[ "day" ] = df_date[ "calendar_date" ].apply( lambda x : datetime.datetime.strftime( x, "%d" ) )
    df_date[ "week" ] = df_date[ "calendar_date" ].apply( lambda x : datetime.datetime.strftime( x, "%W" ) )
    df_date[ "month" ] = df_date[ "calendar_date" ].apply( lambda x : datetime.datetime.strftime( x, "%m" ) )
    df_date[ "quarter" ] = df_date[ "calendar_date" ].dt.to_period( "Q" ).dt.strftime( "Q%q" )
    df_date[ "year_month" ] = df_date[ "calendar_date" ].apply( lambda x : datetime.datetime.strftime( x, "%Y-%m" ) )
    df_date[ "year_quarter" ] = df_date[ "calendar_date" ].dt.to_period( "Q" )
    df_date[ "year" ] = df_date[ "calendar_date" ].apply( lambda x : datetime.datetime.strftime( x, "%Y" ) )
    df_date[ "fiscal_year_quarter" ] = df_date[ "calendar_date" ].dt.to_period( "q-dec" )    ## "q-dec" = last quarter end in December
    df_date[ "fiscal_year" ] = df_date[ "calendar_date" ].dt.to_period( "a-dec" )            ## "a-dec" = year end in December
    df_date[ "start_of_month" ] = df_date[ "calendar_date" ].apply( get_start_of_month )
    df_date[ "end_of_month" ] = df_date[ "calendar_date" ].apply( get_end_of_month )
    df_date[ "is_eom" ] = df_date[ "calendar_date" ].dt.is_month_end
    df_date[ "end_of_quarter" ] = df_date[ "calendar_date" ].apply( get_end_of_quarter )
    df_date[ "is_eoq" ] = df_date[ "calendar_date" ].dt.is_quarter_end

    return df_date


class percentile:
    def __init__( self, q ):
        self.q = q
    def __call__( self, x, y = "midpoint" ):
        return x.quantile( self.q, y )
        # return npy.quantile( x.dropna( ), self.q )


def dsk_tunique( ):
    ##GROUPBY AGGREGATION FUNCTION FOR DASK DATAFRAME -- TO CALCULATE THE DISTINCT COUNT OF VALUES
    def chunk( s ):
        '''
        The function applied to the individual partition (map)
        '''
        return s.apply( lambda x : list( set( x ) ) )
    def agg( s ):
        '''
        The function which will aggregate the result from all the partitions (reduce)
        '''
        s = s._selected_obj
        return s.groupby( level = list( range( s.index.nlevels ) ) ).sum( )
    def finalise( s ):
        '''
        The optional function that will be applied to the result of the agg_tu functions
        '''
        return s.apply( lambda x : len( set( x ) ) )
    tunique = dsk.Aggregation( "tunique", chunk, agg, finalise )
    '''
    e.g.
    dsk_groupby = \
        dsk_df.groupby(
            [
                "col1"
                , "col2"
            ]
            , dropna = False
        ).agg(
            uniq_count = ( "col3", dsk_tunique( ) )
        )
    '''
    return tunique


def split_period_list(
         start_on_date
        , end_before_date
        , split_num_periods
    ):
    start_on_date = start_on_date
    end_before_date = end_before_date
    split_num_periods = split_num_periods
    days_span = ( datetime.datetime.strptime( end_before_date, "%Y-%m-%d" ) - datetime.datetime.strptime( start_on_date, "%Y-%m-%d" ) ).days
    every_nth_period = int( days_span / split_num_periods )
    date_list_0 = pda.date_range(
         start = start_on_date
        , end = end_before_date
        , freq = f"""{every_nth_period}D"""
    )
    date_list = [ x.strftime( "%Y-%m-%d" ) for x in date_list_0 ]
    if date_list[ -1 ] != end_before_date:
        date_list.append( end_before_date )
    
    return date_list


def add_minutes(
         timestamp
        , minutes
    ):
    '''
    timestamp : timestamp format
    minutes : include minus (-) sign if want to subtract
    '''
    x = timestamp + datetime.timedelta( minutes = minutes )
    return x


def gsheet_download(
         file_name
        , sheet_name
        , url_file_codename
        , url_sheet_codename
        , url_cell_range
        , save_folder
        , save_filename
    ):
    default_user = os.environ[ "USERPROFILE" ]
    download_folder = Path( default_user, "Downloads" )

    gdocs_default = "https://docs.google.com/spreadsheets/d/"

    gsheet_url = gdocs_default + url_file_codename + "/export?format=csv&gid=" + url_sheet_codename + "&range=" + url_cell_range
    download_file = Path( download_folder, file_name + " - " + sheet_name + ".csv" )
    save_file = Path( save_folder, save_filename + ".csv" )
    try:
        webbrowser.open( gsheet_url, new = 0 )

        ##check if file is successfully downloaded
        x = 0
        while os.path.exists( download_file ) == False:
            x += 1
            time.sleep( 1 )
            if x == 300:
                break
            print( "elapsed seconds : " + str( x ) )
        else:
            print( "file found" )

        time.sleep( 7 )

    except Exception as exception:
        print( exception )

    time.sleep( 0.5 )


    shutil.move(
         download_file
        , save_file
    )

    df = pda.read_csv( save_file )
    df.loc[ :, "downloaded_at" ] = datetime.datetime.now( )

    return df


def postgres_query(
     assign_query_name
    , sql_query
    , save_folder_path_str = None
    , credentials = None
    ):
    # credentials = [ "username", "password", "hostname", "port", "databasename" ]

    engine = \
        sa.create_engine(
             f"postgresql+psycopg2://{credentials[ 0 ]}:{credentials[ 1 ]}@{credentials[ 2 ]}:{credentials[ 3 ]}/{credentials[ 4 ]}"
        )
    with engine.connect( ) as conn:
        df = pda.read_sql( sa.text( sql_query ), conn )
    
    if save_folder_path_str == None:
        pass
    else:
        df.to_csv( Path( save_folder_path_str, assign_query_name + ".csv" ), index = False )

    return df


def load_to_postgres(
     table_name
    , dataframe = None
    , csv_source_file_path = None
    , credentials = None
    ):
    # credentials = [ "username", "password", "hostname", "port", "databasename", "schema" ]

    try:
        connection = \
             f"postgresql+psycopg2://{credentials[ 0 ]}:{credentials[ 1 ]}@{credentials[ 2 ]}:{credentials[ 3 ]}/{credentials[ 4 ]}"
        print( "connection set" )
    except Exception as error:
        print( "connection error: " + str( error ) )
    
    if isinstance( dataframe, pda.DataFrame ):
        df = dataframe
    elif csv_source_file_path != None:
        df = pda.read_csv( csv_source_file_path )

    df.loc[ :, "upload_at" ] = datetime.datetime.now( )

    try:
        df.to_sql(
             name = table_name
            , con = connection
            , schema = credentials[ 5 ]
            , if_exists = "replace"
            , index = False
            , chunksize = 10000
            , method = "multi"
        )
    except Exception as error:
        print( "db loading error: " + str( error ) )


def sqlite3_connect(
         db_name
        , db_folderpath
        , timeout = 120
    ):
    '''
    connect to sqlite
    '''

    connection = sqlite3.connect( Path( db_folderpath, db_name + ".db" ), timeout = timeout )
    return connection


def sql_table_query(
         table_name = None
        , connection = None
        , query = None
        , is_table_snapshot = "Yes"
        , snapshot_column = "downloaded_at"
    ):
    
    ##GET RELEVANT TABLES
    if query is not None:
        table_name = None
        query = query
    else:
        table_name = table_name
        if is_table_snapshot.lower( ) == "yes":
            query = f"""
                select
                    r1.*
                from
                    {table_name} r1
                where
                    r1.{snapshot_column} = ( select max( {snapshot_column} ) from {table_name} )
            """
        elif is_table_snapshot.lower( ) == "no":
            query = f"""
                select
                    r1.*
                from
                    {table_name} r1
            """

    df = pda.read_sql( query, connection )
    
    return df


def upload_to_sqlite3(
         df
        , table_name
        , db_name
        , db_folderpath
        , overwrite = "Yes"
    ):
    df_colnum = df.shape[ 1 ]
    q_mark = "?"
    q_marks = ",".join( q_mark * df_colnum )

    df_columns = list( df.columns )
    df_col_str = " text,".join( [ str( col ) for col in df_columns ] ) + " text"

    rows = df.values.tolist( )

    connection = sqlite3_connect( db_name, db_folderpath )
    cursor = connection.cursor( )

    if overwrite.lower( ) == "yes":
        cursor.execute(
            "drop table if exists {table_name}".\
                format(
                    table_name = table_name
                )
        )
    elif overwrite.lower( ) == "no":
        pass

    cursor.execute(
        "create table if not exists {table_name}( {column_names} )".\
            format(
                table_name = table_name
                , column_names = df_col_str
            )
    )

    cursor.executemany(
        "insert into {table_name} values( {q_marks} )".\
            format(
                table_name = table_name
                , q_marks = q_marks
            )
        , rows
    )

    connection.commit( )
    connection.close( )


def new_upload_to_sqlite3(
         df
        , table_name
        , db_name
        , db_folderpath
        , overwrite = "Yes"
    ):
    df = df.rename( columns = { "index" : "idx" } )
    df_columns = list( df.columns )

    connection = sqlite3_connect( db_name, db_folderpath )
    cursor = connection.cursor( )

    if overwrite.lower( ) == "yes":
        cursor.execute(
            "drop table if exists {table_name}".\
                format(
                     table_name = table_name
                )
        )
    elif overwrite.lower( ) == "no":
        ##CHECK EXISTING TABLE INTEGRITY
        df_chunks= \
            pda.read_sql(
                 f"""select * from {table_name}"""
                , connection
                , chunksize = 5
            )
        for idx, chunk in enumerate( df_chunks ):
            if idx == 0:
                df_col_check = chunk.copy( )
            else:
                break
        
        exist_table_cols = list( df_col_check.columns )

        new_cols = [ ]
        for col in df_columns:
            if col in exist_table_cols:
                pass
            else:
                new_cols.append( col )
        
        if exist_table_cols == [ ]:
            pass
        else:
            for new_col in new_cols:
                cursor.execute( f"""alter table {table_name} add column {new_col} text""" )
        exist_table_cols.extend( new_cols )

        ##CONFORMING DATAFRAME COLUMNS
        new_dfcols = [ ]
        for col in exist_table_cols:
            if col in df_columns:
                pass
            else:
                new_dfcols.append( col )
        for newcol in new_dfcols:
            df.loc[ :, newcol ] = npy.nan
        df = df[ exist_table_cols ]
    df = df.astype( str )


    df_colnum = df.shape[ 1 ]
    q_mark = "?"
    q_marks = ",".join( q_mark * df_colnum )

    df_columns = list( df.columns )
    df_col_str = " text, ".join( [ str( col ) for col in df_columns ] ) + " text"

    rows = df.values.tolist( )
    cursor.execute(
        "create table if not exists {table_name}( {column_names} )".\
            format(
                 table_name = table_name
                , column_names = df_col_str
            )
    )

    cursor.executemany(
        "insert into {table_name} values( {q_marks} )".\
            format(
                 table_name = table_name
                , q_marks = q_marks
            )
        , rows
    )

    connection.commit( )
    connection.close( )


def db_query_todask(
         db_table_name
        , connection_str_uri
        , partition_column
        , npartitions = 1
        , query = None
        , snapshot_column_name = None
        , save_folderpath = None
    ):
    ##table_name : { "description": "database table_name and will be used as the filename if save_folderpath is activated" }
    ##connection_str_uri : { "description": "set connection to database in full string format", "values": "sqlite:///path_to_sqlite_db.db" }
    ##partition_column : { "description": "specify column name for partitioning" }
    ##npartitions : { "description": "specify desired number of partitions (if applicable)" }
    ##query : { "description": "query script (if applicable)" }
    ##snapshot_column_name : { "description": "column name that represents timestamp snapshots", "options": [ None, "column_name" ] }
    ##save_folderpath : { "description": "folder path to store query results", "options": [ None, "filepath" ] }

    table_name = db_table_name.split( "." )[ -1 ]

    ##GET RELEVANT TABLES
    if query is None:
        if snapshot_column_name is not None:
            sql_query = f"""
                select
                    r1.*
                from
                    {db_table_name} r1
                where
                    r1.{snapshot_column_name} = ( select max( {snapshot_column_name} ) from {db_table_name} )
            """
        elif snapshot_column_name is None:
            sql_query = f"""
                select
                    r1.*
                from
                    {db_table_name} r1
            """
    else:
        sql_query = query
    
    sql_query = sql_query.replace( "select", "" )

    idx_col = pda.read_sql( f"""select {sql_query}""", connection_str_uri )
    idx_col = sorted( idx_col[ partition_column ].unique( ).tolist( ) )
    nth_num = int( len( idx_col ) / npartitions )
    partition_list = idx_col[ ::nth_num ]

    if "where" in query.lower( ):
        pda_df = pda.read_sql( f"""select {sql_query}""", connection_str_uri )
        dsk_df = dsk.from_pandas( pda_df, npartitions = npartitions )
    else:
        dsk_df = \
            dsk.read_sql_query(
                 sql = sa.sql.select( sa.sql.text( sql_query ) )
                , con = connection_str_uri
                , index_col = partition_column
                , divisions = partition_list
            )

    if save_folderpath is not None:
        save_path = Path( save_folderpath, f"""query_{table_name}""" )
        if os.path.exists( save_path ):
            os.remove( save_path )
        else:
            pass
        dsk_df.to_parquet( save_path )
    else:
        pass
    
    return dsk_df


def dsk_read_messy_data(
         filepath
        , dtype
    ):
    '''
    ##filepath : self-explanatory
    ##dtype : explicitly set data type mapping for all columns, e.g. dtype = { "id" : "object", "name" : "object", "x" : "object", "y" : "object" }
    
    read "https://examples.dask.org/dataframes/04-reading-messy-data-into-dataframes.html?highlight=meta"
    to be used in subsequent steps as follows:
    from dask import delayed
    import glob
    import sweep.swiip as swp

    dtype_mapping = {
         "id"       : "object"
        , "name"    : "object"
        , "x"       : "object"
        , "y"       : "object"
    }

    files = glob.glob( filepath_keyword_with_extension )    ##e.g. glob.glob( str( Path( folder, f"""{filename} *.csv""" ) ) )

    dsk_read_messy_data = dask.delayed( swp.dsk_read_messy_data )
    pda_df = [ dsk_read_messy_data( filepath = file, dtype = dtype_mapping ) for file in files ]
    dsk_df = dsk.from_delayed( pda_df, meta = dtype_mapping, verify_meta = False ).persist( )
    '''


    try:
        ##try reading in the data with pandas
        dsk_df = dsk.read_csv( filepath, dtype = dtype )
        pda_df = dsk_df.compute( )
    except:
        ##if this fails create an empty pandas dataframe with the same dtypes as the good data
        pda_df = pda.read_csv( io.StringIO( "" ), names = dtype.keys( ), dtype = dtype )
    
    ##for the case with the missing column, add a column of data with NaN's
    for col in list( dtype.keys( ) ):
        if col not in pda_df.columns:
            pda_df[ col ] = npy.NaN
    
    return pda_df


def dsk_df_read_from_format(
         read_folderpath
        , read_filename
        , read_format
        , npartitions = None
        , dtype_mapping = None
        , parse_dates = None
        , header_mapping = None
        , header_names = None
        , header_row_index = "infer"
        , compression_method = "infer"
    ):

    file_list = \
        glob_compile_list(
             scan_folder_path = read_folderpath
            , scan_file_regex_string = read_filename
            , scan_file_format = read_format
        )

    npartitions = 5 if npartitions is None else npartitions
    read_format_list = read_format if "list" in str( type( read_format ) ) else read_format.replace( "_", "" ).replace( "normalise", "" ).replace( "normalize", "" ).split( ";" )
    compression_method = compression_dict[ read_format_list[ 0 ] ][ "default_value" ] if compression_method is None else compression_method

    for idx, file in enumerate( file_list ):
        filename = file.split( "\\" )[ -1 ].split( "." )[ 0 ]

        if "csv" in read_format:
            pda_df = \
                pda.read_csv(
                     file
                    , header = header_row_index
                    , dtype = dtype_mapping
                    , parse_dates = parse_dates
                    , compression = compression_method
                )
            dsk_df_0 = dsk.from_pandas( pda_df, npartitions = npartitions )
        # elif "json" in read_format:
        #     if "normalise" in read_format or "normalize" in read_format:
        #         pda_df = \
        #             pda.json_normalize(
        #                  file
        #             )
        #     else:
        #         pda_df = \
        #             pda.read_json(
        #                  file
        #                 , dtype = dtype_mapping
        #                 , orient = None if len( read_format_list ) <= 1 else read_format_list[ 1 ]
        #                 , convert_dates = True
        #                 , compression = compression_method
        #             )
        #     dsk_df_0 = dsk.from_pandas( pda_df, npartitions = npartitions )
        elif "parquet" in read_format:
            dsk_df_0 = \
                dsk.read_parquet(
                     file
                    , dtype = dtype_mapping
                )
        elif "part" in read_format:
            dsk_df_0 = \
                dsk.read_csv(
                     file
                    , dtype = dtype_mapping
                )


        if header_names is not None:
            dsk_df_0.columns = header_names
        elif header_mapping is not None:
            dsk_df_0 = dsk_df_0.rename( columns = header_mapping )
        else:
            pass

        dsk_df_0[ "filename" ] = filename

        if idx == 0:
            dsk_df = dsk_df_0
        else:
            dsk_df_append = dsk_df_0
            dsk_df = \
                dsk.concat( [ dsk_df, dsk_df_append ], ignore_index = True )


    dsk_df = dsk_df.repartition( npartitions = npartitions )

    return dsk_df


def df_save_to_format(
         df
        , dataframe_type
        , save_folderpath
        , save_filename
        , save_format = "parquet"
        , compression_method = None
        , schema = None
    ):
    # df = { "description" : "dataframe that needs to be saved" }
    # dataframe_type = { "values" : [ "pandas", "dask" ] }
    # save_format = { "default_value" : "parquet", "values" : [ "csv", "parquet", "json" ] }
    # compression = {
    #      "default_value" : None
    #     , "values" : "refer to compression_dict"
    # }
    # schema = {
    #      "description" : "schemapyarrow.Schema, dict, 'infer', or None, default 'infer'"
    #     , "values" : f"""{"field" : "pa.string( )"}"""
    # }

    save_folderpath_filename = Path( save_folderpath, save_filename )
    save_format_list = save_format if "list" in str( type( save_format ) ) else save_format.split( ";" )
    compression_method = compression_dict[ save_format_list[ 0 ] ][ "default_value" ] if compression_method is None else compression_method

    if dataframe_type == "pandas":
        dsk_df = \
            dsk.from_pandas(
                 df
                , npartitions = 5 if len( save_format_list ) <= 1 else int( save_format_list[ 1 ] )
            )
    elif dataframe_type == "dask":
        dsk_df = df

    if "csv" in save_format:
        dsk_df.to_csv(
             Path( f"""{save_folderpath_filename}""" )
            , index = False
            , compression = compression_method
            , mode = 'wt'   ##overwrite files
        )
    # elif "json" in save_format:
    #     dsk_df.to_json(
    #          Path( f"""{save_folderpath_filename}""" )
    #         , orient = "records" if len( save_format_list ) <= 1 else save_format_list[ 1 ]
    #         , compression = compression_method
    #     )
    elif "parquet" in save_format:
        dsk_df.to_parquet(
             Path( f"""{save_folderpath_filename}""" )
            , compression = compression_method
            , overwrite = True  ##overwrite files
            , schema = schema
        )


def check_df_inconsistent_dtype(
         standard_dtype_dict
        , check_df_dict
    ):
    '''
    standard_dtype_dict : specify column names that need to be standardised in dictionary format
    check_df_dict       : label each dataframe in a dictionary format, e.g. { "latest_data" : <<dataframe name>>, "previous_data" : <<dataframe name>> }
    '''

    ##CHECKING FOR DATA TYPE CONSISTENCY
    pda_df_standard_dtypes_main = pda.DataFrame( [ { "column_name" : i, "standard_data_type" : standard_dtype_dict[ i ] } for i in standard_dtype_dict ] )
    compile_dtype_check_errors = { }
    for check_df in check_df_dict:
        pda_df_standard_dtypes = pda_df_standard_dtypes_main.copy( )

        check_df_dtype_dict = check_df_dict[ check_df ].dtypes.to_dict( )
        pda_df_check_df = pda.DataFrame( [ { "column_name" : i, f"{check_df}_data_type" : check_df_dtype_dict[ i ] } for i in check_df_dtype_dict ] )

        new_col_list = [ ]
        for check_df_col in pda_df_check_df[ "column_name" ].tolist( ):
            if check_df_col not in pda_df_check_df[ "column_name" ].tolist( ):
                new_col_list.append( check_df_col )

        if new_col_list != [ ]:    
            compile_dtype_check_errors.update(
                { f"new_columns_in_{check_df}" : new_col_list }
            )

        pda_df_standard_dtypes = \
            pda.merge(
                 pda_df_standard_dtypes
                , pda_df_check_df
                , how = "inner"
                , on = "column_name"
                , suffixes = ( "", "_x" )
                , validate = "1:1"
                , indicator = False
            )
        pda_df_inconsistent_dtypes = \
            pda_df_standard_dtypes[
                 ( pda_df_standard_dtypes[ "standard_data_type" ] != pda_df_standard_dtypes[ f"{check_df}_data_type" ] )
            ]

        if len( pda_df_inconsistent_dtypes ) > 0:
            compile_dtype_check_errors.update(
                { f"diff_dtype_in_{check_df}" : pda_df_inconsistent_dtypes.to_dict( orient = "records" ) }
            )

    return { "error": compile_dtype_check_errors } if compile_dtype_check_errors != { } else "Awesome !! No errors in data type consistency !!"


def streamlit_filter_dataframe( df: pda.DataFrame ) -> pda.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pda.DataFrame): Original dataframe

    Returns:
        pda.DataFrame: Filtered dataframe
    """
    modify = strmlt.checkbox("Add filters")

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype( df[ col ] ):
            try:
                df[ col ] = pda.to_datetime( df[ col ] )
            except Exception:
                pass

        if is_datetime64_any_dtype( df[ col ] ):
            df[ col ] = df[ col ].dt.tz_localize( None )

    modification_container = strmlt.container( )

    with modification_container:
        to_filter_columns = strmlt.multiselect( "Filter dataframe on", df.columns )
        for column in to_filter_columns:
            left, right = strmlt.columns( ( 1, 20 ) )
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype( df[ column ] ) or df[ column ].nunique( ) < 10:
                user_cat_input = right.multiselect(
                     f"Values for {column}"
                    , sorted( df[ column ].unique( ).tolist( ) )
                    # , default = list( df[ column ].unique( ) )
                )
                df = df[ df[ column ].isin( user_cat_input ) ]
            elif is_numeric_dtype( df[ column ] ):
                _min = float( df[ column ].min( ) )
                _max = float( df[ column ].max( ) )
                step = ( _max - _min ) / 100
                user_num_input = right.slider(
                     f"Values for {column}"
                    , min_value = _min
                    , max_value = _max
                    , value = ( _min, _max )
                    , step = step
                )
                df = df[ df[ column ].between( *user_num_input ) ]
            elif is_datetime64_any_dtype( df[ column ] ):
                user_date_input = right.date_input(
                     f"Values for {column}"
                    , value = (
                         df[ column ].min( )
                        , df[ column ].max( )
                      )
                )
                if len( user_date_input ) == 2:
                    user_date_input = tuple( map( pda.to_datetime, user_date_input ) )
                    start_date, end_date = user_date_input
                    df = df.loc[ df[ column ].between( start_date, end_date ) ]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}"
                )
                if user_text_input:
                    df = df[ df[ column ].str.lower( ).str.contains( user_text_input.lower( ) ) ]

    return df


def accrual_accounting_allocation_by_months(
         df
        , primary_key_column_name
        , start_date_column_name
        , end_date_column_name
        , premature_end_date_column_name
        , accrue_value_list_column_names
    ):

    ##allocation_by_months
    pk_id_list = sorted( df[ primary_key_column_name ].unique( ).tolist( ) )

    compile_accrual = [ ]
    compile_accrual_cols = [
         primary_key_column_name
        , "fiscal_period_end"
        , "num_of_days"
    ]
    compile_accrual_cols.extend( accrue_value_list_column_names )

    for pk_id in pk_id_list:
        df_accrual = df[ df[ primary_key_column_name ] == pk_id ]
        start_on_date = df_accrual[ start_date_column_name ].values.tolist( )[ 0 ]
        end_on_date = df_accrual[ end_date_column_name ].values.tolist( )[ 0 ]
        period_days = ( datetime.datetime.strptime( end_on_date, "%Y-%m-%d" ) - datetime.datetime.strptime( start_on_date, "%Y-%m-%d" ) ).days + 1
        
        accrue_value_columns_dict = { }
        for accrue_value_col in accrue_value_list_column_names:
            accrue_value_columns_dict.update(
                { accrue_value_col : df_accrual[ accrue_value_col ].values.tolist( )[ 0 ] }
            )

        date_list_0 = \
            pda.date_range(
                start = start_on_date
                , end = end_on_date
                , freq = "M"
            )
        date_list = [ x.strftime( "%Y-%m-%d" ) for x in date_list_0 ]
        if date_list[ 0 ] != start_on_date:
            date_list.insert( 0, start_on_date )
        if date_list[ -1 ] != end_on_date:
            date_list.append( end_on_date )
        
        x = 0
        y = 1
        for idx, dates in enumerate( date_list ):
            if idx == 0:
                pass
            else:
                period_list = date_list[ x : y + 1 ]
                num_of_days_0 = ( datetime.datetime.strptime( period_list[ 1 ], "%Y-%m-%d" ) - datetime.datetime.strptime( period_list[ 0 ], "%Y-%m-%d" ) ).days
                num_of_days = num_of_days_0 + 1 if idx == 1 else num_of_days_0
                calculation = [
                    pk_id
                    , period_list[ 1 ]
                    , num_of_days
                ]
                calculation.extend( [ ( num_of_days / period_days * x ) for x in accrue_value_columns_dict.values( ) ] )
                compile_accrual.append( calculation )

                x += 1
                y += 1
    df_compile_accrual_wip = pda.DataFrame( compile_accrual, columns = compile_accrual_cols )


    ##accounting_for_premature_end
    df_accrual_premature_end_wip = df.copy( )
    df_accrual_premature_end_wip = \
        df_accrual_premature_end_wip[
             ~( df_accrual_premature_end_wip[ premature_end_date_column_name ].isnull( ) )
        ]
    df_accrual_premature_end = \
        df_accrual_premature_end_wip[
             [
                 primary_key_column_name
                , premature_end_date_column_name
             ]
        ]
    
    df_compile_accrued_revenue_stg = \
        pda.merge(
             df_compile_accrual_wip
            , df_accrual_premature_end
            , how = "left"
            , left_on = [ primary_key_column_name ]
            , right_on = [ primary_key_column_name ]
            , suffixes = ( "", "_x" )
            , validate = "m:1"
            , indicator = False
        )
    df_compile_accrued_revenue_stg[ "fiscal_period_end_check" ] = \
        df_compile_accrued_revenue_stg.apply(
             lambda row:
                 row[ "fiscal_period_end" ] if pda.isnull( row[ premature_end_date_column_name ] )
                else row[ "fiscal_period_end" ] if datetime.datetime.strftime( pda.to_datetime( row[ "fiscal_period_end" ] ), "%Y-%m" ) <= datetime.datetime.strftime( pda.to_datetime( row[ premature_end_date_column_name ] ), "%Y-%m" )
                else datetime.datetime.strftime( pda.to_datetime( row[ premature_end_date_column_name ] ) + relativedelta( day = 31 ), "%Y-%m-%d" )
            , axis = 1
        )

    return df_compile_accrued_revenue_stg


def random_float(
         min_range = 0
        , max_range = 1
        , return_lowerbound = None
        , return_upperbound = None
    ):
    list_range = [ x for x in range( min_range, max_range ) ]
    random_float_num = random.sample( list_range, 1 )[ 0 ] + random.random( )
    
    if return_lowerbound is None:
        random_float_num = random_float_num
    else:
        random_float_num = random_float_num if random_float_num >= return_lowerbound else return_lowerbound

    if return_upperbound is None:
        random_float_num = random_float_num
    else:
        random_float_num = random_float_num if random_float_num <= return_upperbound else return_upperbound

    return random_float_num


def df_splitjson_tocolumns(
         df
        , splitjson_column_name
    ):
    df_filter_stg = \
        df[ df[ splitjson_column_name ].isnull( ) ]
    df_filter_tojson = df[ ~df[ splitjson_column_name ].isnull( ) ]
    df_filter_tojson = \
        df_filter_tojson.join(
             df_filter_tojson[ splitjson_column_name ].apply(
                 lambda x:
                     pda.Series( json.loads( str( x ) ) )
             )
        )
    if df_filter_stg.empty:
        df_filter_final = df_filter_tojson
    else:
        df_filter_final = pda.concat( [ df_filter_stg, df_filter_tojson ], ignore_index = True )
    
    return df_filter_final


# def calculate_fuzzy_scores_v0(
#          lookup_string
#         , master_string
#         , cutoff = 75
#     ):
#     """
#     Calculate all the fuzzy matching scores between lookup_string and master_string.
#     """
#     lookup_string = lookup_string.lower( ) if lookup_string is not None else ""

#     master_string_data_type = str( type( master_string ) ).replace( "<class '", "" ).replace( "'>" , "" ).lower( )

#     if isinstance( master_string, type( "asdf" ) ) or isinstance( master_string, type( None ) ):
#         master_string_list = [ master_string.lower( ) if master_string is not None else "" ]
#     elif isinstance( master_string, type( {} ) ):
#         master_string_list = [ list( master_string.keys( ) )[ 0 ] ]
#         master_string_list.extend( master_string[ master_string_list[ 0 ] ] )

#     scores = { }
#     for idx, version in enumerate( master_string_list ):
#         if idx == 0:
#             scores.update(
#                 {
#                      "fuzz_ratio"                     : rapidfuzz.fuzz.ratio( lookup_string, version, score_cutoff = cutoff )
#                     , "fuzz_partial_ratio"            : rapidfuzz.fuzz.partial_ratio( lookup_string, version, score_cutoff = 90 ) if rapidfuzz.fuzz.partial_ratio( lookup_string, version, score_cutoff = 90 ) < 100 else 0
#                     # , "fuzz_partial_token_ratio"      : rapidfuzz.fuzz.partial_token_ratio( lookup_string, version, score_cutoff = cutoff )
#                     # , "fuzz_partial_token_set_ratio"  : rapidfuzz.fuzz.partial_token_set_ratio( lookup_string, version, score_cutoff = cutoff )
#                     # , "fuzz_partial_token_sort_ratio" : rapidfuzz.fuzz.partial_token_sort_ratio( lookup_string, version, score_cutoff = cutoff )
#                     , "fuzz_token_ratio"              : rapidfuzz.fuzz.token_ratio( lookup_string, version, score_cutoff = cutoff )
#                     , "fuzz_token_set_ratio"          : rapidfuzz.fuzz.token_set_ratio( lookup_string, version, score_cutoff = cutoff )
#                     , "fuzz_token_sort_ratio"         : rapidfuzz.fuzz.token_sort_ratio( lookup_string, version, score_cutoff = cutoff )
#                     # , "fuzz_wratio"                   : rapidfuzz.fuzz.WRatio( lookup_string, version, score_cutoff = cutoff )
#                     , "fuzz_qratio"                   : rapidfuzz.fuzz.QRatio( lookup_string, version, score_cutoff = cutoff )
#                     , "dist_jarowinkler_normalized_similarity"
#                                                       : rapidfuzz.distance.JaroWinkler.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100
#                     , "dist_levensh_normalized_similarity"
#                                                       : rapidfuzz.distance.Levenshtein.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100
#                     , "dist_hamm_normalized_similarity"
#                                                       : rapidfuzz.distance.Hamming.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100
#                     , "dist_indel_normalized_similarity"
#                                                       : rapidfuzz.distance.Indel.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100
#                 }
#             )
#         else:
#             temp_calc = [ ]
#             temp_calc.append( [ "fuzz_ratio",                     rapidfuzz.fuzz.ratio( lookup_string, version, score_cutoff = cutoff ) ] )
#             temp_calc.append( [ "fuzz_partial_ratio",             rapidfuzz.fuzz.partial_ratio( lookup_string, version, score_cutoff = 90 ) if rapidfuzz.fuzz.partial_ratio( lookup_string, version, score_cutoff = 90 ) < 100 else 0 ] )
#             # temp_calc.append( [ "fuzz_partial_token_ratio",       rapidfuzz.fuzz.partial_token_ratio( lookup_string, version, score_cutoff = cutoff ) ] )
#             # temp_calc.append( [ "fuzz_partial_token_set_ratio",   rapidfuzz.fuzz.partial_token_set_ratio( lookup_string, version, score_cutoff = cutoff ) ] )
#             # temp_calc.append( [ "fuzz_partial_token_sort_ratio",  rapidfuzz.fuzz.partial_token_sort_ratio( lookup_string, version, score_cutoff = cutoff ) ] )
#             temp_calc.append( [ "fuzz_token_ratio",               rapidfuzz.fuzz.token_ratio( lookup_string, version, score_cutoff = cutoff ) ] )
#             temp_calc.append( [ "fuzz_token_set_ratio",           rapidfuzz.fuzz.token_set_ratio( lookup_string, version, score_cutoff = cutoff ) ] )
#             temp_calc.append( [ "fuzz_token_sort_ratio",          rapidfuzz.fuzz.token_sort_ratio( lookup_string, version, score_cutoff = cutoff ) ] )
#             # temp_calc.append( [ "fuzz_wratio",                    rapidfuzz.fuzz.WRatio( lookup_string, version, score_cutoff = cutoff ) ] )
#             temp_calc.append( [ "fuzz_qratio",                    rapidfuzz.fuzz.QRatio( lookup_string, version, score_cutoff = cutoff ) ] )
#             temp_calc.append( [ "dist_jarowinkler_normalized_similarity",   rapidfuzz.distance.JaroWinkler.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100 ] )
#             temp_calc.append( [ "dist_levensh_normalized_similarity",       rapidfuzz.distance.Levenshtein.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100 ] )
#             temp_calc.append( [ "dist_hamm_normalized_similarity",          rapidfuzz.distance.Hamming.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100 ] )
#             temp_calc.append( [ "dist_indel_normalized_similarity",         rapidfuzz.distance.Indel.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100 ] )

#             for review in temp_calc:
#                 if review[ 1 ] > scores[ review[ 0 ] ]:
#                     scores.update( { review[ 0 ] : review[ 1 ] } )
#                 else:
#                     pass
#     return scores


def calculate_fuzzy_scores_v0_1(
         lookup_string
        , master_string
        , cutoff = 75
    ):
    """
    Calculate all the fuzzy matching scores between lookup_string and master_string.
    """
    lookup_string = lookup_string.lower( ) if lookup_string is not None else ""

    if isinstance( master_string, type( "asdf" ) ) or isinstance( master_string, type( None ) ):
        master_string_list = [ master_string.lower( ) if master_string is not None else "" ]
    elif isinstance( master_string, type( { } ) ):
        master_string_list = [ list( master_string.keys( ) )[ 0 ] ]
        master_string_list.extend( master_string[ master_string_list[ 0 ] ] )


    scores = {
         "fuzz_ratio"               : 0
        , "fuzz_partial_ratio"      : 0
        , "fuzz_token_ratio"        : 0
        , "fuzz_token_set_ratio"    : 0
        , "fuzz_token_sort_ratio"   : 0
        , "fuzz_qratio"             : 0
        , "dist_jarowinkler_normalized_similarity"  : 0
        , "dist_levensh_normalized_similarity"      : 0
        , "dist_hamm_normalized_similarity"         : 0
        , "dist_indel_normalized_similarity"        : 0
    }

    # Calculate and update scores
    for version in master_string_list:
        fuzz_ratio = rapidfuzz.fuzz.ratio( lookup_string, version, score_cutoff = cutoff )
        fuzz_partial_ratio = (
             rapidfuzz.fuzz.partial_ratio( lookup_string, version, score_cutoff = 90 )
            if rapidfuzz.fuzz.partial_ratio( lookup_string, version, score_cutoff = 90 ) < 100
            else 0
        )
        fuzz_token_ratio        = rapidfuzz.fuzz.token_ratio( lookup_string, version, score_cutoff = cutoff )
        fuzz_token_set_ratio    = rapidfuzz.fuzz.token_set_ratio( lookup_string, version, score_cutoff = cutoff )
        fuzz_token_sort_ratio   = rapidfuzz.fuzz.token_sort_ratio( lookup_string, version, score_cutoff = cutoff )
        fuzz_qratio             = rapidfuzz.fuzz.QRatio( lookup_string, version, score_cutoff = cutoff )
        dist_jarowinkler_normalized_similarity  = ( rapidfuzz.distance.JaroWinkler.normalized_similarity( lookup_string, version, score_cutoff=cutoff / 100 ) * 100 )
        dist_levensh_normalized_similarity      = ( rapidfuzz.distance.Levenshtein.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100 )
        dist_hamm_normalized_similarity         = ( rapidfuzz.distance.Hamming.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100 )
        dist_indel_normalized_similarity        = ( rapidfuzz.distance.Indel.normalized_similarity( lookup_string, version, score_cutoff = cutoff / 100 ) * 100 )

        scores.update(
            {
                 "fuzz_ratio"               : max( scores[ "fuzz_ratio" ], fuzz_ratio )
                , "fuzz_partial_ratio"      : max( scores[ "fuzz_partial_ratio" ], fuzz_partial_ratio )
                , "fuzz_token_ratio"        : max( scores[ "fuzz_token_ratio" ], fuzz_token_ratio )
                , "fuzz_token_set_ratio"    : max( scores[ "fuzz_token_set_ratio" ], fuzz_token_set_ratio )
                , "fuzz_token_sort_ratio"   : max( scores[ "fuzz_token_sort_ratio" ], fuzz_token_sort_ratio )
                , "fuzz_qratio"             : max( scores[ "fuzz_qratio" ], fuzz_qratio )
                , "dist_jarowinkler_normalized_similarity"  : max( scores[ "dist_jarowinkler_normalized_similarity" ], dist_jarowinkler_normalized_similarity )
                , "dist_levensh_normalized_similarity"      : max( scores[ "dist_levensh_normalized_similarity" ], dist_levensh_normalized_similarity )
                , "dist_hamm_normalized_similarity"         : max( scores[ "dist_hamm_normalized_similarity" ], dist_hamm_normalized_similarity )
                , "dist_indel_normalized_similarity"        : max( scores[ "dist_indel_normalized_similarity" ], dist_indel_normalized_similarity )
            }
        )
    return scores


def highest_score_from_dict_v0( match_result_dict ):
    """
    Find the highest score from the match_result_dict.
    """
    highest_score = 0
    for idx, match_result in enumerate( match_result_dict ):
        if match_result_dict[ match_result ] >= highest_score:
            highest_score = match_result_dict[ match_result ]
            final_result = { match_result : highest_score }

    if highest_score == 0:
        return { "fail_to_match": highest_score }
    else:
        return final_result


''' ##phased out
def car_library_match_v0(
         country_short
        , lookup_string
        , master_dict
        , brandmodel_list = None
        , cutoff = 75
    ):
    """
    Perform fuzzy matching between lookup_string and master_dict items.
    """

    try:
        lookup_string = str( lookup_string ).lower( ) if lookup_string is not None else ""
        brandmodel_list = [ i.lower( ) for i in brandmodel_list ] if brandmodel_list is not None else brandmodel_list
        # bm_list = set( )
        bm_scores = { }

        for record in master_dict:
            bm_string = f"""{record[ "brand" ]} {record[ "model" ]}"""
            bm_string_delimiter = f"""{record[ "brand" ]};{record[ "model" ]}"""
            scores = \
                calculate_fuzzy_scores_v0_1(
                    lookup_string = lookup_string if brandmodel_list is None else " ".join( brandmodel_list )
                    , master_string = bm_string
                    , cutoff = cutoff
                )
            max_score = max( scores.values( ) )
            if max_score > 0:
                bm_scores[ bm_string_delimiter ] = max( scores.values( ) )

            # if bm_string not in bm_list:
            #     bm_list.add( bm_string )
            #     scores = calculate_fuzzy_scores( lookup_string, bm_string, cutoff )
            #     bm_scores[ bm_string ] = max( scores.values( ) )

        bm_matched = highest_score_from_dict_v0( bm_scores )

        # Retrieve the matched brand and model
        if list( bm_matched.keys( ) )[ 0 ] == "fail_to_match":
            component_match_dict = { "fail_to_match" : "no corresponding match for Brand and Model. REVIEW MASTER DATA" }
        else:
            matched_brand_model = list( bm_matched.keys( ) )[ 0 ].split( ";" )
            matched_brand, matched_model = matched_brand_model[ 0 ], matched_brand_model[ 1 ]

            # Filter the master_dict for matched brand and model
            filtered_bm_list = [ item for item in master_dict if item[ "brand" ] == matched_brand and item[ "model" ] == matched_model ]

            if country_short.upper( ) == "PH":
                component_target_list = [
                     [ "brand", "str" ]
                    , [ "model", "str" ]
                    , [ "variant", "dict" ]
                    , [ "variant_extend", "dict" ]
                    , [ "engine_capacity", "dict" ]
                    , [ "transmission", "dict" ]
                    , [ "fuel_type", "str" ]
                    , [ "body_type", "dict" ]
                ]
            elif country_short.upper( ) == "MY":
                component_target_list = [
                     [ "brand", "str" ]
                    , [ "model", "dict" ]
                    , [ "variant", "dict" ]
                    , [ "engine_capacity", "dict" ]
                    , [ "transmission", "dict" ]
                    , [ "fuel_type", "dict" ]
                    , [ "body_type", "dict" ]
                ]
            component_scores = { }


            for component_row in filtered_bm_list:
                for component_target in component_target_list:
                    component_trgt_value = json.loads( component_row[ component_target[ 0 ] ] ) if component_target[ 1 ] == "dict" else component_row[ component_target[ 0 ] ]
                    if component_trgt_value not in [ {}, None, "" ]:
                        keyvalue = (
                             list( component_trgt_value.keys( ) )[ 0 ] if isinstance( component_trgt_value, type( {} ) )
                            else component_trgt_value
                        )
                        high_score = \
                            highest_score_from_dict_v0(
                                calculate_fuzzy_scores_v0_1(
                                     (
                                        lookup_string if ( brandmodel_list is None ) or ( component_target[ 0 ] in [ "brand", "model" ] )
                                        else lookup_string.replace( brandmodel_list[ 0 ], "" ).replace( brandmodel_list[ 1 ], "" ).strip( )
                                     )
                                    , component_trgt_value
                                    , cutoff
                                )
                            )
                        score = list( high_score.values( ) )[ 0 ]
                        if ( component_target[ 0 ] not in component_scores ) and ( score != 0 ):
                            component_scores[ component_target[ 0 ] ] = { keyvalue : high_score }
                        elif ( component_target[ 0 ] in component_scores ) and score >= list( list( component_scores[ component_target[ 0 ] ].values( ) )[ 0 ].values( ) )[ 0 ]:
                            component_scores[ component_target[ 0 ] ] = { keyvalue : high_score }
                    else:
                        pass

            component_match_dict = { item: [ list( component_scores[ item ].keys( ) )[ 0 ], list( component_scores[ item ].values( ) )[ 0 ] ]  for item in component_scores }
    except Exception as exception:
        component_match_dict = { "error": str( exception ) }
    return json.dumps( component_match_dict )
'''


def car_library_match_v0_1(
         country_short
        , lookup_string
        , master_dict
        , brandmodel_list = None
        , cutoff = 75
    ):
    """
    Perform fuzzy matching between lookup_string and master_dict items.
    """

    try:
        lookup_string = str( lookup_string ).lower( ) if lookup_string is not None else ""
        brandmodel_list = [ i.lower( ).strip( ) for i in brandmodel_list ] if brandmodel_list is not None else brandmodel_list
        # bm_list = set( )
        bm_matched_dict = { }

        df_master_dict = pda.DataFrame( master_dict )
        df_master_dict.sort_values(
            by = [ "brand", "model_charlength", "model", "variant_charlength" ]
            , ascending = [ True ] * 2 + [ True ] * 2
            , inplace = True
        )
        df_master_dict[ "model_key" ] = \
            df_master_dict[ "model" ].apply(
                 lambda x:
                     list( json.loads( str( x ) ).keys( ) )[ 0 ] if isinstance( json.loads( str( x ) ), dict ) and json.loads( str( x ) ) != { }
                    else "" if isinstance( json.loads( str( x ) ), dict ) and json.loads( str( x ) ) == { }
                    else x
            )
        df_master_dict[ "bm_string" ] = df_master_dict[ "brand" ] + " " + df_master_dict[ "model_key" ]
        df_master_dict[ "bm_string_delimiter" ] = df_master_dict[ "brand" ] + ";" + df_master_dict[ "model_key" ]
        df_master_dict[ "scores" ] = \
            df_master_dict[ "bm_string" ].apply(
                 lambda x:
                     max(
                         calculate_fuzzy_scores_v0_1(
                             lookup_string = lookup_string if brandmodel_list is None else " ".join( brandmodel_list )
                            , master_string = str( x )
                            , cutoff = cutoff
                         ).values( )
                     )
            )
        df_master_dict = df_master_dict[ df_master_dict[ "scores" ] == df_master_dict[ "scores" ].max( ) ]
        df_master_dict[ "df_bm_scores" ] = df_master_dict.apply( lambda row: { row[ "bm_string_delimiter" ] : row[ "scores" ] }, axis = 1 )
        for i in df_master_dict[ "df_bm_scores" ].tolist( ):
            bm_matched_dict.update( i )
        bm_matched_dict_final = { }
        if len( bm_matched_dict ) > 1:
            for row in bm_matched_dict:
                if brandmodel_list[ 1 ] == row.split( ";" )[ 1 ]:
                    bm_matched_dict_final.update( { row : bm_matched_dict[ row ] } )
            if bm_matched_dict_final == { }:
                for idy, row_1 in enumerate( bm_matched_dict ):
                    if idy == 0:
                        bm_matched_dict_final.update( { row_1 : bm_matched_dict[ row_1 ] } )
                    else:
                        break
        else:
            bm_matched_dict_final = bm_matched_dict

        bm_matched = highest_score_from_dict_v0( bm_matched_dict_final )
        match_brand, match_model = list( bm_matched.keys( ) )[ 0 ].split( ";" )

        # Retrieve the matched brand and model
        if list( bm_matched.keys( ) )[ 0 ] == "fail_to_match":
            component_match_dict = { "fail_to_match" : "no corresponding match for Brand and Model. REVIEW MASTER DATA" }
        else:
            df_bm_list = df_master_dict.copy( )
            df_bm_list = df_bm_list[ df_bm_list[ "df_bm_scores" ] == bm_matched ]
            df_bm_list.sort_values(
                by = [ "brand", "model_charlength", "model", "variant_charlength" ]
                , ascending = [ True ] * 2 + [ True ] * 2
                , inplace = True
            )

            if country_short.upper( ) == "PH":
                component_target_list = [
                     [ "brand", "str" ]
                    , [ "model", "dict" ]
                    , [ "variant", "dict" ]
                    , [ "variant_extend", "dict" ]
                    , [ "engine_capacity", "dict" ]
                    , [ "transmission", "dict" ]
                    , [ "fuel_type", "dict" ]
                    , [ "body_type", "dict" ]
                ]
            elif country_short.upper( ) == "MY":
                component_target_list = [
                     [ "brand", "str" ]
                    , [ "model", "dict" ]
                    , [ "variant", "dict" ]
                    , [ "engine_capacity", "dict" ]
                    , [ "transmission", "dict" ]
                    , [ "fuel_type", "dict" ]
                    , [ "body_type", "dict" ]
                ]
            elif country_short.upper( ) == "ID":
                component_target_list = [
                     [ "brand", "str" ]
                    , [ "model", "dict" ]
                    , [ "variant", "dict" ]
                    , [ "engine_capacity", "dict" ]
                    , [ "transmission", "dict" ]
                    , [ "fuel_type", "dict" ]
                    , [ "body_type", "dict" ]
                ]

            component_scores = { }
            for component_target in component_target_list:
                component_row_list = { component_target[ 0 ] : list( df_bm_list[ component_target[ 0 ] ].unique( ) ) }
                for i in component_row_list[ component_target[ 0 ] ]:
                    component_trgt_value = json.loads( i ) if component_target[ 1 ] == "dict" else i
                    if component_trgt_value not in [ {}, None, "" ]:
                        keyvalue = (
                             list( component_trgt_value.keys( ) )[ 0 ] if isinstance( component_trgt_value, type( {} ) )
                            else component_trgt_value
                        )
                        high_score = \
                            highest_score_from_dict_v0(
                                calculate_fuzzy_scores_v0_1(
                                     (
                                        lookup_string if ( brandmodel_list is None ) or ( component_target[ 0 ] in [ "brand", "model" ] )
                                        else lookup_string.replace( match_brand, "" ).replace( match_model, "" ).strip( )
                                     )
                                    , component_trgt_value
                                    , cutoff
                                )
                            )
                        score = list( high_score.values( ) )[ 0 ]
                        if ( component_target[ 0 ] not in component_scores ) and ( score != 0 ):
                            component_scores[ component_target[ 0 ] ] = { keyvalue : high_score }
                        elif ( component_target[ 0 ] in component_scores ) and score >= list( list( component_scores[ component_target[ 0 ] ].values( ) )[ 0 ].values( ) )[ 0 ]:
                            component_scores[ component_target[ 0 ] ] = { keyvalue : high_score }
                    else:
                        pass
            component_match_dict = { item: [ list( component_scores[ item ].keys( ) )[ 0 ], list( component_scores[ item ].values( ) )[ 0 ] ]  for item in component_scores }

    except Exception as exception:
        component_match_dict = { "error": str( exception ) }
    return json.dumps( component_match_dict )


''' ##not efficient enough
def car_library_match_v1(
         country_short
        , lookup_string
        , df_bmv
        , etfdb_df_dict
        , brandmodel_list = None
        , cutoff = 75
    ):

    try:
        lookup_string = str( lookup_string ).lower( ) if lookup_string is not None else ""
        brandmodel_list = [ i.lower( ) for i in brandmodel_list ] if brandmodel_list is not None else brandmodel_list
        # bm_list = set( )
        bm_matched_dict = { }

        df_bmv[ "model_key" ] = df_bmv[ "model" ].apply( lambda x: list( json.loads( x ).keys( ) )[ 0 ] )
        df_bmv[ "bm_string" ] = df_bmv[ "brand" ] + " " + df_bmv[ "model_key" ]
        df_bmv[ "bm_string_delimiter" ] = df_bmv[ "brand" ] + ";" + df_bmv[ "model_key" ]
        df_bmv[ "scores" ] = \
            df_bmv[ "bm_string" ].apply(
                 lambda x:
                     max(
                         calculate_fuzzy_scores_v0_1(
                             lookup_string = lookup_string if brandmodel_list is None else " ".join( brandmodel_list )
                            , master_string = x
                            , cutoff = cutoff
                         ).values( )
                     )
            )
        df_bmv = df_bmv[ df_bmv[ "scores" ] == df_bmv[ "scores" ].max( ) ]
        df_bmv[ "df_bm_scores" ] = df_bmv.apply( lambda row: { row[ "bm_string_delimiter" ] : row[ "scores" ] }, axis = 1 )
        for i in df_bmv[ "df_bm_scores" ].tolist( ):
            bm_matched_dict.update( i )
        bm_matched = highest_score_from_dict_v0( bm_matched_dict )
        

        # Retrieve the matched brand and model
        if list( bm_matched.keys( ) )[ 0 ] == "fail_to_match":
            component_match_dict = { "fail_to_match" : "no corresponding match for Brand and Model. REVIEW MASTER DATA" }
        else:
            df_bm_list = df_bmv.copy( )
            df_bm_list = df_bm_list[ df_bm_list[ "df_bm_scores" ] == bm_matched ]

            if country_short.upper( ) == "PH":
                component_target_list = [
                     [ "brand", "str" ]
                    , [ "model", "str" ]
                    , [ "variant", "dict" ]
                    , [ "variant_extend", "dict" ]
                    , [ "engine_capacity", "dict" ]
                    , [ "transmission", "dict" ]
                    , [ "fuel_type", "str" ]
                    , [ "body_type", "dict" ]
                ]
            elif country_short.upper( ) == "MY":
                component_target_list = [
                     [ "brand", "str" ]
                    , [ "model", "dict" ]
                    , [ "variant", "dict" ]
                    , [ "engine_capacity", "dict" ]
                    , [ "transmission", "dict" ]
                    , [ "fuel_type", "dict" ]
                    , [ "body_type", "dict" ]
                ]

            component_scores = { }
            for component_target in component_target_list:
                component_row_list = {
                        component_target[ 0 ] :
                            list( etfdb_df_dict[ component_target[ 0 ] ][ component_target[ 0 ] ].unique( ) ) if component_target[ 0 ] in etfdb_df_dict.keys( )
                            else df_bm_list[ component_target[ 0 ] ].unique( ).tolist( )
                }
                for i in component_row_list[ component_target[ 0 ] ]:
                    component_trgt_value = json.loads( i ) if component_target[ 1 ] == "dict" else i
                    if component_trgt_value not in [ {}, None, "" ]:
                        keyvalue = (
                                list( component_trgt_value.keys( ) )[ 0 ] if isinstance( component_trgt_value, type( {} ) )
                            else component_trgt_value
                        )
                        high_score = \
                            highest_score_from_dict_v0(
                                calculate_fuzzy_scores_v0_1(
                                    (
                                        lookup_string if ( brandmodel_list is None ) or ( component_target[ 0 ] in [ "brand", "model" ] )
                                        else lookup_string.replace( brandmodel_list[ 0 ], "" ).replace( brandmodel_list[ 1 ], "" ).strip( )
                                    )
                                    , component_trgt_value
                                    , cutoff
                                )
                            )
                        score = list( high_score.values( ) )[ 0 ]
                        if ( component_target[ 0 ] not in component_scores ) and ( score != 0 ):
                            component_scores[ component_target[ 0 ] ] = { keyvalue : high_score }
                        elif ( component_target[ 0 ] in component_scores ) and score >= list( list( component_scores[ component_target[ 0 ] ].values( ) )[ 0 ].values( ) )[ 0 ]:
                            component_scores[ component_target[ 0 ] ] = { keyvalue : high_score }
                    else:
                        pass
            component_match_dict = { item: [ list( component_scores[ item ].keys( ) )[ 0 ], list( component_scores[ item ].values( ) )[ 0 ] ]  for item in component_scores }

    except Exception as exception:
        component_match_dict = { "error": str( exception ) }
    return json.dumps( component_match_dict )
'''


def iferror(
         success
        , failure
        , failure_is_fx = False
    ):
    '''
    e.g. iferror( lambda: <<success_expression>>, default_value_if_failure )
    '''

    try:
        result = success( )
        return result
    except Exception as exception:
        if failure_is_fx is False:
            return failure
        else:
            return failure( )


def coalesce( select_list ):
    for idx, i in enumerate( select_list ):
        if pda.isnull( i ):
            pass
        else:
            i
            break
    return i


def save_df_append(
         df
        , save_folderpath
        , save_filename
        , save_fileformat = "csv"
    ):

    full_file_path = Path( save_folderpath, f"""{save_filename}.{save_fileformat}""" )
    if os.path.exists( full_file_path ):
        df_existing = pda.read_csv( full_file_path )
        df_final = pda.concat( [ df_existing, df ], ignore_index = True )
        # df.to_csv(
        #      full_file_path
        #     , mode = "a"
        #     , index = False
        #     , header = False
        #     , encoding = "utf-8"
        # )
    else:
        df_final = df

    df_final.to_csv(
         full_file_path
        , index = False
        , encoding = "utf-8"
    )
    
    return df_final


def save_jsonl_append(
         json_data
        , save_folderpath
        , save_filename
        , save_fileformat = "jsonl"
    ):

    full_file_path = Path( save_folderpath, f"""{save_filename}.{save_fileformat}""" )
    with jsonlines.open( full_file_path, mode = "a" ) as jsonl_file:
        jsonl_file.write( json_data )


def jsonl_to_json(
         jsonl_filepath
        , save_json_filepath = None
        , keep_jsonl_file = True
    ):

    with jsonlines.open( Path( jsonl_filepath ), "r" ) as jsonl_file:
        json_data = [ line for line in jsonl_file ]

    if save_json_filepath is not None:
        with open( save_json_filepath, "w", encoding = "utf-8" ) as json_file:
            json.dump( json_data, json_file, ensure_ascii = False, indent = 4 )
    
    if keep_jsonl_file:
        pass
    else:
        os.remove( Path( jsonl_filepath ) )

    return json_data


def save_json_append(
         json_data
        , save_folderpath
        , save_filename
        , save_fileformat = "json"
    ):

    full_file_path = Path( save_folderpath, f"""{save_filename}.{save_fileformat}""" )
    if os.path.exists( full_file_path ):
        with open( full_file_path, "r", encoding = "utf-8" ) as json_file:
            existing_data = json.load( json_file )
    else:
        existing_data = [ ]
    
    if isinstance( json_data, type( [ ] ) ):
        existing_data.extend( json_data )
    else:
        existing_data.append( json_data )
    

    with open( full_file_path, "w", encoding = "utf-8" ) as json_file:
        json.dump( existing_data, json_file, ensure_ascii = False, indent = 4 )


def gen_df_query_string(
         batch_dict_record_format
        , operator = "&"
    ):
    '''
    batch_dict_record_format : dictionary with orient = "records"
    operator : { "options" : [ "&", "|" ] }
    Note -- subsequent use: df = df.query( <<generated_query_string>> )
    '''

    filter_list = [ ]
    for key in batch_dict_record_format:
        if pda.notnull( batch_dict_record_format[ key ] ):
            value = batch_dict_record_format[ key ]
            if "'" in str( value ):
                filter_string = f'''{key} == "{batch_dict_record_format[ key ]}"'''
            else:
                filter_string = f"{key} == '{batch_dict_record_format[ key ]}'"
        else:
            filter_string = f"{key}.isnull( )"
        filter_list.append( filter_string )
    query_string = f" {operator} ".join( filter_list )
    return query_string


## 20240217-0357: not tested yet
def df_overwrite_select_records(
           main_df
          , new_df
          , composite_keys
     ):

     df_batch_cols = new_df[ composite_keys ].groupby( composite_keys, dropna = False ).agg( count = ( composite_keys[ 0 ], "count" ) ).reset_index( ).drop( columns = [ "count" ] )
     batch_cols_dict = df_batch_cols.to_dict( "records" )
     for batch in batch_cols_dict:
          item_string = gen_df_query_string( batch )
          main_df = main_df.query( f"~( {item_string} )" )

     main_df = pda.concat( [ main_df, new_df ], ignore_index = True )
     return main_df


def convert_df_todict(
         df
        , groupby_column_list
        , groupby_count_col_name
        , save_filename = None
        , save_folderpath = None
    ):

    df_batch_col = \
        df.groupby(
             groupby_column_list
            , dropna = False
        ).agg(
             count = ( groupby_count_col_name, "count" )
        ).reset_index( ).drop(
             columns = [ "count" ]
        )
    batch_col_dict = df_batch_col.to_dict( "records" )

    if save_filename is not None and save_folderpath is not None:
        with open( Path( save_folderpath, f"""{save_filename}.json""" ), "w", encoding = "utf-8" ) as dict_file:
            dict_file.write( json.dumps( batch_col_dict, indent = 4 ) )
    
    return batch_col_dict


def selenium_interceptor( request, random_header ):
    del request.headers[ "user-agent" ]
    request.headers[ "user-agent" ] = random_header[ "user-agent" ]
    request.headers[ "sec-ch-ua" ] = random_header[ "sec-ch-ua" ]
    request.headers[ "referer" ] = "https://www.google.com"
    return request.headers


def capture_html(
         args
        , save_fileformat = "txt"
    ):
    '''
    ops_identifier, headers, url_link, time_sleep, retry_attempts, save_filename, save_folderpath = args
    '''

    ops_identifier, headers, url_link, time_sleep, retry_attempts, save_filename, save_folderpath = args

    for attempt in range( retry_attempts ):
        tagging = f"""{ops_identifier}_capture_html_{str( attempt + 1 )}"""
        lg.log_subscript_start( tagging, f"""capture html attempt--{str( attempt + 1 )} for {ops_identifier} -- {response.status_code if attempt != 0 else None}""" )

        try:
            with requests.get( url_link, headers = headers ) as response:
                if response.status_code == 200:
                    time.sleep( time_sleep )

                    if save_fileformat is not None:
                        with open( Path( save_folderpath, f"""{save_filename}.{save_fileformat}"""), "w", encoding = "utf-8" ) as content_txt:
                            content_txt.write( str( response.text ) )
                    else:
                        return str( response.text )

                    lg.log_subscript_finish( tagging, f"""attempt {str( attempt + 1 )} succeeded""" )
                    break
                elif response.status_code == 429:
                    time.sleep( int( response.headers.get( "Retry-After", 7 ) ) )
                else:
                    pass
        except RequestException as exception:
            lg.log_exception( f"errortag_{ops_identifier}", f"""FAILED!!! capture html {ops_identifier} : Attempt no. {str( attempt + 1 )} for {ops_identifier} -- {str( exception )}""" )
# def capture_html_v0(
#          args
#     ):
#     '''
#     ops_identifier, headers, url_link, time_sleep, retry_attempts, save_filename, save_folderpath = args
#     '''

#     ops_identifier, headers, url_link, time_sleep, retry_attempts, save_filename, save_folderpath = args

#     with requests.get( url_link, headers = headers ) as response:
#         try:
#             for attempt in range( 0, retry_attempts ):
#                 tagging = f"""{ops_identifier}_capture_html_{str( attempt + 1 )}"""
#                 lg.log_subscript_start( tagging, f"""capture html attempt--{str( attempt + 1 )} for {ops_identifier} -- {response.status_code if attempt != 0 else None}""" )

#                 if response.status_code == 200:
#                     time.sleep( time_sleep )
#                     with open( Path( save_folderpath, f"""{save_filename}.txt"""), "w", encoding = "utf-8" ) as html_txt:
#                         html_txt.write( str( response.text ) )

#                     lg.log_subscript_finish( tagging, f"""attempt {str( attempt + 1 )} succeeded""" )
#                     break
#                 else:
#                     if "Retry_After" in response.headers:
#                         time.sleep( int( response.headers[ "Retry_After" ] ) )
#                     continue
#         except Exception as exception:
#             lg.log_exception( f"errortag_{ops_identifier}", f"""FAILED!!! capture html {ops_identifier} : Attempt no. {str( attempt + 1 )} for {ops_identifier} -- {str( exception )}""" )


def car_library_match_with_exception_handle( args ):
    ops_identifier, country_short, lookup_string, master_dict, brandmodel_list, multiprocessing_list_dict, save_folderpath, save_filename, lock = args

    # ops_identifier = idx
    # country_short = x[ "country" ]
    # lookup_string = x[ "car_description_string" ]
    # master_dict = transform_master_dict[ "master_dict" ]
    # brandmodel_list = [ x[ "brand" ], x[ "model" ] ]
    # multiprocessing_list_dict = x
    # save_filename = f"""{country_short.lower( )}_{data_source.lower( )}_carlibmatch_wip"""

    try:
        match_result = car_library_match_v0_1( country_short, lookup_string, master_dict, brandmodel_list )
        multiprocessing_list_dict.update( { "carlib_mapped" : json.loads( match_result ) } )

        with lock:
            save_jsonl_append(
                json_data = multiprocessing_list_dict
                , save_folderpath = save_folderpath
                , save_filename = save_filename
            )
    except Exception as exception:
        lg.log_exception( f"""multiprocess_error_{ops_identifier}""", f"""ERROR!!! -- {str( exception )}""" )


def car_library_matching_v1(
         country
        , db_folderpath
        , sql_script_folder
        , carlib_folderpath
        , save_folderpath
        , custom_sqlquery_dict = None
        , db_name = "ee_bom"
        , data_source_list = None
        , set_overwrite = False
        , num_of_cores = 1
        # , query
        # , keep_columns
        # , component_sequence_list
        # , carlib_batch
    ):
    '''
    custom_sqlquery_dict : e.g. { "<<country>>" : { "<<data_source>>" : "<<custom sql query>>" } }
    '''

    with open( Path( sql_script_folder, "etl_base_tables.sql" ), "r", encoding = "utf-8" ) as sql_file:
        etl_base_script = sql_file.read( )

    query_dict = {
         "PH" : {
             "carsurvey" : (
                 f"""
                    {etl_base_script}
                    select
                         m1.*
                    from
                        temp_carsurvey_ph m1
                    where
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "filename"
                    , "country"
                    , "fair_market_value"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "manufacture_year"
                    , "body_type"
                    , "variant_concat"
                    , "transmission"
                    , "car_description_string"
                  ]
             )
            , "pira" : (
                 f"""
                    {etl_base_script}
                    select
                         m1.*
                    from
                        temp_pira_ph m1
                 """
                , [
                     "data_source"
                    , "filename"
                    , "country"
                    , "fair_market_value"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "variant"
                    , "variant_extend"
                    , "manufacture_year"
                    , "engine_capacity"
                    , "transmission"
                    , "fuel_type"
                    , "body_type"
                    , "variant_concat"
                    , "car_description_string"
                  ]
              )
            , "philkotse" : (
                 f"""
                    {etl_base_script}
                    select
                         m1.*
                    from
                        temp_philkotse_scraping m1
                    where
                         m1.timestamp_keycode in(
                            select
                                 timestamp_keycode
                            from
                                temp_philkotse_scraping
                            group by
                                 timestamp_keycode
                            order by
                                 timestamp_keycode desc
                            limit 1
                        )
                    and
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "timestamp_keycode"
                    , "country"
                    , "url_link"
                    , "listing_id"
                    , "date_posted"
                    , "scraped_at"
                    , "price"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "variant"
                    , "manufacture_year"
                    , "transmission"
                    , "body_type"
                    , "car_title_description"
                    , "car_description_string"
                  ]
              )
            , "autodeal" : (
                 f"""
                    {etl_base_script}
                    select
                         m1.*
                    from
                        temp_autodeal_scraping m1
                    where
                         m1.timestamp_keycode in(
                            select
                                 timestamp_keycode
                            from
                                temp_autodeal_scraping
                            group by
                                 timestamp_keycode
                            order by
                                 timestamp_keycode desc
                            limit 2     --contains 2 latest timestamp_keycodes for used and new cars respectively
                        )
                    and
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "timestamp_keycode"
                    , "country"
                    , "url_link"
                    , "listing_id"
                    , "type"
                    , "date_posted"
                    , "scraped_at"
                    , "price"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "variant"
                    , "manufacture_year"
                    , "engine_capacity"
                    , "transmission"
                    , "body_type"
                    , "car_title_description"
                    , "car_description_string"
                  ]
              )
            , "carousell" : (
                 f"""
                    {etl_base_script}
                    select
                         m1.*
                    from
                        temp_carousell_ph_scraping m1
                    where
                         m1.timestamp_keycode in(
                            select
                                 timestamp_keycode
                            from
                                temp_carousell_ph_scraping
                            group by
                                 timestamp_keycode
                            order by
                                 timestamp_keycode desc
                            limit 1
                        )
                    and
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "timestamp_keycode"
                    , "country"
                    , "url_link"
                    , "listing_id"
                    , "type"
                    , "date_posted"
                    , "scraped_at"
                    , "price"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "manufacture_year"
                    , "transmission"
                    , "fuel_type"
                    , "body_type"
                    , "car_title_description"
                    , "car_description_string"
                  ]
              )
            , "carmudi" : (
                 f"""
                    {etl_base_script}
                    select
                         m1.*
                    from
                        temp_carmudi_ph_scraping m1
                    where
                         m1.timestamp_keycode in(
                            select
                                 timestamp_keycode
                            from
                                temp_carmudi_ph_scraping
                            group by
                                 timestamp_keycode
                            order by
                                 timestamp_keycode desc
                            limit 1
                        )
                    and
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "timestamp_keycode"
                    , "country"
                    , "url_link"
                    , "listing_id"
                    , "type"
                    , "date_posted"
                    , "scraped_at"
                    , "price"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "manufacture_year"
                    , "engine_capacity"
                    , "transmission"
                    , "fuel_type"
                    , "body_type"
                    , "car_title_description"
                    , "car_description_string"
                  ]
              )
         }
        , "MY" : {
             "carbase" : (
                 f"""
                    {etl_base_script}
                    select
                         m1.*
                    from
                        temp_carbase_scraping m1
                    where
                         m1.timestamp_keycode in(
                            select
                                 timestamp_keycode
                            from
                                temp_carbase_scraping
                            group by
                                 timestamp_keycode
                            order by
                                 timestamp_keycode desc
                            limit 1
                        )
                    and
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "timestamp_keycode"
                    , "filename"
                    , "updated_at"
                    , "scraped_at"
                    , "country"
                    , "nvic"
                    , "original_price"
                    , "price_peninsular"
                    , "price_borneo"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "variant"
                    , "manufacture_year"
                    , "engine_capacity"
                    , "transmission"
                    , "body_type"
                    , "car_description_string"
                  ]
             )
            , "carlist" : (
                 f"""
                    {etl_base_script}
                    select
                        m1.*
                    from
                        temp_carlist_scraping m1
                    where
                        m1.timestamp_keycode in(
                            select
                                timestamp_keycode
                            from
                                temp_carlist_scraping
                            group by
                                timestamp_keycode
                            order by
                                timestamp_keycode desc
                            limit 1
                        )
                    and
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "timestamp_keycode"
                    , "country"
                    , "url_links"
                    , "listing_id"
                    , "updated_at"
                    , "scraped_at"
                    , "price"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "model_group"
                    , "variant"
                    , "manufacture_year"
                    , "engine_capacity"
                    , "transmission"
                    , "body_type"
                    , "car_description_string"
                  ]
              )
            , "mudah" : (
                 f"""
                    {etl_base_script}
                    select
                         m1.*
                    from
                        temp_mudah_scraping m1
                    where
                         m1.timestamp_keycode in(
                            select
                                 timestamp_keycode
                            from
                                temp_mudah_scraping
                            group by
                                 timestamp_keycode
                            order by
                                 timestamp_keycode desc
                            limit 1
                        )
                    and
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "timestamp_keycode"
                    , "country"
                    , "url_links"
                    , "listing_id"
                    , "updated_at"
                    , "scraped_at"
                    , "price"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "series"
                    , "variant"
                    , "manufacture_year"
                    , "engine_capacity"
                    , "transmission"
                    , "body_type"
                    , "car_description_string"
                  ]
              )
          }
        , "ID" : {
             "mobil123" : (
                 f"""
                    {etl_base_script}
                    select
                        m1.*
                    from
                        temp_mobil123_scraping m1
                    where
                        m1.timestamp_keycode in(
                            select
                                timestamp_keycode
                            from
                                temp_mobil123_scraping
                            group by
                                timestamp_keycode
                            order by
                                timestamp_keycode desc
                            limit 1
                        )
                    and
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "timestamp_keycode"
                    , "country"
                    , "url_links"
                    , "listing_id"
                    , "updated_at"
                    , "scraped_at"
                    , "price"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "model_group"
                    , "variant"
                    , "manufacture_year"
                    , "engine_capacity"
                    , "transmission"
                    , "body_type"
                    , "car_description_string"
                  ]
              )
            , "olx" : (
                 f"""
                    {etl_base_script}
                    select
                         m1.*
                    from
                        temp_olx_scraping m1
                    where
                        m1.timestamp_keycode in(
                            select
                                timestamp_keycode
                            from
                                temp_olx_scraping
                            group by
                                timestamp_keycode
                            order by
                                timestamp_keycode desc
                            limit 1
                        )
                    and
                         m1.is_analysis is null
                 """
                , [
                     "data_source"
                    , "timestamp_keycode"
                    , "country"
                    , "url_links"
                    , "listing_id"
                    , "updated_at"
                    , "scraped_at"
                    , "price"
                    # , "is_analysis"
                    , "brand"
                    , "model"
                    , "variant"
                    , "manufacture_year"
                    , "engine_capacity"
                    , "transmission"
                    , "body_type"
                    , "car_description_string"
                  ]
              )
          }
    }

    car_lib_batch = {
         "PH"   : "20230727-1027"
        , "MY"  : "20230808-1631"
        , "ID"  : "20231120-1654"
    }

    component_sequence_dict = {
         "bmvyetfb" : [
             "brand"
            , "model"
            , "variant"
            , "manufacture_year"
            , "engine_capacity"
            , "transmission"
            , "fuel_type"
            , "body_type"
         ]
    }

    ## ========================================================
    ##  INSERT FUNCTION HERE
    ## ========================================================

    def prepare_df_master_dict(
             country_short
            , carlib_folderpath
            , carlib_batch
        ):

        with open( Path( carlib_folderpath, f"""{carlib_batch} {country_short.lower( )}_master_car_library.json""" ), "r" ) as json_file:
            master_dict = json.load( json_file )

        df_master_dict = pda.DataFrame( master_dict )

        if country_short.upper( ) in [ "MY", "PH", "ID" ]:
            df_bmv = \
                df_master_dict.copy( ).groupby(
                    [
                         "brand"
                        , "model"
                        , "variant"
                    ] if country_short.upper( ) == "MY"
                    else [
                         "brand"
                        , "model"
                        , "variant"
                        , "variant_extend"
                    ] if country_short.upper( ) == "PH"
                    else [
                         "brand"
                        , "model"
                        , "variant"
                    ] if country_short.upper( ) == "ID"
                    else [ ]
                    , dropna = False
                ).agg(
                     count_rows = ( "country", "count" )
                ).reset_index( )
            df_engine = \
                df_master_dict.copy( ).groupby(
                     [ "engine_capacity" ]
                    , dropna = False
                ).agg(
                    count_rows = ( "country", "count" )
                ).reset_index( )
            df_transmission = \
                df_master_dict.copy( ).groupby(
                     [ "transmission" ]
                    , dropna = False
                ).agg(
                    count_rows = ( "country", "count" )
                ).reset_index( )
            df_fuel_type = \
                df_master_dict.copy( ).groupby(
                     [ "fuel_type" ]
                    , dropna = False
                ).agg(
                     count_rows = ( "country", "count" )
                ).reset_index( )
            df_body_type = \
                df_master_dict.copy( ).groupby(
                     [ "body_type" ]
                    , dropna = False
                ).agg(
                     count_rows = ( "country", "count" )
                ).reset_index( )    

        if country_short.upper( ) in [ "PH" ]:
            df_drivetrain_type = \
                df_master_dict.copy( ).groupby(
                     [ "drivetrain_type" ]
                    , dropna = False
                ).agg(
                     count_rows = ( "country", "count" )
                ).reset_index( )
        else:
            df_drivetrain_type = None


        etfdb_df_dict = {
             "engine_capacity"  : df_engine
            , "transmission"    : df_transmission
            , "fuel_type"       : df_fuel_type
            , "drivetrain_type" : df_drivetrain_type
            , "body_type"       : df_body_type
        }

        return { "bmv" : df_bmv, "etfdb" : etfdb_df_dict, "master_dict" : master_dict }


    def transform_df_carsurvey(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] \
            + " " + df[ "model" ] + " " + df[ "transmission" ] + " " + df[ "variant_concat" ]
        return df


    def transform_df_pira(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] \
            + " " + df[ "model" ] + " " + df[ "variant" ] + " " + df[ "variant_extend" ] \
            + " " + df[ "engine_capacity" ] + " " + df[ "transmission" ] \
            + " " + df[ "fuel_type" ] + " " + df[ "body_type" ] + " " + df[ "variant_concat" ]
        return df


    def transform_df_philkotse(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] \
            + " " + df[ "model" ] + " " + df[ "variant" ] \
            + " " + df[ "transmission" ] + " " + df[ "body_type" ] \
            + " " + df[ "car_title_description" ]
        return df


    def transform_df_autodeal(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] \
            + " " + df[ "model" ] + " " + df[ "variant" ] + " " + df[ "engine_capacity" ] \
            + " " + df[ "transmission" ] + " " + df[ "fuel_type" ] + " " + df[ "body_type" ] \
            + " " + df[ "car_title_description" ]
        return df


    def transform_df_carousell_ph(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] \
            + " " + df[ "model" ] + " " + df[ "transmission" ] + " " + df[ "fuel_type" ] \
            + " " + df[ "body_type" ] + " " + df[ "car_title_description" ]
        return df


    def transform_df_carmudi_ph(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] \
            + " " + df[ "model" ] + " " + df[ "variant" ] + " " + df[ "engine_capacity" ] \
            + " " + df[ "transmission" ] + " " + df[ "fuel_type" ] + " " + df[ "body_type" ] \
            + " " + df[ "car_title_description" ]
        return df


    def transform_df_carbase(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] + " " + df[ "model" ] \
            + " " + df[ "variant" ] + " " + df[ "engine_capacity" ] \
            + " " + df[ "transmission" ] + " " + df[ "body_type" ]
        return df


    def transform_df_carlist(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] + " " + df[ "model" ] \
            + " " + df[ "model_group" ] + " " + df[ "variant" ] + " " + df[ "engine_capacity" ] \
            + " " + df[ "transmission" ] + " " + df[ "fuel_type" ] + " " + df[ "body_type" ]
        return df


    def transform_df_mudah(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] + " " + df[ "model" ] \
            + " " + df[ "series" ] + " " + df[ "variant" ] + " " + df[ "engine_capacity" ] \
            + " " + df[ "transmission" ] + " " + df[ "fuel_type" ] + " " + df[ "body_type" ]
        return df


    def transform_df_mobil123(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] + " " + df[ "model" ] \
            + " " + df[ "model_group" ] + " " + df[ "variant" ] + " " + df[ "engine_capacity" ] \
            + " " + df[ "transmission" ] + " " + df[ "fuel_type" ] + " " + df[ "body_type" ]
        return df


    def transform_df_olx(
            df
        ):
        df[ "car_description_string" ] = \
            df[ "manufacture_year" ].astype( "str" ) + " " + df[ "brand" ] + " " + df[ "model" ] \
            + " " + df[ "variant" ] + " " + df[ "engine_capacity" ] + " " + df[ "transmission" ] \
            + " " + df[ "fuel_type" ] + " " + df[ "body_type" ] + " " + df[ "car_title_description" ]
        return df


    def execute_matching_logic(
             country_short
            , db_name
            , db_folderpath
            , data_source
            , query
            , keep_columns
            , component_sequence_list
            , carlib_folderpath
            , save_folderpath
            , overwrite
            , num_of_cores
        ):

        rename_cols_with_ori = {
            "brand"                : "brand_ori"
            , "model"               : "model_ori"
            , "model_group"         : "model_group_ori"
            , "variant"             : "variant_ori"
            , "variant_extend"      : "variant_extend_ori"
            , "engine_capacity"     : "engine_capacity_ori"
            , "transmission"        : "transmission_ori"
            , "fuel_type"           : "fuel_type_ori"
            , "drivetrain_type"     : "drivetrain_type_ori"
            , "body_type"           : "body_type_ori"
        }

        checkpoint = "checkpoint 1.0"
        rename_cols_with_mapped = { }
        for item in rename_cols_with_ori:
            rename_cols_with_mapped.update( { item : rename_cols_with_ori[ item ].replace( "ori", "mapped" ) } )
        
        transform_master_dict = \
            prepare_df_master_dict(
                country_short
                , carlib_folderpath
                , car_lib_batch[ country_short.upper( ) ]
            )

        pda_df = \
            sql_table_query(
                connection = \
                    sqlite3_connect(
                         db_name = db_name
                        , db_folderpath = db_folderpath
                    )
                , query = query
            )

        for col in pda_df.select_dtypes( include = [ "object" ] ).columns:
            pda_df[ col ] = \
                pda_df[ col ].apply(
                    lambda x:
                        "" if pda.isnull( x )
                        else x
                )

        ##PHILIPPINES
        if data_source == "carsurvey":
            pda_df = transform_df_carsurvey( pda_df )
        elif data_source == "pira":
            pda_df = transform_df_pira( pda_df )
        elif data_source == "philkotse":
            pda_df = transform_df_philkotse( pda_df )
        elif data_source == "autodeal":
            pda_df = transform_df_autodeal( pda_df )
        elif data_source == "carousell":
            pda_df = transform_df_carousell_ph( pda_df )
        elif data_source == "carmudi":
            pda_df = transform_df_carmudi_ph( pda_df )

        ##MALAYSIA
        elif data_source == "carbase":
            pda_df = transform_df_carbase( pda_df )
        elif data_source == "carlist":
            pda_df = transform_df_carlist( pda_df )
        elif data_source == "mudah":
            pda_df = transform_df_mudah( pda_df )

        ##INDONESIA
        elif data_source == "mobil123":
            pda_df = transform_df_mobil123( pda_df )
        elif data_source == "olx":
            pda_df = transform_df_olx( pda_df )
        

        checkpoint = "checkpoint 1.1"
        if data_source not in [ "carsurvey", "pira", "carbase" ]:
            pda_df.sort_values(
                by = [ "data_source", "country", "timestamp_keycode", "listing_id", "scraped_at" ]
                , ascending = [ True ] * 4 + [ False ]
                , inplace = True
            )
            pda_df.drop_duplicates(
                subset = [ "data_source", "country", "timestamp_keycode", "listing_id" ]
                , keep = "first"
                , inplace = True
            )


        checkpoint = "checkpoint 2.0"
        # '''## <<insert new step here =============================================================================
        ##GET LIST UNIQUE LIST BASED ON "data_source", "country", "timestamp_keycode", "listing_id", "url_link" and "car_description_string" columns
        multiprocessing_columns_raw = [
            "data_source"
            , "country"
            , "timestamp_keycode"
            , "filename"
            , "listing_id"
            , "car_description_string"
            , "brand"
            , "model"
        ]

        multiprocessing_columns = multiprocessing_columns_raw.copy( )
        for col in multiprocessing_columns:
            if col not in pda_df.columns:
                multiprocessing_columns.remove( col )
        df_multiprocessing = \
            pda_df[ multiprocessing_columns ].drop_duplicates(
                subset = multiprocessing_columns
                , keep = "first"
            )
        multiprocessing_unique_list = df_multiprocessing.to_dict( "records" )
        # x=multiprocessing_unique_list[ 10 ]


        carlibmatch_file = f"""{country_short.lower( )}_{data_source.lower( )}_carlibmatch_wip"""
        file_backup(
            folderpath      = save_folderpath
            , filename      = carlibmatch_file
            , fileformat    = "jsonl"
            , keep_ori_file = False
        )
        manager = mp.Manager( )
        lock = manager.Lock( )
        with mp.Pool( processes = num_of_cores ) as pool:
            # ops_identifier, country_short, lookup_string, master_dict, brandmodel_list
            args_list = [ ( idx, x[ "country" ], x[ "car_description_string" ], transform_master_dict[ "master_dict" ], [ x[ "brand" ], x[ "model" ] ], x, save_folderpath, carlibmatch_file, lock ) for idx, x in enumerate( multiprocessing_unique_list ) ]
            pool.map( car_library_match_with_exception_handle, args_list )


        checkpoint = "checkpoint 3.0"
        carlibmatch_json = \
            jsonl_to_json(
                 jsonl_filepath = Path( save_folderpath, f"""{carlibmatch_file}.jsonl""" )
            )
        df_carlibmatch = pda.json_normalize( carlibmatch_json )

        # pda_df_backup = pda_df.copy( )
        pda_df = pda_df[ keep_columns ]
        # qq = "m" if data_source in [ "carsurvey", "pira", "carlist", "mudah", "mobil123", "olx" ] else "1"
        pda_df = \
            pda.merge(
                 pda_df
                , df_carlibmatch
                , how           = "left"
                , left_on       = multiprocessing_columns
                , right_on      = multiprocessing_columns
                , suffixes      = ( "", "_x" )
                , validate      = "m:1"      ##f"""{qq}:1"""
                , indicator     = False
            )

        pda_df.rename(
             columns    = rename_cols_with_ori
            , inplace   = True
        )

        rename_cols_with_mapped = { col : f"""{col.replace( "carlib_mapped.", "" )}_mapped""" if "carlib_mapped." in col else col for col in pda_df.columns }
        pda_df.rename(
             columns    = rename_cols_with_mapped
            , inplace   = True
        )

        mapped_column_list = list( pda_df.columns[ pda_df.columns.str.contains( "mapped" ) ] )
        for mapped_col in mapped_column_list:
            pda_df[ mapped_col.replace( "_mapped", "" ) ] = \
                pda_df[ mapped_col ].apply(
                    lambda x:
                        json.loads( json.dumps( x ) )[ 0 ].upper( ) if str( x ) != "nan"
                        else None
                        #  str( x ).replace( "[", "" ).replace( "]", "" ).split( ", " )[ 0 ].replace( "'", "" ).upper( ) if str( x ) != "nan"
                        # else None
                )

        pda_df.sort_values(
             by = component_sequence_list
            , ascending = [ True ] * len( component_sequence_list )
            , inplace = True
        )
        carlibmatch_final_json = pda_df.to_dict( "records" )


        ##UPDATE JSON DATA
        carlib_matching_result_file = f"""{country_short.lower( )}_{data_source}_carlib_matching_result"""
        if overwrite is False:
            if os.path.exists( Path( save_folderpath, f"""{carlib_matching_result_file}.csv""" ) ):
                df_carlib_matching_result_ori = pda.read_csv( Path( save_folderpath, f"""{carlib_matching_result_file}.csv""" ) )
                with open( Path( save_folderpath, f"""{carlib_matching_result_file}.json""" ), "w", encoding = "utf-8" ) as json_file:
                    json.dump( df_carlib_matching_result_ori.to_dict( "records" ), json_file, ensure_ascii = False, indent = 4 )

        save_json_append(
            json_data = carlibmatch_final_json
            , save_folderpath = save_folderpath
            , save_filename = carlib_matching_result_file
        )

        ##BACKUP ORI CSV FILE
        file_backup(
            folderpath = save_folderpath
            , filename = carlib_matching_result_file
            , fileformat = "csv"
            , keep_ori_file = False
        )
        ##CONVERT UPDATED JSON TO CSV
        df_new_carlib_matching_result = pda.read_json( Path( save_folderpath, f"""{carlib_matching_result_file}.json""" ) )
        df_new_carlib_matching_result.to_csv( Path( save_folderpath, f"""{carlib_matching_result_file}.csv""" ), index = False )
        file_backup(
            folderpath = save_folderpath
            , filename = carlib_matching_result_file
            , fileformat = "json"
            , keep_ori_file = False
        )
    
    ## ===============================
    ##  RUN CODE
    ## ===============================

    for idx, sequence in enumerate( component_sequence_dict ):
        for idy, sql_query in enumerate( query_dict[ country.upper( ) ] ):
            if sql_query in ( query_dict[ country.upper( ) ].keys( ) if ( data_source_list == [ ] or data_source_list is None ) else data_source_list ):
                try:
                    tagging = f"execute_matching_logic_{idx}_{idy}"
                    lg.log_subscript_start( tagging, f"executing matching logic for {country.upper( )} -- {sql_query}" )

                    execute_matching_logic(
                         country_short = country.upper( )
                        , db_name = db_name
                        , db_folderpath = db_folderpath
                        , data_source = sql_query
                        , query = query_dict[ country.upper( ) ][ sql_query ][ 0 ] if custom_sqlquery_dict is None else custom_sqlquery_dict[ country.upper( ) ][ sql_query ]
                        , keep_columns = query_dict[ country.upper( ) ][ sql_query ][ 1 ]
                        , component_sequence_list = component_sequence_dict[ sequence ]
                        , carlib_folderpath = carlib_folderpath
                        , save_folderpath = save_folderpath
                        , overwrite = set_overwrite
                        , num_of_cores = num_of_cores
                    )

                    lg.log_subscript_finish( tagging, f"matching logic executed for {sql_query}" )
                
                except Exception as exception:
                    lg.log_exception( exception )


def compile_matched_scraping_v0(
         country_list
        , car_library_folder
        , sql_script_folder
        , sqlite_folder
        , custom_sqlquery_dict = None
        , db_name = "ee_bom"
        , save_folderpath = None
    ):
    '''
    custom_sqlquery_dict : e.g. { "<<country>>" : "<<custom sql script>>" }
    '''

    country_dict = {
         "MY" : {
             "target" : {
                 "carlist"  : "slitrac"
                , "mudah"   : "mudah"
             }
            , "distr_details" : {
                 "distr_name" : "carlist"
                , "gsheet_spreadsheet_id" : "1Av9JsR0TOxz6MF2G2knVIdtJYDWPxc_DZ7n7qRUt7KI"
                , "gsheet_spreadsheet_range" : "scraping_grouped!A2"
              }
         }
        , "ID" : {
             "target" : {
                 "mobil123" : "mobil123"
                , "olx"     : "olx"
             }
            , "distr_details" : {
                 "distr_name" : "mobil123"
                , "gsheet_spreadsheet_id" : "14ID2xhWmUCOaAdUC-FctL6_Sd10C0RTO8XJx-Jwe-00"
                , "gsheet_spreadsheet_range" : "scraping_grouped_wybmvetf!A2"
              }
         }
    }

    with open( Path( sql_script_folder, "etl_base_tables.sql" ), "r", encoding = "utf-8" ) as sql_file:
        etl_base_script = sql_file.read( )


    df_list = [ ]
    for country in country_list:
        for t in country_dict[ country.upper( ) ][ "target" ]:
            try:
                tagging = f"""{t}_carlib_match_result"""
                lg.log_subscript_start( tagging, f"""getting carlib match result for {t}...""" )

                pda_df_carlib_match = \
                    pda.read_csv(
                         Path( car_library_folder, "csv_analysis", f"""{country.lower( )}_{t}_carlib_matching_result.csv""" )
                        # , engine = "pyarrow"
                        , dtype = {
                            "listing_id" : "object"
                        }
                    )
                pda_df_carlib_match = \
                    pda_df_carlib_match[
                         ( pda_df_carlib_match[ "error_mapped" ].isnull( ) )
                        & ~( pda_df_carlib_match[ "brand" ].isnull( ) )
                        & ~( pda_df_carlib_match[ "model" ].isnull( ) )
                        & ~( pda_df_carlib_match[ "engine_capacity" ].isnull( ) )
                        & ~( pda_df_carlib_match[ "transmission" ].isnull( ) )
                        & ~( pda_df_carlib_match[ "fuel_type" ].isnull( ) )
                    ]
                select_sorting_columns = [
                     "data_source"
                    , "country"
                    , "timestamp_keycode"
                    , "listing_id"
                    , "scraped_at"
                ]
                select_dropdups_columns = select_sorting_columns.copy( )
                for sel_col in [ "scraped_at" ]:
                    if sel_col in select_dropdups_columns:
                        select_dropdups_columns.remove( sel_col )
                pda_df_carlib_match.sort_values(
                     by         = select_sorting_columns
                    , ascending = [ False ] * len( select_sorting_columns )
                    , inplace   = True
                )
                pda_df_carlib_match.drop_duplicates(
                     subset     = select_dropdups_columns
                    , keep      = "first"
                    , inplace   = True
                )

                lg.log_subscript_finish( tagging, "carlib match result obtained" )

            except Exception as exception:
                lg.log_exception( exception )


            try:
                tagging = f"""{t}_sql_query"""
                lg.log_subscript_start( tagging, f"""querying scrape details for {t}...""" )

                sql_scraping = f"""
                    {etl_base_script}
                    select
                         o1.data_source
                        , o1.country
                        , o1.timestamp_keycode
                        , o1.listing_id
                        , o1.seller_type
                        , o1.ad_type
                        , o1.seller_name
                        , o1.region
                        , o1.location
                    from(
                            select
                                 m1.*
                                , row_number( ) over(
                                    partition by
                                        m1.data_source
                                        , m1.country
                                        , m1.timestamp_keycode
                                        , m1.listing_id
                                    order by
                                        m1.scraped_at desc
                                ) as "latest_listings_ranking"
                            from
                                temp_{t}_scraping m1
                            where
                                 m1.is_analysis is null
                    ) o1
                    where
                        o1.latest_listings_ranking = 1
                """ if country.lower( ) == "my" \
                else f"""
                    {etl_base_script}
                    select
                        o1.data_source
                        , o1.country
                        , o1.timestamp_keycode
                        , o1.listing_id
                        , o1.seller_type
                        , o1.type
                        , o1.seller_name
                        , o1.region
                        , o1.location
                    from(
                            select
                                m1.*
                                , row_number( ) over(
                                    partition by
                                        m1.data_source
                                        , m1.country
                                        , m1.timestamp_keycode
                                        , m1.listing_id
                                    order by
                                        m1.scraped_at desc
                                ) as "latest_listings_ranking"
                            from
                                temp_{t}_scraping m1
                    ) o1
                    where
                        o1.latest_listings_ranking = 1
                """ if country.lower( ) == "id" \
                else ""

                pda_df_dbquery = \
                    sql_table_query(
                         table_name = f"""{t}_scrape_details"""
                        , connection = sqlite3_connect( db_name, sqlite_folder )
                        , query = sql_scraping if custom_sqlquery_dict is None else custom_sqlquery_dict[ country.upper( ) ]
                    )

                lg.log_subscript_finish( tagging, "scrape details queried" )

            except Exception as exception:
                lg.log_exception( exception )


            try:
                tagging = f"""{t}_transform_data"""
                lg.log_subscript_start( tagging, "transforming data..." )

                df_scrape = \
                    pda.merge(
                         pda_df_carlib_match
                        , pda_df_dbquery
                        , how       = "left"
                        , left_on   = select_dropdups_columns
                        , right_on  = select_dropdups_columns
                        , suffixes  = ( "", "_x" )
                        , validate  = "m:1"
                        , indicator = False
                    )
                df_scrape[ "is_carsome_listing" ] = \
                    df_scrape[ "seller_name" ].apply(
                        lambda x:
                            "yes" if "carsome" in str( x ).lower( ) else "no"
                    )
                df_scrape[ "is_olx_listing" ] = \
                    df_scrape[ "seller_name" ].apply(
                        lambda x:
                            "yes" if "olx auto" in str( x ).lower( ) else "no"
                    )
                df_scrape[ "scraped_at" ] = pda.to_datetime( df_scrape[ "scraped_at" ] )

                lg.log_subscript_finish( tagging, "data transformed" )

            except Exception as exception:
                lg.log_exception( exception )


            try:
                tagging = f"""{t}_get_latest_scrape"""
                lg.log_subscript_start( tagging, "getting latest scraped data..." )
                
                df_scrape = \
                    df_scrape[
                        ( df_scrape[ "timestamp_keycode" ] == sorted( df_scrape[ "timestamp_keycode" ].unique( ).tolist( ) )[ -1 ] )
                    ]
                df_list.append( df_scrape )

                lg.log_subscript_finish( tagging, "latest scrape obtained" )

            except Exception as exception:
                lg.log_exception( exception )


        try:
            tagging = "transform_data_2"
            lg.log_subscript_start( tagging, "transforming data phase 2..." )

            append_df_scrape = pda.concat( df_list, ignore_index = True )

            df_timestamp_keycode_mapping = \
                append_df_scrape.groupby(
                     [
                         "data_source"
                        , "timestamp_keycode"
                     ]
                    , dropna = False
                ).agg(
                    last_scraped_at = ( "scraped_at", "max" )
                ).reset_index( )

            group_column_list = [
                 "brand"
                , "model"
                , "variant"
                , "manufacture_year"
                , "engine_capacity"
                , "transmission"
                , "fuel_type"
            ] if country.lower( ) == "my" \
            else [
                 "brand"
                , "model"
                , "variant"
                , "manufacture_year"
                , "engine_capacity"
                , "transmission"
                , "fuel_type"
            ] if country.lower( ) == "id" \
            else [ ]

            append_df_scrape.loc[ :, "listing_id_concat" ] = \
                append_df_scrape.apply(
                     lambda row:
                        row[ "data_source" ] + row[ "country" ] + row[ "timestamp_keycode" ] + str( row[ "listing_id" ] )
                    , axis = 1
                )

            df_scrape_grouped = \
                append_df_scrape.groupby(
                     group_column_list
                    , dropna = False
                ).agg(
                     min_price                  = ( "price",                "min"                                       )
                    , avg_price                 = ( "price",                "mean"                                      )
                    , pct25_price               = ( "price",                percentile( 0.25 )                          )
                    , pct40_price               = ( "price",                percentile( 0.40 )                          )
                    , mdn_price                 = ( "price",                percentile( 0.50 )                          )
                    , pct60_price               = ( "price",                percentile( 0.60 )                          )
                    , pct75_price               = ( "price",                percentile( 0.75 )                          )
                    , max_price                 = ( "price",                "max"                                       )
                    , num_of_unq_listings       = ( "listing_id_concat",    "nunique"                                   )
                    , timestamp_keycode_list    = ( "timestamp_keycode",    lambda x: [ str( i ) for i in set( x ) ]    )
                    , data_source_list          = ( "data_source",          lambda x: [ str( i ) for i in set( x ) ]    )
                ).reset_index( )

            df_scrape_iscarsome = append_df_scrape[ append_df_scrape[ "is_carsome_listing" ] == "yes" ]
            df_scrape_iscarsome = \
                df_scrape_iscarsome.groupby(
                    group_column_list
                    , dropna = False
                ).agg(
                    carsome_min_price              = ( "price",        "min"                    )
                    # , carsome_avg_price             = ( "price",        "mean"                   )
                    # , carsome_pct25_price           = ( "price",        swp.percentile( 0.25 )   )
                    # , carsome_pct40_price           = ( "price",        swp.percentile( 0.40 )   )
                    # , carsome_mdn_price             = ( "price",        swp.percentile( 0.50 )   )
                    # , carsome_pct60_price           = ( "price",        swp.percentile( 0.60 )   )
                    # , carsome_pct75_price           = ( "price",        swp.percentile( 0.75 )   )
                    # , carsome_max_price             = ( "price",        "max"                    )
                    # , carsome_num_of_unq_listings   = ( "listing_id",   "nunique"                )
                ).reset_index( )

            if country.lower( ) == "id":
                df_scrape_isolx = append_df_scrape[ append_df_scrape[ "is_olx_listing" ] == "yes" ]
                df_scrape_isolx = \
                    df_scrape_isolx.groupby(
                        group_column_list
                        , dropna = False
                    ).agg(
                        olx_min_price              = ( "price",        "min"                    )
                        # , olx_avg_price             = ( "price",        "mean"                   )
                        # , olx_pct25_price           = ( "price",        swp.percentile( 0.25 )   )
                        # , olx_pct40_price           = ( "price",        swp.percentile( 0.40 )   )
                        # , olx_mdn_price             = ( "price",        swp.percentile( 0.50 )   )
                        # , olx_pct60_price           = ( "price",        swp.percentile( 0.60 )   )
                        # , olx_pct75_price           = ( "price",        swp.percentile( 0.75 )   )
                        # , olx_max_price             = ( "price",        "max"                    )
                        # , olx_num_of_unq_listings   = ( "listing_id",   "nunique"                )
                    ).reset_index( )

            df_scrape_grouped = \
                pda.merge(
                     df_scrape_grouped
                    , df_scrape_iscarsome
                    , how       = "left"
                    , left_on   = group_column_list
                    , right_on  = group_column_list
                    , suffixes  = ( "", "_x" )
                    , validate  = "1:1"
                    , indicator = False
                )

            if country.lower( ) == "id":
                df_scrape_grouped = \
                    pda.merge(
                         df_scrape_grouped
                        , df_scrape_isolx
                        , how       = "left"
                        , left_on   = group_column_list
                        , right_on  = group_column_list
                        , suffixes  = ( "", "_x" )
                        , validate  = "1:1"
                        , indicator = False
                    )

            df_scrape_analysis = df_scrape_grouped
            # df_scrape_analysis = \
            #     pda.merge(
            #          df_scrape_grouped
            #         , df_timestamp_keycode_mapping
            #         , how       = "left"
            #         , left_on   = [ "timestamp_keycode" ]
            #         , right_on  = [ "timestamp_keycode" ]
            #         , suffixes  = ( "", "_x" )
            #         , validate  = "m:1"
            #         , indicator = False
            #     )

            # latest_scraped_at = sorted( df_scrape_analysis[ "last_scraped_at" ].astype( "str" ).unique( ).tolist( ) )[ -1 ]

            if save_folderpath is not None:
                timestamp_0 = datetime.datetime.now( ).strftime( "%Y%m%d-%H%M" )
                df_scrape_analysis.to_csv( Path( save_folderpath, f"""{country.lower( )} compile_matched_scraping {timestamp_0}.csv""" ), index = False )

            lg.log_subscript_finish( tagging, "phase 2 data transform done" )

        except Exception as exception:
            lg.log_exception( exception )



        try:
            tagging = "apiload"
            lg.log_subscript_start( tagging, "loading to gsheet..." )

            # df_apiload      = df_scrape_analysis[ df_scrape_analysis[ "last_scraped_at" ] == latest_scraped_at ]
            df_apiload      = df_scrape_analysis
            df_apiload      = df_apiload.fillna( "--None" )
            df_apiload      = df_apiload.astype( str )
            df_columnlist   = [ df_apiload.columns.tolist( ) ]
            df_list         = df_apiload.values.tolist( )
            df_columnlist.extend( df_list )


            gsheet_spreadsheet_id   = country_dict[ country.upper( ) ][ "distr_details" ][ "gsheet_spreadsheet_id" ]
            gsheet_range_0          = country_dict[ country.upper( ) ][ "distr_details" ][ "distr_name" ]
            gsheet_range_1          = country_dict[ country.upper( ) ][ "distr_details" ][ "gsheet_spreadsheet_range" ]
            pygsapi_update(
                 service_account_type   = "file_content"
                , key_filename          = "chewy_gsapi_py"
                , encrypted_filename    = "chewy_gsapi_py"
                , spreadsheet_id        = gsheet_spreadsheet_id
                , gsheet_range          = f"""latest_{gsheet_range_0}_{gsheet_range_1}"""
                , data_upload_list      = df_columnlist
            )

            lg.log_subscript_finish( tagging, "gsheet loaded" )

        except Exception as exception:
            lg.log_exception( exception )



## AWS <<===========================================

def aws_s3_folder_create(
         key_name
        , inkript_file_name
        , inkript_file_format
        , bucket_name
        , folder_name
    ):

    a = encrypt.file_decrypt( key_name, inkript_file_name, f".{inkript_file_format}" )

    aws_client = \
        boto3.client(
             "s3"
            , aws_access_key_id     = a[ 0 ]
            , aws_secret_access_key = a[ 1 ]
        )

    aws_client.put_object(
         Bucket = bucket_name
        , Key   = f"{folder_name}/"
    )


def aws_s3_file_upload(
         key_name
        , inkript_file_name
        , inkript_file_format
        , bucket_name
        , retrieve_fullpath_file_name
        , upload_file_name
    ):

    a = encrypt.file_decrypt( key_name, inkript_file_name, f".{inkript_file_format}" )

    aws_client = \
        boto3.client(
             "s3"
            , aws_access_key_id     = a[ 0 ]
            , aws_secret_access_key = a[ 1 ]
        )
    
    aws_client.upload_file( retrieve_fullpath_file_name, bucket_name, upload_file_name )

## AWS >>===========================================



## 20240124-0528: not tested yet
def process_invntry_turnaround(
         previous_dataset
        , latest_dataset
        , composite_keys
        , proxy_invtry_column
        , proxy_cycle_end_column
    ):
    '''
    proxy_invtry_column : { "description" : "assign column name for inventory unique ID" }
    proxy_cycle_end_column : { "description" : "assign column name for end-inventory count date. This is an 'as-at' date" }
    [1] this is used to prepare the main base dataset for subsequent calculation for inventory turnaround, where granularity goes all the way down to inventory ID
    [2] refer to task_list "mb0012 - car_listings_invntry_turnaround" for details on inventory turnaround methodology
    '''

    previous_dataset.loc[ :, "is_previous_balance" ] = previous_dataset[ proxy_invtry_column ]
    previous_dataset.rename(
         columns  = { proxy_cycle_end_column : "previous_cycle_end_date" }
        , inplace = True
    )

    latest_dataset.loc[ :, "is_latest_balance" ] = latest_dataset[ proxy_invtry_column ]
    latest_dataset.rename(
         columns  = { proxy_cycle_end_column : "latest_cycle_end_date" }
        , inplace = True
    )


    existing_invntry = \
        pda.merge(
             previous_dataset
            , latest_dataset[ composite_keys + [ "is_latest_balance", "latest_cycle_end_date" ] ]
            , how       = "left"
            , left_on   = composite_keys
            , right_on  = composite_keys
            , suffixes  = ( "", "_x" )
            , validate  = "1:1"
            , indicator = False
        )

    new_invntry = \
        pda.merge(
             latest_dataset
            , previous_dataset[ composite_keys + [ "is_previous_balance" ] ]
            , how       = "left"
            , left_on   = composite_keys
            , right_on  = composite_keys
            , suffixes  = ( "", "_x" )
            , validate  = "1:1"
            , indicator = False
        )
    new_invntry = \
        new_invntry[
             ( new_invntry[ "is_previous_balance" ].isnull( ) )
        ]

    
    invntry_turn_template = pda.concat( [ existing_invntry, new_invntry ], ignore_index = True )
    invntry_turn_template.loc[ :, "days_cycle" ] = \
        invntry_turn_template.apply(
             lambda row:
                 ( pda.to_datetime( row[ "latest_cycle_end_date" ] ) - pda.to_datetime( row[ "previous_cycle_end_date" ] ) ).days
            , axis = 1
        )
    invntry_turn_template.loc[ :, "days_cycle" ] = invntry_turn_template[ "days_cycle" ].ffill( ).bfill( )
    invntry_turn_template.loc[ :, "latest_cycle_end_date" ] = invntry_turn_template[ "latest_cycle_end_date" ].ffill( ).bfill( )
    return invntry_turn_template


def trend_analysis(
         df
        , set_calculate_column
        , set_cycle_date_column
        , min_abs_pct_change = 0.03
    ):
    '''
    df : { "description" : "dataframe should contain data for only one specific profile, i.e. if there are 2 sets of inventories that need to be trend-analysed, they should be run separately and this dataframe should only be showing 1 inventory at a time for trend analysis" }
    min_abs_pct_change : { "description": "stands for minimum absolute percentage change and denotes the threshold to determine whether a trend is considered as increasing or decreasing trend" }
    '''
    
    df.sort_values(
         by = [ set_cycle_date_column ]
        , ascending = [ True ]
        , inplace = True
    )

    df[ "pct_change_1_cycle" ] = df[ set_calculate_column ].pct_change( )
    df[ "pct_change_3_cycle" ] = df[ set_calculate_column ].pct_change( 3 )
    df[ "pct_change_5_cycle" ] = df[ set_calculate_column ].pct_change( 5 )

    pct_change_1_cycle = df[ "pct_change_1_cycle" ]
    pct_change_3_cycle = df[ "pct_change_3_cycle" ]
    pct_change_5_cycle = df[ "pct_change_5_cycle" ]
    trend_tag_conditions_dict = [
         {
             "condition" : ( ( pct_change_3_cycle >= min_abs_pct_change ) & ( pct_change_5_cycle >= min_abs_pct_change ) ) & ( pct_change_1_cycle >= min_abs_pct_change )
            , "return_value" : "high increase"
         }
        , {
             "condition" : ( ( pct_change_3_cycle >= min_abs_pct_change ) & ( pct_change_5_cycle >= min_abs_pct_change ) )
            , "return_value" : "increase"
          }
        ##-------------------------------------------------------------------------------------------------------------------
        ##-------------------------------------------------------------------------------------------------------------------
        , {
             "condition" : ( ( pct_change_3_cycle <= -min_abs_pct_change ) & ( pct_change_5_cycle <= -min_abs_pct_change ) )
            , "return_value" : "decrease"
          }
        , {
             "condition" : ( ( pct_change_3_cycle <= -min_abs_pct_change ) & ( pct_change_5_cycle <= -min_abs_pct_change ) ) & ( pct_change_1_cycle <= -min_abs_pct_change )
            , "return_value" : "high decrease"
          }
    ]
    conditions = [ i[ "condition" ] for i in trend_tag_conditions_dict ]
    values_to_return = [ i[ "return_value" ] for i in trend_tag_conditions_dict ]
    df[ "cycle_trend_tag" ] = npy.select( conditions, values_to_return, "undetermined" )
    df_latest_trend_tag = \
        df.sort_values(
             by = [ set_cycle_date_column ]
            , ascending = [ False ]
        )
    df[ "latest_trend_tag" ] = df_latest_trend_tag[ "cycle_trend_tag" ].values.tolist( )[ 0 ]

    return df
