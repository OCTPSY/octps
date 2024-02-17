import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir

##additional modules
import webbrowser
import shutil
import sqlite3
import pandas as pds


##<< log tracking ===================

proj_class_name = "scuse"
task_name = "tickers"
save_log_folder = \
    otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        ,folder_extend = proj_class_name
        ,subfolder_extend = "python_logs"
    )
file_name = os.path.basename( __file__ )
is_log_update_1 = 0 ##activate this to 1 if report refresh update is needed

lg.log_setup(
     save_log_folder
    ,file_name
    ,is_log_update_1 = is_log_update_1
)

##log tracking>> ===================

lg.log_script_start( )



##=================================
##  INSERT CODE HERE
##=================================

working_folder = \
     otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        , folder_extend = proj_class_name
        , subfolder_extend = task_name
     )

##=================================
##  download gsheet to csv
##=================================

google_sheet_list = [
     (
         "google sheets"
        , "master_codes"
        , "sic_code"
        , "1tDK5UA19WMW3ntYnnJw9mOD3w8eumwYW-EMXhO_uHjw"
        , "0"
        , "A:C"
     )
]
'''
column_names = [
     file_type      ##e.g. google_sheets
    , google_sheet_filename
    , google_sheet_sheetname
    , google_sheet_urlfilename
    , google_sheet_urlsheetname
    , google_sheet_urlcellrange
]
'''

##prepare webbrowser
webbrowser.open( "https://www.google.com", new = 0 )
time.sleep( 10 )


##official start downloading
##default path will go to downloads folder
for google_sheet in google_sheet_list:
    webbrowser.open(
         "https://docs.google.com/spreadsheets/d/{gs_filename}/export?format=csv&gid={gs_sheetname}&range={gs_cellrange}".format(
             gs_filename = google_sheet[ 3 ]
            , gs_sheetname = google_sheet[ 4 ]
            , gs_cellrange = google_sheet[ 5 ]
         )
        , new = 0
    )
    time.sleep( 7 )


##move files to designated folders
download_folder = Path( os.environ[ "USERPROFILE" ], "Downloads" )

for google_sheet in google_sheet_list:
    if google_sheet[ 0 ] == "google sheets":
        shutil.move(
             Path( download_folder, google_sheet[ 1 ] + " - " + google_sheet[ 2 ] + ".csv" )
            , Path( working_folder, google_sheet[ 2 ] + ".csv" )
        )
    time.sleep( 3 )


##=================================
##  upload to sqlite3
##=================================

csv_files = [
     ( "ticker_sic", "" )
    , ( "company_tickers_exchange", "" )
    , ( "sic_code", "" )
]
'''
column_name = [
     "csv_file_name"
    , "unassigned"
]
'''

for idx, csv_file in enumerate( csv_files ):
    ##create dataframe
    df = pds.read_csv( Path( working_folder, csv_file[ 0 ] + ".csv" ) )

    df_colnum = df.shape[ 1 ]
    q_mark = "?"
    q_marks = ",".join( q_mark * df_colnum )

    df_columns = list( df.columns )
    df_col_str = " text,".join( [ str( col ) for col in df_columns ] ) + " text"
    # print( df_col_str )

    rows = df.values.tolist( )


    sqlite3_db = otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        , folder_extend = "sqlite3"
    )
    db_name = proj_class_name

    connection = sqlite3.connect( Path( sqlite3_db, db_name + ".db" ) )


    cursor = connection.cursor( )

    cursor.execute(
        "drop table if exists {table_name}".\
            format(
                 table_name = csv_file[ 0 ]
            )
    )

    cursor.execute(
        "create table if not exists {table_name}( {column_names} )".\
            format(
                 table_name = csv_file[ 0 ]
                , column_names = df_col_str
            )
    )

    cursor.executemany(
         "insert into {table_name} values( {q_marks} )".\
            format(
                 table_name = csv_file[ 0 ]
                , q_marks = q_marks
            )
        , rows
    )

    connection.commit( )
    connection.close( )



##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
