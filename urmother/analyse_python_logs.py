import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir
import inkript.enkrypt as encrypt

##additional modules
from googleapiclient.discovery import build
from google.oauth2 import service_account
import sqlite3
import json
import webbrowser
import shutil
import sqlalchemy as sa
import numpy as npy
import pandas as pds
pds.set_option( "display.max_columns", None )
pds.set_option( "display.width", None )
import csv
import chardet
import glob


##<< log tracking ===================

file_list = [
      [ "urmother",         "D",    "urmother"                                                              ]
    , [ "rapid_wealth",     "G",    ".shortcut-targets-by-id/1-dtquloyy7eE4eqaugsi0rcVLswJ7y6s/python"      ]
    , [ "ee_bom",           "D",    "ee_bom"                                                                ]
]

select_file = [
    "ee_bom",           "D",    "ee_bom"
]

drive_name = select_file[ 1 ]
proj_class_name = select_file[ 2 ]
task_name = "python_logs"
# save_log_folder = \
#     otomkdir.otomkdir.auto_create_folder_2(
#          driver_name = "D"
#         ,folder_extend = proj_class_name
#         ,subfolder_extend = "python_logs"
#     )
# manual_assign_file = "<<assign_name_manually_if_applicable>>" + ".py"
# auto_assign_file = lambda x : __file__ if x == "win32" else manual_assign_file
# file_name = os.path.basename( auto_assign_file( sys.platform ) )
# is_log_update_1 = 0 ##activate this to 1 if report refresh update is needed

# lg.log_setup(
#      save_log_folder
#     ,file_name
#     ,is_log_update_1 = is_log_update_1
# )

##log tracking>> ===================

# lg.log_script_start( )



##=================================
##  INSERT CODE HERE
##=================================

working_folder = Path( f"""{drive_name}:""", proj_class_name, task_name )

days = 0
timestamp_1 = (
     datetime.datetime.now( )
     +
     datetime.timedelta( days = days )
).strftime( "%Y-%m-%d")
log_folder = working_folder
file_format = "csv"
log_file_path = Path( log_folder, f"""python_logs {timestamp_1}.{file_format}""" )
log_column_names = [
     "pic_name"
    ,"file_name"
    ,"script_exec_key"
    ,"level_name"
    ,"timestamp"
    ,"log_message"
]

##analysing log filename
print( "PYTHON LOG FILENAME\n" + str( log_file_path ) + "\n" )


##debug analysis
debug_logs_df = pds.read_csv(
     log_file_path
    ,sep = "|"
    ,names = log_column_names
)
debug_logs_df = debug_logs_df.loc[
     debug_logs_df[ "level_name" ] == "DEBUG"
]
debug_logs_df = debug_logs_df[ debug_logs_df[ "file_name" ].str.contains( "scrape" ) == False ]
debug_logs_df = debug_logs_df[ debug_logs_df[ "log_message" ].str.contains( "error encountered:" ) == True ]
debug_logs_df = debug_logs_df.reset_index( )
print( "DEBUG CHECKING\n" + debug_logs_df.to_string( ) + "\n" )


##time analysis
logs_df = pds.read_csv(
     log_file_path
    ,sep = "|"
    ,names = log_column_names
)
logs_df[ [ "subscripts", "tags", "log_message" ] ] = logs_df[ "log_message" ].str.split( ":", 2, expand = True )

logs_df = logs_df.loc[
     (
          ( logs_df[ "subscripts" ] == "script started" )
          |
          ( logs_df[ "subscripts" ] == "subscript started" )
          |
          ( logs_df[ "subscripts" ] == "subscript finished" )
          |
          ( logs_df[ "subscripts" ] == "script finished" )
     )
     # &
     # ( logs_df[ "file_name" ] == "table_check" + ".py" )
]
logs_df = logs_df[ [
     "pic_name"
    ,"file_name"
    ,"script_exec_key"
    ,"level_name"
    ,"subscripts"
    ,"tags"
    ,"timestamp"
    ,"log_message"
] ]
print( "PREVIEW LOG FILE\n" + logs_df.to_string( max_rows = 6 ) + "\n" )


##pivot for summary
logs_df_summary = logs_df.loc[
     (
          ( logs_df[ "subscripts" ] == "script started" )
          |
          ( logs_df[ "subscripts" ] == "script finished" )
     )
]
logs_df_summary = logs_df_summary.drop_duplicates(
     subset = [
         "script_exec_key"
        ,"level_name"
        ,"subscripts"
        ,"tags"
     ]
    ,keep = "last"
)
logs_df_summary = logs_df_summary.pivot(
     index = [
         "pic_name"
        ,"file_name"
        ,"script_exec_key"
        ,"level_name"
     ]
    ,columns = "subscripts"
    ,values = "timestamp"
)
# print( logs_df_summary.to_string( max_rows = 20 ) )
logs_df_summary[ [ "script started", "script finished" ] ] = logs_df_summary[ [ "script started", "script finished" ] ].apply( pds.to_datetime )

logs_df_summary[ "time_diff_sec" ] = (
     logs_df_summary[ "script finished" ]
     -
     logs_df_summary[ "script started" ]
).dt.seconds
column_order = [
     "script started"
    ,"script finished"
    ,"time_diff_sec"
]

logs_df_summary = logs_df_summary.reindex( column_order, axis = 1 )

logs_df_summary[ "time_diff_sec_min" ] = logs_df_summary[ "time_diff_sec" ]
logs_df_summary[ "time_diff_sec_avg" ] = logs_df_summary[ "time_diff_sec" ]
logs_df_summary[ "time_diff_sec_mdn" ] = logs_df_summary[ "time_diff_sec" ]
logs_df_summary[ "time_diff_sec_max" ] = logs_df_summary[ "time_diff_sec" ]
# logs_df_summary = logs_df_summary.dropna( how = "any" )

logs_df_summary = logs_df_summary.sort_values(
     by = [ "script started", "file_name" ]
    ,axis = "rows"
    ,ascending = ( True, False )
)

logs_df_summary_pivot = pds.pivot_table(
     logs_df_summary
    ,index = [
         "pic_name"
        ,"file_name"
     ]
    ,columns = [ ]
    ,values = [
         "time_diff_sec_min"
        ,"time_diff_sec_avg"
        ,"time_diff_sec_mdn"
        ,"time_diff_sec_max"
     ]
    ,aggfunc = {
         "time_diff_sec_min" : "min"
        ,"time_diff_sec_avg" : "mean"
        ,"time_diff_sec_mdn" : "median"
        ,"time_diff_sec_max" : "max"
     }
)
values_order = [
     "time_diff_sec_min"
    ,"time_diff_sec_avg"
    ,"time_diff_sec_mdn"
    ,"time_diff_sec_max"
]
logs_df_summary_pivot = logs_df_summary_pivot.reindex( values_order, axis = 1 )


print( "VIEW SUMMARY BY FILE\n" + logs_df_summary_pivot.to_string( ) + "\n" )
print( "VIEW SUMMARY BY EXEC KEY\n" + logs_df_summary.to_string( ) + "\n" )



##pivot for details
logs_df_details = logs_df.loc[
     (
          ( logs_df[ "subscripts" ] == "subscript started" )
          |
          ( logs_df[ "subscripts" ] == "subscript finished" )
     )
]
logs_df_details = logs_df_details.drop_duplicates(
     subset = [
         "script_exec_key"
        ,"level_name"
        ,"subscripts"
        ,"tags"
     ]
    ,keep = "last"
)
logs_df_details = logs_df_details.pivot(
     index = [
         "pic_name"
        ,"file_name"
        ,"script_exec_key"
        ,"level_name"
        ,"tags"
     ]
    ,columns = "subscripts"
    ,values = "timestamp"
)
# print( logs_df_details.to_string( max_rows = 20 ) )
logs_df_details[ [ "subscript started", "subscript finished" ] ] = logs_df_details[ [ "subscript started", "subscript finished" ] ].apply( pds.to_datetime )

logs_df_details[ "time_diff_sec" ] = (
     logs_df_details[ "subscript finished" ]
     -
     logs_df_details[ "subscript started" ]
).dt.seconds
column_order = [
     "subscript started"
    ,"subscript finished"
    ,"time_diff_sec"
]

logs_df_details = logs_df_details.reindex( column_order, axis = 1 )

logs_df_details[ "time_diff_sec_min" ] = logs_df_details[ "time_diff_sec" ]
logs_df_details[ "time_diff_sec_avg" ] = logs_df_details[ "time_diff_sec" ]
logs_df_details[ "time_diff_sec_mdn" ] = logs_df_details[ "time_diff_sec" ]
logs_df_details[ "time_diff_sec_max" ] = logs_df_details[ "time_diff_sec" ]
# logs_df_details = logs_df_details.dropna( how = "any" )

logs_df_details = logs_df_details.sort_values(
     by = [ "file_name", "subscript started" ]
    ,axis = "rows"
    ,ascending = ( True, False )
)

logs_df_details_pivot = pds.pivot_table(
     logs_df_details
    ,index = [
         "pic_name"
        ,"file_name"
        ,"tags"
     ]
    ,columns = [ ]
    ,values = [
         "time_diff_sec_min"
        ,"time_diff_sec_avg"
        ,"time_diff_sec_mdn"
        ,"time_diff_sec_max"
     ]
    ,aggfunc = {
         "time_diff_sec_min" : "min"
        ,"time_diff_sec_avg" : "mean"
        ,"time_diff_sec_mdn" : "median"
        ,"time_diff_sec_max" : "max"
     }
)
values_order = [
     "time_diff_sec_min"
    ,"time_diff_sec_avg"
    ,"time_diff_sec_mdn"
    ,"time_diff_sec_max"
]
logs_df_details_pivot = logs_df_details_pivot.reindex( values_order, axis = 1 )


print( "VIEW DETAIL BY TAGS\n" + logs_df_details_pivot.to_string( ) + "\n" )
print( "VIEW DETAIL BY EXEC KEY\n" + logs_df_details.to_string( ) + "\n" )



##=================================
##  CODE ENDS HERE
##=================================

# lg.log_script_finish( )
