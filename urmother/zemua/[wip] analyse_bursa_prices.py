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
import sweep
from bs4 import BeautifulSoup as bsp
import mechanicalsoup as msp
import ast


##<< log tracking ===================

drive_name = "G"
proj_class_name = "My Drive/projects/zemua"
task_name = "equities_prices"
save_log_folder = \
    otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        ,folder_extend = proj_class_name
        ,subfolder_extend = "python_logs"
    )
manual_assign_file = "<<assign_name_manually_if_applicable>>" + ".py"
auto_assign_file = lambda x : __file__ if x == "win32" else manual_assign_file
file_name = os.path.basename( auto_assign_file( sys.platform ) )
is_log_update_1 = 0 ##activate this to 1 if report refresh update is needed

lg.log_setup(
     save_log_folder
    ,file_name
    ,is_log_update_1 = is_log_update_1
)

##log tracking>> ===================

lg.log_script_start( )



##=====================================
##  INSERT FUNCTION HERE
##=====================================


##=================================
##  INSERT CODE HERE
##=================================

try:
    tagging = "set_folder_paths"
    lg.log_subscript_start( tagging, "setting folder paths..." )

    working_folder = \
        otomkdir.otomkdir.auto_create_folder_2(
             driver_name = drive_name
            ,folder_extend = proj_class_name
            ,subfolder_extend = task_name
        )
    csv_folder = \
        otomkdir.otomkdir.auto_create_folder(
             default_path = working_folder
            , folder_extend = f"""csv"""
        )
    analysis_folder = \
        otomkdir.otomkdir.auto_create_folder(
             default_path = working_folder
            , folder_extend = f"""analysis"""
        )

    lg.log_subscript_finish( tagging, "folder paths set" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "compile_files"
    lg.log_subscript_start( tagging, "compiling files..." )

    file_list = \
        sweep.swiip.glob_compile_list(
             scan_folder_path = csv_folder
            , scan_file_regex_string = "bursamalaysia_html_scrape_*"
            , scan_file_format = "csv"
        )
    
    df_compile_list = [ ]
    for file in file_list:
        filename = file.split( "." )[ 0 ].split( "\\" )[ -1 ]
        timestamp = filename.split( "_" )[ -2 ]
        if "-" in filename:
            pass
        else:
            df = pds.read_csv( Path( file ), encoding = "utf-8" )
            df.loc[ :, "scraped_date" ] = timestamp
            df_compile_list.append( df )

    for idx, df in enumerate( df_compile_list ):
        if idx == 0:
            df_equities_prices = df
        else:
            df_equities_prices = pds.concat( [ df_equities_prices, df ], ignore_index = True )
    

    lg.log_subscript_finish( tagging, "files compiled" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "transform"
    lg.log_subscript_start( tagging, "transforming data..." )
    
    checkpoint = "checkpoint 1.0"
    df_equities_transf_wip = df_equities_prices.copy( )
    df_equities_transf_wip.loc[ :, "scraped_date" ] = df_equities_transf_wip.loc[ :, "scraped_date" ].apply( lambda x : pds.to_datetime( x, format = "%Y%m%d" ) )
    df_equities_transf_wip.loc[ :, "stock_shortname" ] = df_equities_transf_wip.loc[ :, "Name" ].apply( lambda x : x.replace( "amp;", "" ).upper( ).split( "[" )[ 0 ].split( "-" )[ 0 ] )
    df_equities_transf_wip.loc[ :, "is_underlying_stock" ] = df_equities_transf_wip.loc[ :, "Name" ].apply( lambda x : 1 if "-" not in x else 0 )
    df_equities_transf_wip.loc[ :, "char_num_stockcode" ] = df_equities_transf_wip.loc[ :, "Code" ].apply( lambda x : len( x ) )
    df_equities_transf_wip.loc[ :, "last_closing_price" ] = \
        df_equities_transf_wip.apply(
             lambda row:
                 float( row[ "LACP" ] ) if sweep.swiip.isfloat( row[ "Last Done" ] ) is False and sweep.swiip.isfloat( row[ "LACP" ] )
                else float( row[ "Last Done" ] ) if sweep.swiip.isfloat( row[ "Last Done" ] )
                else 0
            , axis = 1
        )
    df_equities_transf_wip.loc[ :, "traded_volume" ] = df_equities_transf_wip.loc[ :, "Vol ('00)" ].apply( lambda x : int( x.replace( ",", "" ) ) * 100 if x != "-" else 0 )
    df_equities_transf_wip.loc[ :, "traded_value" ] = df_equities_transf_wip.apply( lambda row : row[ "traded_volume" ] * row[ "last_closing_price" ], axis = 1 )

    checkpoint = "checkpoint 1.1"
    df_underlying_companies = \
        df_equities_transf_wip.sort_values(
             by = [ "traded_value" ]
            , ascending = [ False ]
        )
    df_underlying_companies = \
        df_underlying_companies[
             ( df_underlying_companies[ "is_underlying_stock" ] == 1 )
        ]
    df_underlying_companies = df_underlying_companies.reset_index( drop = True )
    df_underlying_companies.to_csv( Path( analysis_folder, f"""underlying_companies_full.csv""" ), index = False )

    checkpoint = "checkpoint 1.2"
    df_underlying_companies_traded = \
        df_underlying_companies[
             ( df_underlying_companies[ "traded_value" ] != 0 )
        ]
    df_underlying_companies_traded = df_underlying_companies_traded.reset_index( drop = True )

    lg.log_subscript_finish( tagging, "data transformed" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( f"""{exception} -- {checkpoint}""" )
    lg.update_log_status_1( is_log_update_1, 0, tagging )



##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
