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

def get_main_pages(
         log_tagging
        , target
        , main_url
        , timestamp_keycode
        , save_folder
    ):
    ##log_tagging : refers to the log tagging in logtrek
    ##target : name of the scraping site
    ##main_url : self explanatory
    ##timestamp_keycode : unique key to identify the very file/s that need to be scraped for different scraping operations -- format == f"""%Y%m%d_{lg.random_string( 5 )}"""
    ##save_folder: self-explanatory

    url_page_format = "market_information/equities_prices?page="
    
    browser = msp.StatefulBrowser( user_agent = sweep.swiip.get_random_useragent( ) )

    ##obtain max number of pages
    first_page = f"""{main_url}/{url_page_format}1"""
    browser.open( first_page )
    extract = browser.page.find_all( "li", attrs = { "class":"paginate_button page-item" } )[ -1 ]
    max_page = int( extract.find( "a" ).string )

    ##get html for all pages
    for pages in range( 1, max_page + 1 ):
        try:
            subtagging = f"""{log_tagging}_save_page_{str( pages )}"""
            lg.log_subscript_start( subtagging, f"""saving page {str( pages )}...""" )

            url_pagination = f"""{main_url}/{url_page_format}{pages}"""

            browser.open( url_pagination )
            gethtml = browser.get_current_page( )
            with open( Path( save_folder, f"""{target}_html_{timestamp_keycode} page_{str( pages )}.txt""" ), "w", encoding = "utf-8" ) as html_txt:
                html_txt.write( str( gethtml ) )

            time.sleep( 0.7 )

            lg.log_subscript_finish( subtagging, f"""page {str( pages )} saved""" )
            lg.update_log_status_1( is_log_update_1, 1, subtagging )

        except Exception as exception:
            lg.log_exception( f"""{exception} -- stopped at page: {str( pages )}""" )
            lg.update_log_status_1( is_log_update_1, 0, subtagging )


def scrape_data(
         log_tagging
        , target
        , timestamp_keycode
        , html_save_folderpath
        , csv_save_folderpath
    ):
    ##stopped_listing_index : if there is an event that terminated the scraping operations midway, use this variable to pick up where we left off

    try:
        subtagging = f"""{log_tagging}_compile_htmls"""
        lg.log_subscript_start( subtagging, f"""compiling html files...""" )

        html_save_filename = f"""{target}_html_{timestamp_keycode}"""
        html_compile_list = \
            sweep.swiip.glob_compile_list(
                 scan_folder_path = html_save_folderpath
                , scan_file_regex_string = f"""{html_save_filename} page_*"""
                , scan_file_format = "txt"
            )

        lg.log_subscript_finish( subtagging, f"""html files compiled""" )
        lg.update_log_status_1( is_log_update_1, 1, subtagging )

    except Exception as exception:
        lg.log_exception( exception )
        lg.update_log_status_1( is_log_update_1, 0, subtagging ) 


    scrape_list_raw = [ ]
    for idx, html_file in enumerate( html_compile_list ):
        checkpoint = "checkpoint 0.0"
        list_page = html_file.split( "." )[ 0 ].split( "_" )[ -1 ]

        checkpoint = "checkpoint 1.0"
        try:
            subtagging = f"""{log_tagging}_scrapehtml_{idx}"""
            lg.log_subscript_start( subtagging, f"""scraping html page {str( list_page )}...""" )

            with open( Path( html_save_folderpath, f"""{html_save_filename} page_{list_page}.txt""" ), "r", encoding = "utf-8" ) as html_file:
                read_html = bsp( html_file.read( ), "html.parser" )

            col_title_string = read_html.find_all( "div", attrs = { "class" : "label-dark" } )
            col_title_list = [ ]
            for indx, x in enumerate( col_title_string ):
                missing_cols = [ "No", "Name", "Code" ]
                if indx == 0:
                    col_title_list.extend( missing_cols )
                col_title_list.append( x.string )
                if x.string == "Low":
                    col_title_list.extend( [ "redundant_col" ] )
                    col_title_list.extend( missing_cols )


            json_string = read_html.find_all( "tbody" )[ 0 ]

            cycle = 0
            for idy, ( x, y ) in enumerate( zip( json_string.find_all( "td" ), col_title_list ) ):
                if "<span" in str( x ):
                    cell = str( x ).replace( "\n", "" ).replace( " ", "" ).split( ">" )[ -2 ].split( "<" )[ 0 ]
                    scrape_list_raw.append( [ f"""{list_page}.{cycle}""", cell, y ] )
                elif "<div" in str( x ):
                    cell = str( x ).replace( "\n", "" ).replace( " ", "" ).split( ">" )[ -3 ].split( "<" )[ 0 ]
                    scrape_list_raw.append( [ f"""{list_page}.{cycle}""", cell, y ] )
                elif '''class="stock-id hidden">''' in str( x ):
                    pass
                else:
                    x = x.string
                    cell = str( x ).replace( "\n", "" ).replace( " ", "" )
                    scrape_list_raw.append( [ f"""{list_page}.{cycle}""", cell, y ] )
                
                if ( idy + 1 ) % 16 == 0:
                    cycle += 1


            lg.log_subscript_finish( subtagging, f"""html page {str( list_page )} done""" )
            lg.update_log_status_1( is_log_update_1, 1, subtagging )

        except Exception as exception:
            lg.log_exception( f"""{exception} -- {checkpoint}""" )
            lg.update_log_status_1( is_log_update_1, 0, subtagging )


    try:
        subtagging = f"""{log_tagging}_transform_scrape"""
        lg.log_subscript_start( subtagging, f"""transforming scraped data...""" )

        checkpoint = "checkpoint 1.0"
        df_scrape_raw = pds.DataFrame( scrape_list_raw, columns = [ "row_index", "values", "col_titles" ] )
        df_scrape = \
            df_scrape_raw.pivot(
                 index = [ "row_index" ]
                , columns = [ "col_titles" ]
                , values = [ "values" ]
            )
        df_cols = [ ]
        for x in df_scrape.columns:
            df_cols.append( x[ 1 ] )
        df_scrape.columns = df_cols
        df_scrape = df_scrape.reset_index( col_level = 0, drop = True )

        checkpoint = "checkpoint 1.1"
        df_scrape.loc[ :, "No" ] = df_scrape.loc[ :, "No" ].apply( lambda x : int( x ) )
        df_scrape = \
            df_scrape.sort_values(
                 by = [ "No" ]
                , ascending = [ True ]
            )
        df_scrape = \
            df_scrape[
                 [
                     "No"
                    , "Name"
                    , "Code"
                    , "REM"
                    , "Last Done"
                    , "LACP"
                    , "CHG"
                    , "%CHG"
                    , "Vol ('00)"
                    , "Buy Vol ('00)"
                    , "Buy"
                    , "Sell"
                    , "Sell Vol ('00)"
                    , "High"
                    , "Low"
                 ]
            ]

        lg.log_subscript_finish( subtagging, f"""scraped data transformed""" )
        lg.update_log_status_1( is_log_update_1, 1, subtagging )

    except Exception as exception:
        lg.log_exception( f"""{exception} -- {checkpoint}""" )
        lg.update_log_status_1( is_log_update_1, 0, subtagging )


    try:
        subtagging = f"""{log_tagging}_save_csv"""
        lg.log_subscript_start( subtagging, f"""saving as csv file...""" )

        csv_save_file_name = f"""{target}_html_scrape_{timestamp_keycode}"""
        csv_file_path = Path( csv_save_folderpath, f"""{csv_save_file_name}.csv""" )

        timestamp_1 = datetime.datetime.now( ).strftime( "%Y%m%d-%H%M-%S" )
        df_scrape.to_csv( Path( csv_save_folderpath, f"""{csv_save_file_name}.csv""" ), index = False )
        df_scrape.to_csv( Path( csv_save_folderpath, f"""{csv_save_file_name} {timestamp_1}.csv""" ), index = False )


        lg.log_subscript_finish( subtagging, f"""csv file saved""" )
        lg.update_log_status_1( is_log_update_1, 1, subtagging )

    except Exception as exception:
        lg.log_exception( f"""{exception} -- {checkpoint}""" )
        lg.update_log_status_1( is_log_update_1, 0, subtagging )





##=================================
##  INSERT CODE HERE
##=================================

try:
    tagging = "set_folder_paths_variables"
    lg.log_subscript_start( tagging, "setting folder paths and variables..." )

    working_folder = \
        otomkdir.otomkdir.auto_create_folder_2(
             driver_name = drive_name
            ,folder_extend = proj_class_name
            ,subfolder_extend = task_name
        )
    html_folder = \
        otomkdir.otomkdir.auto_create_folder(
             default_path = working_folder
            , folder_extend = f"""html"""
        )
    csv_folder = \
        otomkdir.otomkdir.auto_create_folder(
             default_path = working_folder
            , folder_extend = f"""csv"""
        )

    target = "bursamalaysia"
    main_url = f"""https://www.{target}.com"""

    ##setting timesamp_keycode variable
    set_manual_assign = [ "20230129_mvtui", "2023-01-29 21:52:00" ]
    set_manual_assign_cols = [
         "timestamp_keycode_manual_assign"
        , "updated_at"
    ]
    if datetime.datetime.now( ) > datetime.datetime.strptime( set_manual_assign[ 1 ], "%Y-%m-%d %H:%M:%S" ) + datetime.timedelta( hours = 12 ):
        timestamp_keycode = f"""{datetime.datetime.now( ).strftime( "%Y%m%d" )}_{lg.random_string( 5 )}"""
    else:
        timestamp_keycode = set_manual_assign[ 0 ]

    lg.log_subscript_finish( tagging, "folder paths and variables set" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "run_functions"
    lg.log_subscript_start( tagging, "running functions..." )

    # get_main_pages(
    #      log_tagging = "get_main_pages"
    #     , target = target
    #     , main_url = main_url
    #     , timestamp_keycode = timestamp_keycode
    #     , save_folder = html_folder
    # )

    scrape_data(
         log_tagging = "scrape_data"
        , target = target
        , timestamp_keycode = timestamp_keycode
        , html_save_folderpath = html_folder
        , csv_save_folderpath = csv_folder
    )

    lg.log_subscript_finish( tagging, "functions run completed" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
