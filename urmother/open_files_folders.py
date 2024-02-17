import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir
import inkript.enkrypt as encrypt
import sweep.swiip as swp
from sweep.swiip import timeit
import functools

##additional modules
from googleapiclient.discovery import build
from google.oauth2 import service_account
import sqlite3
import json
import webbrowser
import shutil
import sqlalchemy as sa
import numpy as npy
import pandas as pda
pda.set_option( "display.max_columns", None )
pda.set_option( "display.width", None )
import swifter
import csv
import chardet
import glob
import itertools as itert
from matplotlib_venn import venn2, venn2_circles, venn2_unweighted
from matplotlib_venn import venn3, venn3_circles, venn3_unweighted
from matplotlib import pyplot as plt


##<< log tracking ===================

drive_name = "D"
proj_class_name = "urmother"
# task_name = ""
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

def log_execution( func ):
    @functools.wraps( func )
    def wrapper( description_tag, description_start, description_end, *args, **kwargs ):
        try:
            tagging = f"{lg.random_string( 5 )}__{description_tag}"
            lg.log_subscript_start( tagging, f"{description_start}..." )

            result = func( description_tag, description_start, description_end, *args, **kwargs )

            lg.log_subscript_finish( tagging, f"{description_end}" )
            lg.update_log_status_1( is_log_update_1, 1, tagging )

        except Exception as exception:
            lg.log_exception( exception )
            lg.update_log_status_1( is_log_update_1, 0, tagging )

        return result
    return wrapper

##log tracking>> ===================

lg.log_script_start( )



##=====================================
##  INSERT FUNCTION HERE
##=====================================

##<<no functions yet>>


##=========================================================================
##=========================================================================

##USE NEW METHOD -- USING DECORATORS
@log_execution
def open_command(
         xx,yy,zz
        , file_folder_path
    ):
    os.startfile( file_folder_path )
    time.sleep( 0.5 )


##=================================
##  SET GLOBAL VARIABLES
##=================================

##USE OLD METHOD
try:
    tagging = "set_folder_paths"
    lg.log_subscript_start( tagging, "setting folder paths..." )

    working_folder = \
        otomkdir.otomkdir.auto_create_folder_2(
             driver_name = drive_name
            , folder_extend = proj_class_name
        )
    gdrive_folder = \
        otomkdir.otomkdir.auto_create_folder_2(
             driver_name = "G"
            , folder_extend = "My Drive"
        )
    github_folder = \
        otomkdir.otomkdir.auto_create_folder_2(
             driver_name = "C"
            , folder_extend = "Users/User/Documents/GitHub"
        )

    compile_dict = {
         "folder_list" : [
              ( "github folder", Path( github_folder ) )
         ]
        , "application_list" : [ ]  ##( "atom application", Path( <<folder_path>>, "<<application_path>>.exe" ) )
        , "file_list" : [
             ( "urmother gantt chart", Path( gdrive_folder, "urmother/gantt_chart/urmother_gantt_chart.png" ) )
          ]
    }
    
    ''' ##OLD FORMAT
    var_compile_list = [ ]

    ##compile for folder list
    var_folder_list = [
        ( "github folder", "C:/Users/cheon/Documents/GitHub" )
        ,( "py_mytukar", "C:/Users/cheon/AppData/Local/Programs/Python/Python310/Lib/py_modules_mytukar" )
    ]
    var_compile_list.extend( var_folder_list )

    ##compile for application list
    var_application_list = [
        ( "atom application", "C:/Users/cheon/AppData/Local/atom/app-1.58.0/atom.exe" )
    ]
    var_compile_list.extend( var_application_list )

    ##compile for file list
    var_file_list = [
        # ( "", "" )
    ]
    var_compile_list.extend( var_file_list )
    '''


    lg.log_subscript_finish( tagging, "folder paths set" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )



##=================================
##  INSERT CODE HERE
##=================================

try:
    tagging = "run_functions"
    lg.log_subscript_start( tagging, "running_functions..." )

    for idx, group_type in enumerate( compile_dict ):
        for idy, path_item in enumerate( compile_dict[ group_type ] ):
            open_command(
                 f"""{group_type}_open_command_{idx}_{idy}""", f"""open command for {path_item[ 0 ]} in {group_type}""", f"""open command completed"""
                , path_item[ 1 ]
            )

    lg.log_subscript_finish( tagging, "functions completed" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
