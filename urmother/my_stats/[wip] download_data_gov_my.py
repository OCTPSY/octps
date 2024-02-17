import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir

##additional modules
import pandas as pds
import webbrowser
import glob
import shutil


##<< log tracking ===================

proj_class_name = "my_stats"
task_name = "household"
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
##  INSERT FUNCTIONS HERE
##=================================

def open_browser( ):
    try:
        tagging = "open_browser"
        lg.log_subscript_start( tagging, "preparing browser..." )

        webbrowser.open( "https://www.google.com", new = 0 )
        time.sleep( 10 )

        lg.log_subscript_finish( tagging, "browser prepared" )
    except Exception as exception:
        lg.log_exception( exception )


def download_link( link ):
    webbrowser.open(
         link
        , new = 0
    )
    time.sleep( 7 )



##=================================
##  INSERT CODE HERE
##=================================

working_folder = \
    otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        ,folder_extend = proj_class_name
        ,subfolder_extend = task_name
    )

'''
##compile for household data from data.gov.my
compile_list = [
     ( "income", "mean-monthly-household-gross-income-by-state-malaysia", "mean_monthly_household_gross_income_by_state_malaysia", "https://www.data.gov.my/data/dataset/3ab13217-816f-4d74-8ecd-12fa9ab11ca2/resource/2a8493ce-bff3-4f0e-ae49-52ee7456c03a/download/_202108260434240_mean-monthly-household-gross-income-by-state-malaysia.csv" )
    , ( "income", "mean-monthly-household-gross-income-of-top-20-middle-40-and-bottom-40-of-househ", "mean_monthly_household_gross_income_of_t20_m40_b40_of_households_by_ethnicity", "https://www.data.gov.my/data/dataset/86b2a7ce-5bc0-4f04-a6dc-4a3fb4f121ce/resource/5d43dcfe-632d-4e38-9d7f-bdf6c28ad257/download/_202103260357150_mean-monthly-household-gross-income-of-top-20-middle-40-and-bottom-40-of-househ.csv" )
    , ( "expense", "mean-monthly-household-consumption-expenditure-2004-2019-malaysia", "mean_monthly_household_consumption_expenditure_malaysia", "https://www.data.gov.my/data/dataset/0a5d0be6-a1b7-4cbd-8f42-90fcbda380b5/resource/6243cc84-6eb9-4a7a-8de8-538337494a11/download/_202103091017540_mean-monthly-household-consumption-expenditure-2004-2019-malaysia.csv" )
]

open_browser( )
download_files = [ ]
for link in compile_list:
    try:
        tagging = "download_files"
        lg.log_subscript_start( tagging, f"downloading {link[ 2 ]}..." )

        download_link( link[ 3 ] )

        lg.log_subscript_finish( tagging, f"{link[ 2 ]} downloaded" )
    except Exception as exception:
        lg.log_exception( exception )
'''

download_folder = Path( os.environ[ "USERPROFILE" ], "Downloads" )
glob_download_folder = str( Path( download_folder ) ) + r"\\"
print( glob_download_folder )
glob_save = str( working_folder ) + r"\\"
glob_pattern = glob_download_folder + "*.csv"
print( glob_pattern )
glob_keyword = glob.glob( glob_pattern )
print( glob_keyword )

for file in glob_keyword:
    print( file )



'''
save_file_keywords = [
     ( "save_files", "saving", "saved" )
    , ( "backup_files", "backing up", "backed up" )
    , ( "distr_files", "distributing", "distributed" )
]

##sample looping
for idx, ( df, list ) in enumerate( zip( df_list, compile_list ) ):
    for idy, keyword in enumerate( save_file_keywords ):
        timestamp_01 = datetime.datetime.now( ).strftime( "%Y%m%d-%H%M" )
        try:
            tagging = keyword[ 0 ] + "_" + list[ 1 ] + "_" + str( idx ) + "_" + str( idy )
            lg.log_subscript_start( tagging, keyword[ 1 ] + " " + list[ 1 ] + "..." )

            if keyword[ 0 ] == "backup_files":
                backup_folder = otomkdir.otomkdir.auto_create_folder_2( driver_name = "D", folder_extend = proj_class_name + "/" + task_name, subfolder_extend = "backup/" + list[ 0 ] + "/history" )
                df.to_csv( Path( backup_folder, list[ 1 ] + " " + timestamp_01 + ".csv" ), index = False )

            elif keyword[ 0 ] == "distr_files":
                distr_folder = otomkdir.otomkdir.auto_create_folder_2( driver_name = "D", folder_extend = "distr" + "/" + proj_class_name + "/" + task_name )
                df.to_csv( Path( distr_folder, list[ 1 ] + ".csv" ), index = False )

            elif keyword[ 0 ] == "save_files":
                save_folder = otomkdir.otomkdir.auto_create_folder_2( driver_name = "D", folder_extend = proj_class_name + "/" + task_name )
                df.to_csv( Path( save_folder, list[ 1 ] + ".csv" ), index = False )

            lg.log_subscript_finish( tagging, list[ 1 ] + " " + keyword[ 2 ] )
            lg.update_log_status_1( is_log_update_1, 1, tagging )

        except Exception as exception:
            lg.log_exception( exception )
            lg.update_log_status_1( is_log_update_1, 0, tagging )

            time.sleep( 0.5 )
'''


##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
