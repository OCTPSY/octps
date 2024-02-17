import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir

##additional modules
import glob
import pandas as pds


##<< log tracking ===================

var_proj_class_name = "urmother"
var_task_name = "process_data"
var_save_log_folder = \
    otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        ,folder_extend = var_proj_class_name
        ,subfolder_extend = "python_logs"
    )
var_file_name = os.path.basename( __file__ )
var_is_log_update_1 = 0 ##activate this to 1 if report refresh update is needed

lg.log_setup(
     var_save_log_folder
    ,var_file_name
    ,is_log_update_1 = var_is_log_update_1
)

##log tracking>> ===================

lg.log_script_start( )



##=================================
##  INSERT CODE HERE
##=================================

var_working_folder = \
    otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        , folder_extend = "urmother/eagle_eye"
        , subfolder_extend = "download_csv"
    )


##compile csv from folder list
glob_folder_subfolder = str( Path( var_working_folder ) ) + r"\\"
glob_save = str(
     otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        , folder_extend = "urmother/eagle_eye"
     )
) + r"\\"

pattern = glob_folder_subfolder + "*.csv"
keyword_file = glob.glob( pattern )

csv_files = [ os.path.abspath( x ) for x in keyword_file ]


# first_csv = pds.read_csv( csv_files[ 0 ] )
# col_names = first_csv.columns.str.lower( ).values.tolist( )
# df_compile = pds.DataFrame( columns = col_names )
df_compile = pds.DataFrame( )

for csv_file in csv_files:
    file_name = csv_file.split( "\\" )[ -1 ]
    df = pds.read_csv( csv_file )
    df[ "file_name" ] = file_name
    df_compile = df_compile.append( df )

print( df_compile.to_string( ) )


##compile for folder list
var_compile_list = [
     ( "x", "y" )
    ,( "a", "b" )
]

var_save_file_keywords = [
     ( "save_files", "saving", "saved" )
    ,( "backup_files", "backing up", "backed up" )
    ,( "distr_files", "distributing", "distributed" )
]

##sample looping
for var_idx, ( var_df, var_list ) in enumerate( zip( var_df_list, var_compile_list ) ):
    for var_idy, var_keyword in enumerate( var_save_file_keywords ):
        var_timestamp_01 = datetime.datetime.now( ).strftime( "%Y%m%d-%H%M" )
        try:
            var_tagging = var_keyword[ 0 ] + "_" + var_list[ 1 ] + "_" + str( var_idx ) + "_" + str( var_idy )
            lg.log_subscript_start( var_tagging, var_keyword[ 1 ] + " " + var_list[ 1 ] + "..." )

            if var_keyword[ 0 ] == "backup_files":
                var_backup_folder = otomkdir.otomkdir.auto_create_folder_2( driver_name = "D", folder_extend = var_proj_class_name + "/" + var_task_name, subfolder_extend = "backup/" + var_list[ 0 ] + "/history" )
                var_df.to_csv( Path( var_backup_folder, var_list[ 1 ] + " " + var_timestamp_01 + ".csv" ), index = False )

            elif var_keyword[ 0 ] == "distr_files":
                var_distr_folder = otomkdir.otomkdir.auto_create_folder_2( driver_name = "D", folder_extend = "distr" + "/" + var_proj_class_name + "/" + var_task_name )
                var_df.to_csv( Path( var_distr_folder, var_list[ 1 ] + ".csv" ), index = False )

            elif var_keyword[ 0 ] == "save_files":
                var_save_folder = otomkdir.otomkdir.auto_create_folder_2( driver_name = "D", folder_extend = var_proj_class_name + "/" + var_task_name )
                var_df.to_csv( Path( var_save_folder, var_list[ 1 ] + ".csv" ), index = False )

            lg.log_subscript_finish( var_tagging, var_list[ 1 ] + " " + var_keyword[ 2 ] )
            lg.update_log_status_1( var_is_log_update_1, 1, var_tagging )

        except Exception as var_exception:
            lg.log_exception( var_exception )
            lg.update_log_status_1( var_is_log_update_1, 0, var_tagging )

            time.sleep( 0.5 )



##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
