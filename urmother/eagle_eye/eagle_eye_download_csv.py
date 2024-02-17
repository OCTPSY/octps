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


##<< log tracking ===================

var_proj_class_name = "urmother"
var_task_name = "download_csv"
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
        ,folder_extend = "urmother/eagle_eye"
        ,subfolder_extend = var_task_name
    )


##compile for folder list
gs_filename_default = "eagle_eye_compile"
gs_urlfilename_default = "1Wc5Vw8YRz4R44bkk5wC89N072EM0ILImh5CTyHwWeF4"
gs_urlcellrange_default = "A1:H"
google_sheet_list = [
     (
         "google sheets"
        , gs_filename_default
        , "bodywash_400ml"
        , gs_urlfilename_default
        , "299914233"
        , gs_urlcellrange_default
     )
    , (
         "google sheets"
        , gs_filename_default
        , "bodywash_850ml"
        , gs_urlfilename_default
        , "1734965513"
        , gs_urlcellrange_default
      )
    , (
         "google sheets"
        , gs_filename_default
        , "conditioner_hair_320ml"
        , gs_urlfilename_default
        , "493468881"
        , gs_urlcellrange_default
      )
    , (
         "google sheets"
        , gs_filename_default
        , "handwash_500ml"
        , gs_urlfilename_default
        , "1148230139"
        , gs_urlcellrange_default
      )
    , (
         "google sheets"
        , gs_filename_default
        , "keyboard_gaming"
        , gs_urlfilename_default
        , "1111433770"
        , gs_urlcellrange_default
      )
    , (
         "google sheets"
        , gs_filename_default
        , "life_premium_great_eastern"
        , gs_urlfilename_default
        , "515449783"
        , gs_urlcellrange_default
      )
    , (
         "google sheets"
        , gs_filename_default
        , "shampoo_330ml"
        , gs_urlfilename_default
        , "1527465086"
        , gs_urlcellrange_default
      )
    , (
         "google sheets"
        , gs_filename_default
        , "shampoo_340ml"
        , gs_urlfilename_default
        , "1545937165"
        , gs_urlcellrange_default
      )
    , (
         "google sheets"
        , gs_filename_default
        , "toothpaste_225g"
        , gs_urlfilename_default
        , "1441758742"
        , gs_urlcellrange_default
      )
]
'''
column_names = [
     file_type                      ##e.g. google_sheets
    , google_sheet_filename         ##as per google sheet naming
    , google_sheet_sheetname        ##as per google sheet naming
    , google_sheet_urlfilename
    , google_sheet_urlsheetname
    , google_sheet_urlcellrange
]
'''

try:
    var_tagging = "open_browser"
    lg.log_subscript_start( var_tagging, "preparing browser..." )

    webbrowser.open( "https://www.google.com", new = 0 )
    time.sleep( 10 )

    lg.log_subscript_finish( var_tagging, "browser prepared" )
    lg.update_log_status_1( var_is_log_update_1, 1, var_tagging )

except Exception as var_exception:
    lg.log_exception( var_exception )
    lg.update_log_status_1( var_is_log_update_1, 0, var_tagging )




##download files
##default path will go to downloads folder
for idx, google_sheet in enumerate( google_sheet_list ):
    try:
        var_tagging = "download_files_" + str( idx )
        lg.log_subscript_start( var_tagging, "downloading " + google_sheet[ 2 ] + "..." )

        webbrowser.open(
             "https://docs.google.com/spreadsheets/d/{gs_filename}/export?format=csv&gid={gs_sheetname}&range={gs_cellrange}".format(
                 gs_filename = google_sheet[ 3 ]
                , gs_sheetname = google_sheet[ 4 ]
                , gs_cellrange = google_sheet[ 5 ]
             )
            , new = 0
        )

        lg.log_subscript_finish( var_tagging, google_sheet[ 2 ] + " downloaded" )
        lg.update_log_status_1( var_is_log_update_1, 1, var_tagging )

    except Exception as var_exception:
        lg.log_exception( var_exception )
        lg.update_log_status_1( var_is_log_update_1, 0, var_tagging )

    time.sleep( 7 )


##move files to target destination folder
download_folder = Path( os.environ[ "USERPROFILE" ], "Downloads" )

for idx, google_sheet in enumerate( google_sheet_list ):
    try:
        var_tagging = "move_file_" + str( idx )
        lg.log_subscript_start( var_tagging, "moving " + google_sheet[ 2 ] + "..." )

        if google_sheet[ 0 ] == "google sheets":
            shutil.move(
                 Path( download_folder, google_sheet[ 1 ] + " - " + google_sheet[ 2 ] + ".csv" )
                , Path( str( var_working_folder ), google_sheet[ 2 ] + ".csv" )
            )

        lg.log_subscript_finish( var_tagging, google_sheet[ 2 ] + " moved" )
        lg.update_log_status_1( var_is_log_update_1, 1, var_tagging )

    except Exception as var_exception:
        lg.log_exception( var_exception )
        lg.update_log_status_1( var_is_log_update_1, 0, var_tagging )

    time.sleep( 3 )



##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
