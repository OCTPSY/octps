import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir

##additional modules
import subprocess
import pandas as pda
pda.set_option( "display.max_columns", None )
pda.set_option( "display.width", None )
import schedule
import multiprocessing as mp


##<< log tracking ===================

proj_class_name = "urmother"
task_name = "python_logs"
save_log_folder = \
    otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        ,folder_extend = proj_class_name
        ,subfolder_extend = task_name
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

# lg.log_script_start( )



##=================================
##  INSERT CODE HERE
##=================================

##setup paths
python_loc = sys.executable

##folder that stores python files
octps_folder_path = \
    Path(
         os.environ[ 'USERPROFILE' ]
        , "Documents/GitHub"
        , "octps_invst/octps_python"
    )
urmother_folder_path = \
    Path(
         os.environ[ 'USERPROFILE' ]
        , "Documents/GitHub"
        , "urmother"
    )
rapid_folder_path = \
    Path(
         os.environ[ 'USERPROFILE' ]
        , "Documents/GitHub"
        , "rapid_wealth"
    )
eebom_folder_path = \
    Path(
         os.environ[ 'USERPROFILE' ]
        , "Documents/GitHub"
        , "ee_bom"
    )

timestamp_1 = datetime.datetime.now( ).strftime( "%Y-%m-%d" )

shell_output_folder = \
    otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        ,folder_extend = proj_class_name
        ,subfolder_extend = "py_shell_output"
    )


##compile ALL schedules
py_compile_list = [
    ##batch 00
      [ "projects_gantt_chart.py",                  "minutes",      "every_30mins",                 1,  urmother_folder_path    ]
    , [ "projects_log_report.py",                   "minutes",      "every_30mins",                 1,  urmother_folder_path    ]

    ##batch 01
    , [ "nonsubscriber_info_compile.py",            "daily",        "daily_refresh_0",              1,  rapid_folder_path       ]
    , [ "update_google_sheet_directories.py",       "daily",        "daily_refresh_0",              1,  rapid_folder_path       ]
    , [ "upload_google_sheets_to_database.py",      "daily",        "daily_refresh_0",              1,  rapid_folder_path       ]
    , [ "phoenix_reports.py",                       "daily",        "daily_refresh_0",              1,  rapid_folder_path       ]
    , [ "phoenix_dashboards.py",                    "daily",        "daily_refresh_0",              1,  rapid_folder_path       ]
    , [ "revenue_recognise_accrue_method_forecast_v2.py",
                                                    "daily",        "daily_refresh_0",              1,  rapid_folder_path       ]
    , [ "client_latest_journey.py",                 "daily",        "daily_refresh_0",              1,  rapid_folder_path       ]
    , [ "capture_sqlite_db_size.py",                "daily",        "daily_refresh_0",              1,  rapid_folder_path       ]

    ##batch 02
    , [ "scraping_hadoom_new.py",                   "monthly",     "first_daymth_scrape",           1,  eebom_folder_path       ]
    , [ "filter_hadoom_scraping_analysis.py",       "monthly",     "first_daymth_scrape",           0,  eebom_folder_path       ]

    , [ "scraping_slitrac_new.py",                  "monthly",     "second_daymth_scrape",          1,  eebom_folder_path       ]
    , [ "filter_my_scraping_analysis.py",           "monthly",     "second_daymth_scrape",          1,  eebom_folder_path       ]

    , [ "scraping_bdounibank.py",                   "monthly",     "third_daymth_scrape",           1,  eebom_folder_path       ]
    , [ "scraping_philkotse.py",                    "monthly",     "third_daymth_scrape",           1,  eebom_folder_path       ]
    , [ "filter_philkotse_scraping_analysis.py",    "monthly",     "third_daymth_scrape",           0,  eebom_folder_path       ]
    , [ "scraping_autodeal.py",                     "monthly",     "third_daymth_scrape",           1,  eebom_folder_path       ]
    , [ "scraping_autodeal_newcars.py",             "monthly",     "third_daymth_scrape",           1,  eebom_folder_path       ]
    , [ "scraping_carmudi_ph_newcars.py",           "monthly",     "third_daymth_scrape",           1,  eebom_folder_path       ]
    , [ "scraping_carousell_v2.py",                 "monthly",     "third_daymth_scrape",           1,  eebom_folder_path       ]
    , [ "filter_ph_scraping_analysis.py",           "monthly",     "third_daymth_scrape",           1,  eebom_folder_path       ]

    , [ "ph_compare_price_depn_v4.py",              "monthly",     "fourth_daymth_scrape",          1,  eebom_folder_path       ]
    , [ "scraping_olx.py",                          "monthly",     "fourth_daymth_scrape",          1,  eebom_folder_path       ]

    , [ "scraping_mobil123.py",                     "monthly",     "fifth_daymth_scrape",           1,  eebom_folder_path       ]
    , [ "filter_id_scraping_analysis.py",           "monthly",     "fifth_daymth_scrape",           1,  eebom_folder_path       ]

    # ##<< ===================================================================================================================================
    # , [ "scraping_one2car.py",                      "monthly",     "sixth_daymth_scrape",           1,  eebom_folder_path       ]
    # , [ "filter_one2car_scraping_analysis.py",      "monthly",     "sixth_daymth_scrape",           1,  eebom_folder_path       ]
    # ##>> ===================================================================================================================================

    ##batch 03
    , [ "scraping_hadoom_new.py",                   "monthly",     "fifteenth_daymth_scrape",       1,  eebom_folder_path       ]
    , [ "filter_hadoom_scraping_analysis.py",       "monthly",     "fifteenth_daymth_scrape",       0,  eebom_folder_path       ]

    , [ "scraping_slitrac_new.py",                  "monthly",     "sixteenth_daymth_scrape",       1,  eebom_folder_path       ]
    , [ "filter_my_scraping_analysis.py",           "monthly",     "sixteenth_daymth_scrape",       1,  eebom_folder_path       ]

    , [ "scraping_bdounibank.py",                   "monthly",     "seventeenth_daymth_scrape",     1,  eebom_folder_path       ]
    , [ "scraping_philkotse.py",                    "monthly",     "seventeenth_daymth_scrape",     1,  eebom_folder_path       ]
    , [ "filter_philkotse_scraping_analysis.py",    "monthly",     "seventeenth_daymth_scrape",     0,  eebom_folder_path       ]
    , [ "scraping_autodeal.py",                     "monthly",     "seventeenth_daymth_scrape",     1,  eebom_folder_path       ]
    , [ "scraping_autodeal_newcars.py",             "monthly",     "seventeenth_daymth_scrape",     1,  eebom_folder_path       ]
    , [ "scraping_carmudi_ph_newcars.py",           "monthly",     "seventeenth_daymth_scrape",     1,  eebom_folder_path       ]
    , [ "scraping_carousell_v2.py",                 "monthly",     "seventeenth_daymth_scrape",     1,  eebom_folder_path       ]
    , [ "filter_ph_scraping_analysis.py",           "monthly",     "seventeenth_daymth_scrape",     1,  eebom_folder_path       ]

    , [ "ph_compare_price_depn_v4.py",              "monthly",     "eighteenth_daymth_scrape",      1,  eebom_folder_path       ]
    , [ "scraping_olx.py",                          "monthly",     "eighteenth_daymth_scrape",      1,  eebom_folder_path       ]

    , [ "scraping_mobil123.py",                     "monthly",     "nineteenth_daymth_scrape",      1,  eebom_folder_path       ]
    , [ "filter_id_scraping_analysis.py",           "monthly",     "nineteenth_daymth_scrape",      1,  eebom_folder_path       ]

    # ##<< ===================================================================================================================================
    # , [ "scraping_one2car.py",                      "monthly",     "twentieth_daymth_scrape",       0,  eebom_folder_path       ]
    # , [ "filter_one2car_scraping_analysis.py",      "monthly",     "twentieth_daymth_scrape",       0,  eebom_folder_path       ]
    # ##>> ===================================================================================================================================

    ##batch 04
    , [ "scrape_bursa_announce_header.py",          "friday",      "friday_refresh_0",              0,  octps_folder_path       ]
    , [ "scrape_bursa_icap_announce_details.py",    "friday",      "friday_refresh_0",              0,  octps_folder_path       ]
    , [ "upload_icap_nav_to_sqlite.py",             "friday",      "friday_refresh_0",              0,  octps_folder_path       ]
    , [ "chart_icap_nav.py",                        "friday",      "friday_refresh_0",              0,  octps_folder_path       ]
]
col_names = [
     "script_name"          ##{ "description" : "name of python file/s" }
    , "frequency"           ##{ "values" : [ "minutes", "daily", { "weekly" : [ "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday" ] }, "monthly" ]
    , "schedule_name"       ##{ "description" : "name of shedule that need to be created under the 'compile schedules' section"
    , "is_active"           ##{ "values" : [ 0, 1 ] }
    , "github_folder_path"  ##{ "description" : "folder path that stores 'script name' files" }
]
df_0 = pda.DataFrame( py_compile_list, columns = col_names )


##setup templates
def minutes_refresh_template( schedule_name ):
    df = df_0.loc[
         ( df_0[ "is_active" ] == 1 )
         &
         ( df_0[ "schedule_name" ] == schedule_name )
         &
         (
             ( df_0[ "frequency" ] == "minutes" )
         )
    ]
    df = df[ [ "github_folder_path", "script_name" ] ]
    py_file_list = df[ "script_name" ].tolist( )
    py_folder_list = df[ "github_folder_path" ].tolist( )


    try:
        tagging = "generate_" + schedule_name
        lg.log_subscript_start( tagging, "generating schedule for " + str( py_file_list ) )

        print( py_file_list )

        lg.log_subscript_finish( tagging, "schedule generated" )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 1, tagging )

    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 0, tagging )


    for py_folder, py_file in zip( py_folder_list, py_file_list ):
        with open( Path( shell_output_folder, timestamp_1 + ".txt" ), "a+" ) as shell_output_file:
            subprocess.run(
                 [ python_loc, Path( py_folder, py_file ) ]
                ,shell = True
                ,stdout = shell_output_file
            )
        time.sleep( 3 )

    shell_output_file.close( )


def daily_refresh_template( schedule_name ):
    today = datetime.datetime.now( )
    if today.weekday( ) > 4:
        today_type = "weekend"
    else:
        today_type = "weekday"

    df = df_0.loc[
         ( df_0[ "is_active" ] == 1 )
         &
         ( df_0[ "schedule_name" ] == schedule_name )
         &
         (
             ( df_0[ "frequency" ] == today_type )
             |
             ( df_0[ "frequency" ] == "daily" )
         )
    ]
    df = df[ [ "github_folder_path", "script_name" ] ]
    py_file_list = df[ "script_name" ].tolist( )
    py_folder_list = df[ "github_folder_path" ].tolist( )


    try:
        tagging = "generate_" + schedule_name
        lg.log_subscript_start( tagging, "generating schedule for " + str( py_file_list ) )

        print( py_file_list )

        lg.log_subscript_finish( tagging, "schedule generated" )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 1, tagging )

    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 0, tagging )


    for py_folder, py_file in zip( py_folder_list, py_file_list ):
        with open( Path( shell_output_folder, timestamp_1 + ".txt" ), "a+" ) as shell_output_file:
            subprocess.run(
                 [ python_loc, Path( py_folder, py_file ) ]
                ,shell = True
                ,stdout = shell_output_file
            )
        time.sleep( 3 )

    shell_output_file.close( )


def weekly_refresh_template( schedule_name, frequency ):
    df = df_0.loc[
         ( df_0[ "is_active" ] == 1 )
         &
         ( df_0[ "schedule_name" ] == schedule_name )
         &
         ( df_0[ "frequency" ] == frequency )
    ]
    df = df[ [ "github_folder_path", "script_name" ] ]
    py_file_list = df[ "script_name" ].tolist( )
    py_folder_list = df[ "github_folder_path" ].tolist( )


    try:
        tagging = "generate_" + schedule_name
        lg.log_subscript_start( tagging, "generating schedule for " + str( py_file_list ) )

        print( py_file_list )

        lg.log_subscript_finish( tagging, "schedule generated" )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 1, tagging )

    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 0, tagging )


    for py_folder, py_file in zip( py_folder_list, py_file_list ):
        with open( Path( shell_output_folder, timestamp_1 + ".txt" ), "a+" ) as shell_output_file:
            subprocess.run(
                 [ python_loc, Path( py_folder, py_file ) ]
                ,shell = True
                ,stdout = shell_output_file
            )
        time.sleep( 3 )

    shell_output_file.close( )


def monthly_refresh_template( schedule_name, day_date ):
    if day_date == int( datetime.datetime.now( ).strftime( "%d" ) ):

        df = df_0.loc[
             ( df_0[ "is_active" ] == 1 )
             &
             ( df_0[ "schedule_name" ] == schedule_name )
             &
             ( df_0[ "frequency" ] == "monthly" )
        ]
        df = df[ [ "github_folder_path", "script_name" ] ]
        py_file_list = df[ "script_name" ].tolist( )
        py_folder_list = df[ "github_folder_path" ].tolist( )


        try:
            tagging = "generate_" + schedule_name
            lg.log_subscript_start( tagging, "generating schedule for " + str( py_file_list ) )

            print( py_file_list )

            lg.log_subscript_finish( tagging, "schedule generated" )

            ##update report refresh status
            lg.update_log_status_1( is_log_update_1, 1, tagging )

        except Exception as exception:
            lg.log_exception( exception, with_beep = 0 )

            ##update report refresh status
            lg.update_log_status_1( is_log_update_1, 0, tagging )


        for py_folder, py_file in zip( py_folder_list, py_file_list ):
            with open( Path( shell_output_folder, timestamp_1 + ".txt" ), "a+" ) as shell_output_file:
                subprocess.run(
                     [ python_loc, Path( py_folder, py_file ) ]
                    ,shell = True
                    ,stdout = shell_output_file
                )
            time.sleep( 3 )

        shell_output_file.close( )

    else:
        pass


def run_pending( ):
    while True:
        schedule.run_pending( )
        time.sleep( 1 )



##compile schedules
def every_30mins( ):
    try:
        tagging = "every_30mins"
        lg.log_subscript_start( tagging, "run schedule for " + tagging )

        minutes_refresh_template(
             schedule_name = tagging
        )

        lg.log_subscript_finish( tagging, "schedule done" )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 1, tagging )

    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def daily_refresh_0( ):
    try:
        tagging = "daily_refresh_0"
        lg.log_subscript_start( tagging, "run schedule for " + tagging )

        daily_refresh_template(
             schedule_name = tagging
        )

        lg.log_subscript_finish( tagging, "schedule done" )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 1, tagging )

    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def friday_refresh_0( ):
    try:
        tagging = "friday_refresh_0"
        lg.log_subscript_start( tagging, "run schedule for " + tagging )

        weekly_refresh_template(
             schedule_name = tagging
            , frequency = "friday"
        )

        lg.log_subscript_finish( tagging, "schedule done" )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 1, tagging )

    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )

        ##update report refresh status
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def first_daymth_scrape( ):
    try:
        tagging = "first_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 1
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def second_daymth_scrape( ):
    try:
        tagging = "second_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 2
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def third_daymth_scrape( ):
    try:
        tagging = "third_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 3
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def fourth_daymth_scrape( ):
    try:
        tagging = "fourth_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 4
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def fifth_daymth_scrape( ):
    try:
        tagging = "fifth_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 5
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def sixth_daymth_scrape( ):
    try:
        tagging = "sixth_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 6
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def fifteenth_daymth_scrape( ):
    try:
        tagging = "fifteenth_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 15
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def sixteenth_daymth_scrape( ):
    try:
        tagging = "sixteenth_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 16
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def seventeenth_daymth_scrape( ):
    try:
        tagging = "seventeenth_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 17
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def eighteenth_daymth_scrape( ):
    try:
        tagging = "eighteenth_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 18
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def nineteenth_daymth_scrape( ):
    try:
        tagging = "nineteenth_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 19
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )


def twentieth_daymth_scrape( ):
    try:
        tagging = "twentieth_daymth_scrape"
        lg.log_subscript_start( tagging, f"""run schedule for {tagging}""" )

        monthly_refresh_template(
             schedule_name = tagging
            , day_date = 20
        )

        lg.log_subscript_finish( tagging, "schedule done" )
        lg.update_log_status_1( is_log_update_1, 1, tagging )
    
    except Exception as exception:
        lg.log_exception( exception, with_beep = 0 )
        lg.update_log_status_1( is_log_update_1, 0, tagging )




###
###
# churn schedule templates
'''//churn schedule templates//

# ##checklist when setting windows task scheduler used to trigger python schedule:
#     GENERAL
#     [v] run only when user is logged on
#     [v] run with highest priviledges

#     TRIGGER
#     [v] at log on -- specific user
#     [v] at 12:00am every day

#     CONDITIONS
#     [v] wake the computer to run this task

#     SETTINGS
#     [v] allow task to be run on demand
#     [v] run task as soon as possible after a schedule start is missed
#     [v] if the running task does not end when requested, force it to stop
#     [v] if the task is already running, the following rule applies: {"STOP THE EXISTING INSTANCE"}

#     EXAMPLE
#     schedule_list = [
#           [ 'run_1000_00',       '',     'day',      '09:57:00',     'cs_scraping',          ''          ]
#         , [ 'run_1300_00s',      '',     'sunday',   '12:57:00',     'cs_scraping_sunday',   ''          ]
#         , [ 'run_evry_3mins',    '3',    'minutes',  ':08',          'check_rpt_rfrsh',      '18:30:00'  ]
#         , [ 'run_evry_3mins',    '3',    'minutes',  ':08',          'check_rpt_rfrsh',      '18:30:00'  ]
#         , [ 'run_day_01_1045_00, '',     'day',      '10:45:00',     'first_daymth_rfrsh',   ''          ]
#     ]


schedule_dataframe = "SCHEDULE LIST IN DATAFRAME\n"
schedule_list = [
      [ 'run_every30mins',      '30',   'minutes',      ':00',          'every_30mins',                 '19:00:00'  ]
    , [ 'run_daily_00',         '',     'day',          '09:30:00',     'daily_refresh_0',              ''          ]
    , [ 'run_fri_2300_00',      '',     'friday',       '23:00:00',     'friday_refresh_0',             ''          ]
    , [ 'run_day_01_1100_00',   '',     'day',          '11:00:00',     'first_daymth_scrape',          ''          ]
    , [ 'run_day_02_1100_00',   '',     'day',          '11:00:00',     'second_daymth_scrape',         ''          ]
    , [ 'run_day_03_1100_00',   '',     'day',          '11:00:00',     'third_daymth_scrape',          ''          ]
    , [ 'run_day_04_1100_00',   '',     'day',          '11:00:00',     'fourth_daymth_scrape',         ''          ]
    , [ 'run_day_05_1100_00',   '',     'day',          '11:00:00',     'fifth_daymth_scrape',          ''          ]
    , [ 'run_day_06_1100_00',   '',     'day',          '11:00:00',     'sixth_daymth_scrape',          ''          ]
    , [ 'run_day_15_1100_00',   '',     'day',          '11:00:00',     'fifteenth_daymth_scrape',      ''          ]
    , [ 'run_day_16_1100_00',   '',     'day',          '11:00:00',     'sixteenth_daymth_scrape',      ''          ]
    , [ 'run_day_17_1100_00',   '',     'day',          '11:00:00',     'seventeenth_daymth_scrape',    ''          ]
    , [ 'run_day_18_1100_00',   '',     'day',          '11:00:00',     'eighteenth_daymth_scrape',     ''          ]
    , [ 'run_day_19_1100_00',   '',     'day',          '11:00:00',     'nineteenth_daymth_scrape',     ''          ]
    , [ 'run_day_20_1100_00',   '',     'day',          '11:00:00',     'twentieth_daymth_scrape',      ''          ]
]
schedule_list_cols = [
     "schedule_name"
    , "schedule_period"
    , "schedule_frequency"
    , "schedule_time"
    , "schedule_function"
    , "schedule_until"
]
schedule_list_df = pda.DataFrame( schedule_list )
schedule_list_df.columns = schedule_list_cols


schedule = "##ASSIGN TIMING"
for item in schedule_list:
    if item[ 1 ] != '':
        every = ".every( {schedule_period} )".format( schedule_period = item[ 1 ] )
    else:
        every = ".every( )"

    frequency = "." + item[ 2 ]

    if item[ 3 ] != '':
        at = ".at( '{schedule_time}' )".format( schedule_time = item[ 3 ] )
    else:
        at = ""

    if item[ 5 ] != '':
        until = ".until( '{schedule_until}' )".format( schedule_until = item[ 5 ] )
    else:
        until = ""

    function = ".do( {schedule_function} )".format( schedule_function = item[ 4 ] )

    schedule_str = f"""def {item[ 0 ]}( ):\n\tschedule{every}{frequency}{at}{until}{function}\n\trun_pending( )"""
    schedule = f"""{schedule}\n{schedule_str}"""


mp_process = f"""##RUN JOB\ndef run_job( ):\n\t##setting multiprocessing"""
for item in schedule_list:
    mp_process_str = f"""\t{item[ 0 ].replace( "run", "var" )} = mp.Process( target = {item[ 0 ]} )"""
    mp_process = f"""{mp_process}\n{mp_process_str}"""


start = "\t##set start"
for item in schedule_list:
    start_str = f"""\t{item[ 0 ].replace( "run", "var" )}.start( )"""
    start = f"""{start}\n{start_str}"""


join = "\t##set join"
for item in schedule_list:
    join_str = f"""\t{item[ 0 ].replace( "run", "var" )}.join( )"""
    join = f"""{join}\n{join_str}"""


print( f"""{schedule_dataframe}{schedule_list_df}\n\n\n{schedule}\n\n\n{mp_process}\n\n\n{start}\n\n\n{join}""" )


//churn schedule templates//'''
###
###




##
##
# ''' << paste schedule template here

##ASSIGN TIMING
def run_every30mins( ):
    schedule.every( 30 ).minutes.at( ':00' ).until( '19:00:00' ).do( every_30mins )
    run_pending( )
def run_daily_00( ):
    schedule.every( ).day.at( '09:30:00' ).do( daily_refresh_0 )
    run_pending( )
def run_fri_2300_00( ):
    schedule.every( ).friday.at( '23:00:00' ).do( friday_refresh_0 )
    run_pending( )
def run_day_01_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( first_daymth_scrape )
    run_pending( )
def run_day_02_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( second_daymth_scrape )
    run_pending( )
def run_day_03_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( third_daymth_scrape )
    run_pending( )
def run_day_04_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( fourth_daymth_scrape )
    run_pending( )
def run_day_05_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( fifth_daymth_scrape )
    run_pending( )
def run_day_06_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( sixth_daymth_scrape )
    run_pending( )
def run_day_15_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( fifteenth_daymth_scrape )
    run_pending( )
def run_day_16_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( sixteenth_daymth_scrape )
    run_pending( )
def run_day_17_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( seventeenth_daymth_scrape )
    run_pending( )
def run_day_18_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( eighteenth_daymth_scrape )
    run_pending( )
def run_day_19_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( nineteenth_daymth_scrape )
    run_pending( )
def run_day_20_1100_00( ):
    schedule.every( ).day.at( '11:00:00' ).do( twentieth_daymth_scrape )
    run_pending( )


##RUN JOB
def run_job( ):
    ##setting multiprocessing
    var_every30mins = mp.Process( target = run_every30mins )
    var_daily_00 = mp.Process( target = run_daily_00 )
    var_fri_2300_00 = mp.Process( target = run_fri_2300_00 )
    var_day_01_1100_00 = mp.Process( target = run_day_01_1100_00 )
    var_day_02_1100_00 = mp.Process( target = run_day_02_1100_00 )
    var_day_03_1100_00 = mp.Process( target = run_day_03_1100_00 )
    var_day_04_1100_00 = mp.Process( target = run_day_04_1100_00 )
    var_day_05_1100_00 = mp.Process( target = run_day_05_1100_00 )
    var_day_06_1100_00 = mp.Process( target = run_day_06_1100_00 )
    var_day_15_1100_00 = mp.Process( target = run_day_15_1100_00 )
    var_day_16_1100_00 = mp.Process( target = run_day_16_1100_00 )
    var_day_17_1100_00 = mp.Process( target = run_day_17_1100_00 )
    var_day_18_1100_00 = mp.Process( target = run_day_18_1100_00 )
    var_day_19_1100_00 = mp.Process( target = run_day_19_1100_00 )
    var_day_20_1100_00 = mp.Process( target = run_day_20_1100_00 )


    ##set start
    var_every30mins.start( )
    var_daily_00.start( )
    var_fri_2300_00.start( )
    var_day_01_1100_00.start( )
    var_day_02_1100_00.start( )
    var_day_03_1100_00.start( )
    var_day_04_1100_00.start( )
    var_day_05_1100_00.start( )
    var_day_06_1100_00.start( )
    var_day_15_1100_00.start( )
    var_day_16_1100_00.start( )
    var_day_17_1100_00.start( )
    var_day_18_1100_00.start( )
    var_day_19_1100_00.start( )
    var_day_20_1100_00.start( )


    ##set join
    var_every30mins.join( )
    var_daily_00.join( )
    var_fri_2300_00.join( )
    var_day_01_1100_00.join( )
    var_day_02_1100_00.join( )
    var_day_03_1100_00.join( )
    var_day_04_1100_00.join( )
    var_day_05_1100_00.join( )
    var_day_06_1100_00.join( )
    var_day_15_1100_00.join( )
    var_day_16_1100_00.join( )
    var_day_17_1100_00.join( )
    var_day_18_1100_00.join( )
    var_day_19_1100_00.join( )
    var_day_20_1100_00.join( )

## paste schedule template here >>
##
##


if __name__ == "__main__":
    while True:
        run_job( )
# '''



##=================================
##  CODE ENDS HERE
##=================================

# lg.log_script_finish( with_beep = 0 )
