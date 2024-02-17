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
import json
import webbrowser
import shutil
import sqlalchemy as sa
import numpy as npy
import pandas as pda
pda.set_option( "display.max_columns", None )
pda.set_option( "display.width", None )
import ast
import glob


##<< log tracking ===================

proj_class_name = "urmother"
task_name = "gantt_chart"
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

def gsheet_download(
     file_name
    , sheet_name
    , url_file_codename
    , url_sheet_codename
    , url_cell_range
    , save_folder
):
    default_user = os.environ[ "USERPROFILE" ]
    download_folder = Path( default_user, "Downloads" )

    gdocs_default = "https://docs.google.com/spreadsheets/d/"

    gsheet_url = gdocs_default + url_file_codename + "/export?format=csv&gid=" + url_sheet_codename + "&range=" + url_cell_range
    try:
        webbrowser.open( gsheet_url, new = 0 )

        ##check if file is successfully downloaded
        x = 0
        while os.path.exists( Path( download_folder, file_name + " - " + sheet_name + ".csv" ) ) == False:
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
         Path( download_folder, f"""{file_name} - {sheet_name}.csv""" )
        , Path( save_folder, f"""{file_name.lower( ).replace( " ", "_" )} - {sheet_name}.csv""" )
    )


def pygsapi_update(
         service_account_type
        , key_filename
        , encrypted_filename
        , spreadsheet_id
        , gsheet_range
        , extend_gsheet_range
        , data_upload_list
    ):
    '''
    service_account_type : method used to set credentials --> using either "file" or "file_content" to generate credentials
    key_filename : filename that stores the decrypt key
    encrypted_filename : filename that contains encrypted details
    spreadsheet_id : ID (aka file code) of the gsheet
    gsheet_range : specify sheetname and cell range that needs to be updated e.g. "sheet 1!D20:D30"
    data_upload_list : converted dataset in list format
    '''

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

    ##CLEAR VALUES
    clear_gsheet = \
        sheet.values( ).clear(
             spreadsheetId = spreadsheet_id
            , range = gsheet_range
            , body = { }
        )
    clear_gsheet.execute( )

    ##WRITE SHEET
    write_gsheet = \
        sheet.values( ).update(
            spreadsheetId = spreadsheet_id
            , range = f"""{gsheet_range}{extend_gsheet_range}"""
            , valueInputOption = "USER_ENTERED"
            , body = { "values" : data_upload_list }
        )
    write_gsheet.execute( )


def glob_compile_df(
         scan_folder_path = None
        , scan_file_regex_string = None
        , scan_file_format = None
        , save_folder_path = None
        , save_compiled_filename = None
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

    for idx, file in enumerate( file_list ):
        if idx == 0:
            df = pda.read_csv( file )
        else:
            df_append = pda.read_csv( file )
            df = pda.concat( [ df, df_append ], ignore_index = True )
    df.to_csv( Path( save_folder_path, f"""{save_compiled_filename}.csv""" ), index = False )

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


def filter_keyword_from_dflist( df, df_column, keyword_list ):
    contains = npy.vectorize( str.__contains__ )
    contain_all = lambda words: df[ contains( df[ df_column ], words ).all( axis = 0 ) ]
    df_new = contain_all( [ keyword_list ] )

    return df_new


##=================================
##  INSERT CODE HERE
##=================================

try:
    tagging = "set_folder_paths_variables"
    lg.log_subscript_start( tagging, "setting folder paths and variables..." )

    working_folder = \
        otomkdir.otomkdir.auto_create_folder_2(
            driver_name = "G"
            ,folder_extend = f"""My Drive/{proj_class_name}"""
            ,subfolder_extend = task_name
        )

    staff_name = "Cheong Wei-In"

    lg.log_subscript_finish( tagging, "folder paths and variables set" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "download_log_report"
    lg.log_subscript_start( tagging, "downloading log hours report..." )

    gsheet_directory_list = [ "google_sheet_directories", "1257881744", "A2:E" ]
    gsheet_directory_list_cols = [
        "sheet_name"
        , "url_sheet_codename"
        , "url_cell_range"
    ]
    gsheet_download(
         file_name = "projects_gantt_chart"
        , sheet_name = gsheet_directory_list[ 0 ]
        , url_file_codename = "1IZ_syfhPuPtJDaWbEMO37SNhZ1F9hFKQMzrKDTryx6M"
        , url_sheet_codename = gsheet_directory_list[ 1 ]
        , url_cell_range = gsheet_directory_list[ 2 ]
        , save_folder = working_folder
    )

    df_gsheet_directories = pda.read_csv( Path( working_folder, "projects_gantt_chart - google_sheet_directories.csv" ), dtype = { "gsheet_tab_gid_code" : "object" } )
    df_gsheet_directories = \
        df_gsheet_directories[
            [
                 "gsheet_tab_name"
                , "gsheet_tab_gid_code"
                , "cell_range"
            ]
        ]
    df_gsheet_logreports = \
        df_gsheet_directories[
             df_gsheet_directories[ "gsheet_tab_name" ].str.contains( "log_hours" )
        ]
    gsheet_logreport_list = df_gsheet_logreports.values.tolist( )


    for gsheet in gsheet_logreport_list:
        gsheet_download(
             file_name = "projects_gantt_chart"
            , sheet_name = gsheet[ 0 ]
            , url_file_codename = "1IZ_syfhPuPtJDaWbEMO37SNhZ1F9hFKQMzrKDTryx6M"
            , url_sheet_codename = gsheet[ 1 ]
            , url_cell_range = gsheet[ 2 ]
            , save_folder = working_folder
        )

    lg.log_subscript_finish( tagging, "log report downloaded" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "download_hols_leaves"
    lg.log_subscript_start( tagging, "downloading holidays and leaves..." )

    df_gsheet_holsleaves = \
        df_gsheet_directories[
             ( df_gsheet_directories[ "gsheet_tab_name" ] == "holidays" )
            | ( df_gsheet_directories[ "gsheet_tab_name" ] == "work_leaves" )
        ]
    gsheet_holsleaves_list = df_gsheet_holsleaves.values.tolist( )


    for gsheet in gsheet_holsleaves_list:
        gsheet_download(
             file_name = "projects_gantt_chart"
            , sheet_name = gsheet[ 0 ]
            , url_file_codename = "1IZ_syfhPuPtJDaWbEMO37SNhZ1F9hFKQMzrKDTryx6M"
            , url_sheet_codename = gsheet[ 1 ]
            , url_cell_range = gsheet[ 2 ]
            , save_folder = working_folder
        )

    lg.log_subscript_finish( tagging, "holidays and leaves downloaded" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "create_df"
    lg.log_subscript_start( tagging, "creating dataframe..." )

    df_log_hours = \
        glob_compile_df(
             scan_folder_path = working_folder
            , scan_file_regex_string = "projects_gantt_chart - *_log_hours"
            , scan_file_format = "csv"
            , save_folder_path = working_folder
            , save_compiled_filename = "projects_gantt_chart - log_hours"
        )
    # df_log_hours = df_log_hours[ df_log_hours[ "log_hours_id" ] < 10 ]
    df_log_hours = \
        df_log_hours[
             ( df_log_hours[ "is_complete" ] == 1 )
        ]
    df_log_hours[ "start_timestamp" ] = pda.to_datetime( df_log_hours[ "start_timestamp" ] )
    df_log_hours[ "end_timestamp" ] = pda.to_datetime( df_log_hours[ "end_timestamp" ] )
    df_log_hours = \
        df_log_hours[
             [
                 "log_hours_id"
                , "start_timestamp"
                , "end_timestamp"
                , "initiative_id_list"
             ]
        ]


    df_gantt_chart = pda.read_csv( Path( working_folder, f"""projects_gantt_chart - roadmap_gantt.csv""" ) )
    df_gantt_chart = \
        df_gantt_chart[
             [
                 "umbrella_initiative"
                , "initiative_id"
                , "initiative"
                , "objective"
                , "key_contact"
                , "client"
                , "expected_log_hours"
                , "start_on_date"
                , "end_before_date"
                , "initiative_status"
                , "department"
             ]
        ]

    lg.log_subscript_finish( tagging, "dataframe created" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "split_logs"
    lg.log_subscript_start( tagging, "splitting logs based on shared initiatives..." )

    log_hours_id_list = df_log_hours[ "log_hours_id" ].tolist( )
    df_initiative_logs = df_log_hours.drop( df_log_hours.index )    ##create empty dataframe using existing datafram

    for log_id in log_hours_id_list:
        df_split_logs = df_log_hours[ df_log_hours[ "log_hours_id" ] == log_id ]
        initiative_list_str = df_split_logs[ "initiative_id_list" ].values.tolist( )[ 0 ]
        initiative_list = ast.literal_eval( initiative_list_str )
        
        for initiative in initiative_list:
            df_split_logs[ "initiative_id" ] = initiative[ 0 ]
            df_split_logs[ "log_hours_weight" ] = initiative[ 1 ]
            df_initiative_logs = pda.concat( [ df_initiative_logs, df_split_logs ], ignore_index = True )
    
    df_initiative_logs = df_initiative_logs.reset_index( drop = True )

    lg.log_subscript_finish( tagging, "split logs done" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "get_workleaves"
    lg.log_subscript_start( tagging, "getting work leaves..." )

    df_workleaves = pda.read_csv( Path( working_folder, f"""projects_gantt_chart - work_leaves.csv""" ) )
    df_approved_workleaves = \
        df_workleaves[
             ( df_workleaves[ "staff_name" ] == staff_name )
            & ( df_workleaves[ "is_approved" ] == 1 )
        ]
    df_approved_workleaves = \
        df_approved_workleaves[
            [
                 "calendar_date"
                , "leave_id"
                , "leave_reason"
                , "staff_name"
                , "states_id"
            ]
        ]

    lg.log_subscript_finish( tagging, "work leaves obtained" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "create_local_calendar"
    lg.log_subscript_start( tagging, "generating local calendar..." )

    future_date = datetime.datetime.strftime( datetime.datetime.now( ) + datetime.timedelta( days = 20 ), "%Y-%m-%d" )
    df_calendar_wip = \
        create_calendar_df(
             '2022-01-01'
            , future_date
        )
    df_calendar_wip[ "calendar_date" ] = df_calendar_wip[ "calendar_date" ].apply( lambda x : x.strftime( "%Y-%m-%d" ) )
    df_holidays = pda.read_csv( Path( working_folder, f"""projects_gantt_chart - holidays.csv""" ) )
    df_holidays = \
        df_holidays[
            [
                 "calendar_date"
                , "holiday_name"
                , "states_list"
            ]
        ]
    df_sel_holidays = filter_keyword_from_dflist( df_holidays, "states_list", [ "SEL" ] )
    

    df_calendar = \
        pda.merge(
             df_calendar_wip
            , df_sel_holidays
            , how = "left"
            , left_on = [ "calendar_date" ]
            , right_on = [ "calendar_date" ]
            , suffixes = ( "_x", "_y" )
            , validate = "m:1"
            , indicator = False
        )
    df_calendar[ "is_holiday" ] = df_calendar[ "holiday_name" ].apply( lambda x : 1 if pda.notnull( x ) else 0 )
    df_calendar = \
        pda.merge(
             df_calendar
            , df_approved_workleaves
            , how = "left"
            , left_on = [ "calendar_date" ]
            , right_on = [ "calendar_date" ]
            , suffixes = ( "_x", "_y" )
            , validate = "1:1"
            , indicator = False
        )
    df_calendar[ "states_list" ] = \
        df_calendar.apply(
             lambda row:
                 [ row[ "states_id" ] ] if pda.isnull( row[ "states_list" ] ) and pda.notnull( row[ "states_id" ] )
                else ast.literal_eval( row[ "states_list" ] ).append( row[ "states_id" ] ) if pda.notnull( row[ "states_list" ] ) and pda.notnull( row[ "states_id" ] )
                else row[ "states_list" ] if pda.isnull( row[ "states_list" ] ) and pda.isnull( row[ "states_id" ] )
                else ast.literal_eval( row[ "states_list" ] )
            , axis = 1
        )
    df_calendar[ "is_off_day" ] = df_calendar.apply( lambda row : 1 if row[ "is_holiday" ] == 1 or pda.notnull( row[ "states_id" ] ) else 0, axis = 1 )
    df_calendar[ "off_day_name" ] = df_calendar.apply( lambda row : row[ "holiday_name" ] if pda.notnull( row[ "holiday_name" ] ) else row[ "leave_id" ], axis = 1 )
    df_gantt_calendar = \
        df_calendar[
            [
                 "calendar_date"
                , "day_name"
                , "off_day_name"
                , "states_list"
                , "is_off_day"
            ]
        ]

    lg.log_subscript_finish( tagging, "local calendar created" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "generate_log_reports"
    lg.log_subscript_start( tagging, "generating log reports..." )

    df_log_reports = df_initiative_logs.drop( columns = [ "initiative_id_list" ] )
    df_log_reports[ "log_hours_weight" ] = df_log_reports[ "log_hours_weight" ].apply( lambda x : float( x ) )
    df_log_reports[ "total_log_duration_hrs" ] = df_log_reports.apply( lambda row : pda.Timedelta( row[ "end_timestamp" ] - row[ "start_timestamp" ] ).seconds / 60 / 60, axis = 1 )
    df_log_reports[ "total_log_duration_mins" ] = df_log_reports.apply( lambda row : pda.Timedelta( row[ "end_timestamp" ] - row[ "start_timestamp" ] ).seconds / 60, axis = 1 )
    df_log_reports[ "log_end_yrmth" ] = df_log_reports[ "end_timestamp" ].apply( lambda x : x.strftime( "%Y-%m" ) )
    df_log_reports[ "log_end_date" ] = df_log_reports[ "end_timestamp" ].apply( lambda x : x.strftime( "%Y-%m-%d" ) )
    df_log_reports[ "initiative_log_duration_hrs" ] = df_log_reports.apply( lambda row : row[ "total_log_duration_hrs" ] * row[ "log_hours_weight" ], axis = 1 )
    df_log_reports[ "initiative_log_duration_mins" ] = df_log_reports.apply( lambda row : row[ "total_log_duration_mins" ] * row[ "log_hours_weight" ], axis = 1 )

    df_log_reports_detail = \
        pda.merge(
             df_gantt_chart
            , df_log_reports
            , how = "left"
            , left_on = [ "initiative_id" ]
            , right_on = [ "initiative_id" ]
            , suffixes = ( "_x", "_y" )
            , validate = "1:m"
            , indicator = False
        )


    ##SLOT MISSING CALENDAR DATES
    df_logdates = df_log_reports[ [ "log_end_date" ] ]
    df_logdates[ "log_end_date" ] = pda.to_datetime( df_logdates[ "log_end_date" ] )
    df_logdates = df_logdates[ ~df_logdates[ "log_end_date" ].isnull( ) ]
    df_logdates = df_logdates.drop_duplicates( )
    logdate_list = sorted( df_logdates[ "log_end_date" ].tolist( ) )
    min_log_date = "2022-10-01"
    max_log_date = future_date

    df_subcalendar = create_calendar_df( min_log_date, max_log_date )
    calendar_list = df_subcalendar[ "calendar_date" ].tolist( )
    append_date_list = [ ]
    for x in calendar_list:
        if x in logdate_list:
            pass
        else:
            append_date_list.append( datetime.datetime.strftime( x, "%Y-%m-%d" ) )
    df_append_dates = pda.DataFrame( append_date_list, columns = [ "log_end_date" ] )
    df_append_dates[ "log_end_yrmth" ] = df_append_dates[ "log_end_date" ].apply( lambda x : pda.to_datetime( x ).strftime( "%Y-%m" ) )
    df_append_dates[ "client" ] = "aaa_no_log"



    df_log_reports_detail = pda.concat( [ df_log_reports_detail, df_append_dates ], ignore_index = True )
    df_log_reports_detail = \
        pda.merge(
             df_log_reports_detail
            , df_gantt_calendar
            , how = "left"
            , left_on = [ "log_end_date" ]
            , right_on = [ "calendar_date" ]
            , suffixes = ( "_x", "_y" )
            , validate = "m:1"
            , indicator = False
        )

    df_log_reports_detail[ "downloaded_at" ] = datetime.datetime.now( )
    df_log_reports_detail = \
        df_log_reports_detail[
             [
                 "umbrella_initiative"
                , "initiative_id"
                , "initiative"
                , "objective"
                , "key_contact"
                , "client"
                , "expected_log_hours"
                , "log_hours_id"
                , "start_timestamp"
                , "end_timestamp"
                , "log_hours_weight"
                , "total_log_duration_hrs"
                , "total_log_duration_mins"
                , "log_end_yrmth"
                , "log_end_date"
                , "initiative_log_duration_hrs"
                , "initiative_log_duration_mins"
                , "calendar_date"
                , "day_name"
                , "off_day_name"
                , "states_list"
                , "is_off_day"
                , "downloaded_at"
                , "start_on_date"
                , "end_before_date"
                , "initiative_status"
                , "department"
             ]
        ]
    df_log_reports_detail.to_csv( Path( working_folder, f"""project_log_report_detail.csv""" ), index = False )


    lg.log_subscript_finish( tagging, "log reports generated" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "log_summary"
    lg.log_subscript_start( tagging, "getting summary log reports..." )

    df_log_reports_summary = \
        df_log_reports_detail.groupby(
             [
                 "log_end_yrmth"
                , "client"
             ]
            , dropna = False
        ).agg(
             total_logged_days = ( "log_end_date", "nunique" )
            , total_logged_hrs = ( "initiative_log_duration_hrs", "sum" )
            , total_logged_mins = ( "initiative_log_duration_mins", "sum" )
            , total_initiatives = ( "initiative_id", "nunique" )
            , total_umbrella_initiatives = ( "umbrella_initiative", "nunique" )
        )
    df_log_reports_summary = df_log_reports_summary.reset_index( )
    df_log_reports_summary.to_csv( Path( working_folder, f"""project_log_report_summary.csv""" ), index = False )


    lg.log_subscript_finish( tagging, "summary log reports done" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


# try:
#     tagging = "upload_gsheet"
#     lg.log_subscript_start( tagging, "uploading to gsheet..." )

#     df_detail_upload = df_log_reports_detail.copy( )
#     df_detail_upload = df_detail_upload.astype( str )
#     df_list = [ df_detail_upload.columns.tolist( ) ]
#     df_list.extend( df_detail_upload.values.tolist( ) )

#     pygsapi_update(
#              service_account_type = "file_content"
#             , key_filename = "chewy_gsapi_py"
#             , encrypted_filename = "chewy_gsapi_py"
#             , spreadsheet_id =  "18dmhWmKjoZymuI71gi6Jqb14_XVP0x-0FaFjROg7acI"
#             , gsheet_range =  "raw_data_log_detail_report"
#             , extend_gsheet_range =  "!A1"
#             , data_upload_list =  df_list
#         )
    

#     lg.log_subscript_finish( tagging, "uploaded to gsheet" )
#     lg.update_log_status_1( is_log_update_1, 1, tagging )

# except Exception as exception:
#     lg.log_exception( exception )
#     lg.update_log_status_1( is_log_update_1, 0, tagging )






##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
