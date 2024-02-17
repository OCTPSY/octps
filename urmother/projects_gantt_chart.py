import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir

##additional modules
import pandas as pds
import numpy as npy
import matplotlib.pyplot as plt
from matplotlib.patches import Patch as patch
import matplotlib.dates as matdates
import webbrowser
import shutil
from PIL import Image as image
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

        time.sleep( 3 )

    except Exception as exception:
        print( exception )

    time.sleep( 0.5 )


    shutil.move(
         Path( download_folder, file_name + " - " + sheet_name + ".csv" )
        , Path( save_folder, file_name.lower( ).replace( " ", "_" ) + " - " + sheet_name + ".csv" )
    )


def to_dict( list_items ):
    thisdict = { }
    for x in list_items:
        y = iter( x )
        result = dict( zip( y, y ) )
        thisdict.update( result )
    return thisdict


def strike( text ):
    result = "\u0336" + "\u0336".join( text ) + "\u0336"
    return result


def underline( text ):
    result = "\u0332" + "\u0332".join( text ) + "\u0332"
    return result


def add_minutes(
        timestamp
        , minutes
    ):
    '''
    timestamp : timestamp format
    minutes : include minus (-) sign if want to subtract
    '''
    x = timestamp + datetime.timedelta( minutes = minutes )
    return x


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
            df = pds.read_csv( file )
        else:
            df_append = pds.read_csv( file )
            df = pds.concat( [ df, df_append ], ignore_index = True )
    df.to_csv( Path( save_folder_path, f"""{save_compiled_filename}.csv""" ), index = False )

    return df


##=====================================
##  INSERT CODE HERE
##=====================================

try:
    tagging = "set_folder_paths"
    lg.log_subscript_start( tagging, "setting folder paths..." )

    working_folder = \
        otomkdir.otomkdir.auto_create_folder_2(
            driver_name = "G"
            , folder_extend = "My Drive" + "/" + proj_class_name
            , subfolder_extend = task_name
        )

    client_path_list = [
         ( "self", working_folder, "urmother" )
        , ( "rapid", "G:/.shortcut-targets-by-id/1-dtquloyy7eE4eqaugsi0rcVLswJ7y6s/python/gantt_chart", "rapid_wealth" )
        , ( "carsome", working_folder, "carsome" )
        , ( "mobee", working_folder, "mobee")
        , ( "protec", working_folder, "protec" )
        , ( "colibri", working_folder, "colibri" )
        , ( "moon", working_folder, "moon" )
        , ( "kgteh", working_folder, "kgteh" )
        , ( "benji", working_folder, "benji" )
        , ( "teflon", working_folder, "teflon" )
    ]
    client_path_list_cols = [
         "client_name"
        , "folder_path"
        , "file_name"
    ]

    lg.log_subscript_finish( tagging, "folder paths set" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "gsheet_download"
    lg.log_subscript_start( tagging, "downloading gsheet..." )

    gsheet_directory_list = [ "google_sheet_directories", "1257881744", "A2:G" ]
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

    df_gsheet_directories = \
        pds.read_csv(
             Path( working_folder, "projects_gantt_chart - google_sheet_directories.csv" )
            , dtype = {
                 "gsheet_tab_gid_code"  : "object"
                , "is_scheduled_upload" : "int64"
                , "is_adhoc_upload"     : "int64"
              }
        )
    if df_gsheet_directories[ "is_adhoc_upload" ].sum( ) > 0:
        df_gsheet_directories = df_gsheet_directories.query( "is_adhoc_upload == 1" )
    else:
        df_gsheet_directories = df_gsheet_directories.query( "is_scheduled_upload == 1" )
    df_gsheet_directories = \
        df_gsheet_directories[
            [
                 "gsheet_tab_name"
                , "gsheet_tab_gid_code"
                , "cell_range"
            ]
        ]
    df_gsheet_directories = \
        df_gsheet_directories[
             ~df_gsheet_directories[ "gsheet_tab_name" ].str.contains( "log_hours" )
        ]
    gsheet_list = df_gsheet_directories.values.tolist( )


    for gsheet in gsheet_list:
        gsheet_download(
             file_name = "projects_gantt_chart"
            , sheet_name = gsheet[ 0 ]
            , url_file_codename = "1IZ_syfhPuPtJDaWbEMO37SNhZ1F9hFKQMzrKDTryx6M"
            , url_sheet_codename = gsheet[ 1 ]
            , url_cell_range = gsheet[ 2 ]
            , save_folder = working_folder
        )

    lg.log_subscript_finish( tagging, "gsheet downloaded" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "create_df_list"
    lg.log_subscript_start( tagging, "creating dataframe and unique list data..." )

    df_umbrella = \
        glob_compile_df(
             scan_folder_path = working_folder
            , scan_file_regex_string = "projects_gantt_chart - *_umbrella_initiative"
            , scan_file_format = "csv"
            , save_folder_path = working_folder
            , save_compiled_filename = "projects_gantt_chart - umbrella_initiative"
        )
    df_umbrella[ "concat_umbrella_initiative_id" ] = df_umbrella[ "umbrella_initiative_id" ].astype( "str" ) + " - " + df_umbrella[ "umbrella_initiative" ]
    df_full = \
        glob_compile_df(
             scan_folder_path = working_folder
            , scan_file_regex_string = "projects_gantt_chart - *_roadmap_gantt"
            , scan_file_format = "csv"
            , save_folder_path = working_folder
            , save_compiled_filename = "projects_gantt_chart - roadmap_gantt"
        )

    df_full = \
        pds.merge(
             df_full
            , df_umbrella
            , how = "left"
            , left_on = [ "umbrella_initiative" ]
            , right_on = [ "umbrella_initiative" ]
            , suffixes = [ "_x", "_y" ]
            , validate = "m:1"
            , indicator = False
        )
    df_full[ "initiative_id" ] = df_full[ "initiative_id" ].apply( lambda x : str( x ).replace( "initiative", "ID" ) )
    df_full[ "initiative" ] = df_full[ "client" ].str.upper( ) + " - " + df_full[ "umbrella_initiative_id" ].astype( "str" ) + "//" + df_full[ "initiative_id" ] + "//" + df_full[ "initiative" ]
    df_full[ "completion_%" ] = df_full[ [ "is_overall", "completion_%", "overall_completion_%" ] ].apply( lambda row : row[ "overall_completion_%" ] if row[ "is_overall" ] == 1 else row[ "completion_%" ], axis = 1 )
    df_full[ "start_on_date" ] = pds.to_datetime( df_full[ "start_on_date" ] )
    df_full[ "end_before_date" ] = pds.to_datetime( df_full[ "end_before_date" ] )

    ##EXCLUDE INITIATIVES THAT ARE DONE OR SUSPENDED AND ARE ALREADY MORE THAN 2 WEEKS OLD (14 DAYS)
    df_exclude = \
        df_full[
             ( df_full[ "end_before_date" ] < add_minutes( datetime.datetime.now( ), -( 14 * 24 * 60 ) ) )
            & ( df_full[ "initiative_status" ].isin( [ "Done", "Suspend" ] ) )
        ]
    excl_initiative_list = df_exclude[ "initiative_id" ].unique( ).tolist( )

    df_full[ "initiative" ] = df_full[ [ "initiative", "initiative_status" ] ].apply( lambda x : strike( x[ "initiative" ] ) if x[ "initiative_status" ] == "Done" else x[ "initiative" ], axis = 1 )
    df_full[ "initiative" ] = df_full[ [ "initiative", "initiative_status" ] ].apply( lambda x : strike( underline( x[ "initiative" ] ) ) if x[ "initiative_status" ] == "Suspend" else x[ "initiative" ], axis = 1 )
    df_full[ "initiative" ] = df_full[ [ "initiative", "initiative_status" ] ].apply( lambda x : underline( x[ "initiative" ] ) if x[ "initiative_status" ] == "Hold" else x[ "initiative" ], axis = 1 )
    df_full[ "initiative" ] = df_full[ [ "initiative", "initiative_status" ] ].apply( lambda x : f"""[REVIEW] - {x[ "initiative" ]}""" if x[ "initiative_status" ] == "Review - pending" else f"""[REVIEW] - {x[ "initiative" ]}""" if x[ "initiative_status" ] == "Review - wip" else x[ "initiative" ], axis = 1 )
    df_full = df_full[ ~df_full[ "initiative_id" ].isin( excl_initiative_list ) ]
    df_full = df_full.reset_index( drop = True )

    ##LIMIT GANTT CHART VIEW OF NOT MORE THAN 90 DAYS FROM REPORT RUN DATE
    df_full = \
        df_full[
             ( df_full[ "end_before_date" ] <= datetime.datetime.now( ) + datetime.timedelta( days = 90 ) )
        ]

    lg.log_subscript_finish( tagging, "dataframe and unique list created" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )


try:
    tagging = "data_transform"
    lg.log_subscript_start( tagging, "transforming data..." )

    client_list = df_full[ "client" ].unique( ).tolist( )

    for idx, client in enumerate( client_list ):
        try:
            sub_tagging = f"""data_prep_{idx}"""
            lg.log_subscript_start( sub_tagging, f"""preparing data for {client}...""" )

            ##### DATA PREP #####
            set_path = list( filter( lambda x : x[ 0 ] == client, client_path_list ) )[ 0 ]
            # print( set_path )

            if client == "self":
                df = df_full[ df_full[ "client" ] != "carsome" ]
            else:
                df = df_full[ df_full[ "client" ] == client ]
            df.sort_values(
                by = [ "client", "umbrella_initiative", "is_overall", "start_on_date", "initiative_id" ]
                , axis = 0
                , ascending = [ False, True, True, False, False ]
                , inplace = True
                , na_position = "last"
                , ignore_index = True
            )

            # project start date
            proj_start = df[ "start_on_date" ].min( )

            # number of days from project start to task start
            df[ "start_num" ] = ( df[ "start_on_date" ] - proj_start ).dt.days

            # number of days from project start to end of tasks
            df[ "end_num" ] = ( df[ "end_before_date" ] - proj_start ).dt.days

            # days between start and end of each task
            df[ "days_start_to_end" ] = df[ "end_num" ] - df[ "start_num" ]

            # days between start and current progression of each task
            df[ "current_num" ] = ( df[ "days_start_to_end" ] * df[ "completion_%" ] )

            # create a column with the color for each umbrella_initiative
            def color( row ):
                c_dict = to_dict( df_umbrella[ [ "umbrella_initiative", "hex_colour" ] ].values.tolist( ) )
                return c_dict[ row[ 'umbrella_initiative' ] ]

            df[ "color" ] = df.apply( color, axis = 1 )

            lg.log_subscript_finish( sub_tagging, "dataframe and unique list created" )
            lg.update_log_status_1( is_log_update_1, 1, sub_tagging )

        except Exception as exception:
            lg.log_exception( exception )
            lg.update_log_status_1( is_log_update_1, 0, sub_tagging )


        try:
            sub_tagging = f"""create_gantt_chart_{idx}"""
            lg.log_subscript_start( sub_tagging, f"""creating gantt chart for {client}...""" )

            checkpoint = "checkpoint 1.0"
            ##### PLOT #####
            fig, ( ax, ax1 ) = \
                plt.subplots(
                    2
                    , figsize = ( 30, 15 )
                    , gridspec_kw = { 'height_ratios' : [ 6, 1 ] }
                )

            checkpoint = "checkpoint 2.0"
            # bars
            ax.barh( df[ "initiative" ], df[ "current_num" ], left = df[ "start_num" ], color = df[ "color" ] )
            ax.barh( df[ "initiative" ], df[ "days_start_to_end" ], left = df[ "start_num" ], color = df[ "color" ], alpha = 0.4 )

            for idx, row in df.iterrows( ):
                ax.text( row[ "end_num" ] + 0.1, idx, f"""{int(row[ "completion_%" ] * 100 )}%""", va = "center", alpha = 0.8 )
                ax.text( row[ "start_num" ] - 0.1, idx, row[ "initiative" ], va = "center", ha = "right", alpha = 0.8 )

            # grid lines
            ax.set_axisbelow( True )
            ax.xaxis.grid( color = 'gray', linestyle = 'dashed', alpha = 0.33, which = 'both')

            checkpoint = "checkpoint 3.0"
            # ticks
            # xticks = npy.arange( 0, matdates.datestr2num( df[ "end_before_date" ].max( ).strftime( "%Y-%m-%d" ) ), 3 )
            xticks = npy.arange( 0, df[ "end_num" ].max( ) + 1 )
            xticks_labels = pds.date_range( proj_start, end = df[ "end_before_date" ].max( ) ).strftime( "%b %d, %a" )
            # xticks_minor = npy.arange( 0, matdates.datestr2num( df[ "end_before_date" ].max( ).strftime( "%Y-%m-%d" ) ), 1 )
            xticks_minor = npy.arange( 0, df[ "end_num" ].max( ) + 1, 1 )
            ax.set_xticks( xticks[ ::3 ] )
            ax.set_xticks( xticks_minor[ ::1 ], minor = True )
            ax.set_xticklabels( xticks_labels[ ::3 ], rotation = 90 )
            ax.set_yticks( [ ] )

            checkpoint = "checkpoint 4.0"
            # highlight current date (time range)
            today_date = datetime.datetime.today( )
            today_date = today_date.strftime( "%b %d, %a" )
            map_dates = dict( zip( xticks, list( xticks_labels ) ) )
            for idx, i in enumerate( map_dates ):
                if map_dates.get( i ) == today_date:
                    index_date = i
                    break
                elif idx == list( map_dates )[ -1 ]:
                        index_date = i
            ax.axvspan( index_date, index_date + 1, color = "gray", alpha = 0.1 )


            checkpoint = "checkpoint 5.0"
            # ticks top
            # create a new axis with the same y
            ax_top = ax.twiny( )

            # align x axis
            ax.set_xlim( 0, df[ "end_num" ].max( ) )
            ax_top.set_xlim( 0, df[ "end_num" ].max( ) )

            # top ticks (markings)
            xticks_top_minor = npy.arange( 0, df[ "end_num" ].max( ) + 1, 7 )
            ax_top.set_xticks( xticks_top_minor, minor = True )
            # top ticks (label)
            xticks_top_major = npy.arange( 0, df[ "end_num" ].max( ) + 1, 7 )
            ax_top.set_xticks( xticks_top_major, minor = False )
            # week labels
            # xticks_top_labels = [ f"Week {i}" for i in npy.arange( 1, len( xticks_top_major ) + 1, 1 ) ]
            # ax_top.set_xticklabels( xticks_top_labels, ha='center', minor = False )

            # hide major tick (we only want the label)
            ax_top.tick_params( which = 'major', color = 'w' )
            # increase minor ticks (to marks the weeks start and end)
            ax_top.tick_params( which = 'minor', length = 8, color = 'k' )

            # remove spines
            ax.spines[ 'right' ].set_visible( False )
            ax.spines[ 'left' ].set_visible( False )
            ax.spines[ 'left' ].set_position( ( 'outward', 10 ) )
            ax.spines[ 'top' ].set_visible( False )

            ax_top.spines[ 'right' ].set_visible( False )
            ax_top.spines[ 'left' ].set_visible( False )
            ax_top.spines[ 'top' ].set_visible( False )

            plt.suptitle( 'ROADMAP STATUS' )

            checkpoint = "checkpoint 6.0"
            ##### LEGENDS #####
            ''' ##old
            legend_elements = [
                patch( facecolor = '#E64646', label = "icar_performance_tracking" )
                , patch( facecolor = '#E69646', label = "b2c_icar_leads_conversion" )
                # , patch( facecolor = '#34D05C', label = "" )
                # , patch( facecolor = '#34D0C3', label = "" )
                # , patch( facecolor = '#3475D0', label = "" )
            ]
            '''
            legend_elements = [ ]
            df_umbrella_unq = \
                df.groupby(
                     [
                         "concat_umbrella_initiative_id"
                        , "color"
                     ]
                    , dropna = False
                ).agg(
                     count = ( "concat_umbrella_initiative_id", "count" )
                )
            df_umbrella_unq = df_umbrella_unq.reset_index( )
            for x in df_umbrella_unq[ [ "concat_umbrella_initiative_id", "color" ] ].values.tolist( ):
                patch_formula = patch( facecolor = x[ 1 ], label = x[ 0 ] )
                legend_elements.append( patch_formula )


            ax1.legend( handles = legend_elements, loc = 'upper center', ncol = 5, frameon = False )

            checkpoint = "checkpoint 7.0"
            # clean second axis
            ax1.spines[ 'right' ].set_visible( False )
            ax1.spines[ 'left' ].set_visible( False )
            ax1.spines[ 'top' ].set_visible( False )
            ax1.spines[ 'bottom' ].set_visible( False )
            ax1.set_xticks( [ ] )
            ax1.set_yticks( [ ] )

            image_file = Path( f"""{set_path[ 1 ]}/{set_path[ 2 ]}_gantt_chart.png""" )
            plt.savefig( image_file )
            # image.open( image_file ).show( )
            # plt.show( )

            lg.log_subscript_finish( sub_tagging, f"""gantt chart created for {client}""" )
            lg.update_log_status_1( is_log_update_1, 1, sub_tagging )

        except Exception as exception:
            lg.log_exception( f"""{exception} -- {checkpoint}""" )
            lg.update_log_status_1( is_log_update_1, 0, sub_tagging )


    lg.log_subscript_finish( tagging, "data transformed" )
    lg.update_log_status_1( is_log_update_1, 1, tagging )

except Exception as exception:
    lg.log_exception( exception )
    lg.update_log_status_1( is_log_update_1, 0, tagging )






##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
