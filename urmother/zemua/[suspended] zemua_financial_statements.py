import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir

##additional modules
import sqlite3
import pandas as pds
import webbrowser
from PIL import Image
import pytesseract
import numpy as npy


##<< log tracking ===================

proj_class_name = "zemua"
task_name = "urmother"
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

def sqlite3_connect( db_name ):
    '''
    connect to sqlite
    '''

    sqlite3_db = otomkdir.otomkdir.auto_create_folder_2(
        driver_name = "D"
        , folder_extend = "sqlite3"
    )
    db_name = db_name
    connection = sqlite3.connect( Path( sqlite3_db, db_name + ".db" ) )
    return connection


def open_browser( ):
    try:
        tagging = "open_browser"
        lg.log_subscript_start( tagging, "preparing browser..." )

        webbrowser.open( "https://www.google.com", new = 0 )
        time.sleep( 10 )

        lg.log_subscript_finish( tagging, "browser prepared" )
    except Exception as exception:
        lg.log_exception( exception )


def download_gsheet( gs_filename, gs_sheetname, gs_cellrange ):
    webbrowser.open(
         "https://docs.google.com/spreadsheets/d/{gs_filename}/export?format=csv&gid={gs_sheetname}&range={gs_cellrange}".\
                format(
                    gs_filename = gs_filename
                    , gs_sheetname = gs_sheetname
                    , gs_cellrange = gs_cellrange
                )
        , new = 0
    )
    time.sleep( 7 )


def new_updated_df( existing_table_name, latest_df ):
    sqlite_conn = sqlite3_connect( proj_class_name )
    cursor = sqlite_conn.cursor( )
    query_tbl_check = f"""
        select
             count( r1.name ) as "count"
        from
            sqlite_master r1
        where
             r1.type = 'table'
        and
             r1.name = '{existing_table_name}'
    """
    query_dl = f"select * from {existing_table_name}"
    cursor.execute( query_tbl_check )
    if cursor.fetchone( )[ 0 ] == 1:
        table_exists = 1
    else:
        table_exists = 0

    if table_exists == 1:
        df_download = pds.read_sql( query_dl, sqlite_conn )
        # print( df_download.to_string( max_rows = 10 ) )

        df_concat = \
            pds.concat(
                 [ latest_df, df_download ]
                , ignore_index = True
                , sort = False
            )
    else:
        df_concat = df

    sqlite_conn.commit( )
    sqlite_conn.close( )

    return df_concat


def upload_to_sqlite3( df, table_name ):
    df_colnum = df.shape[ 1 ]
    q_mark = "?"
    q_marks = ",".join( q_mark * df_colnum )

    df_columns = list( df.columns )
    df_col_str = " text,".join( [ str( col ) for col in df_columns ] ) + " text"
    # print( df_col_str )

    rows = df.values.tolist( )

    connection = sqlite3_connect( proj_class_name )
    cursor = connection.cursor( )
    cursor.execute(
        "drop table if exists {table_name}".\
            format(
                table_name = table_name
            )
    )

    cursor.execute(
        "create table if not exists {table_name}( {column_names} )".\
            format(
                table_name = table_name
                , column_names = df_col_str
            )
    )

    cursor.executemany(
        "insert into {table_name} values( {q_marks} )".\
            format(
                table_name = table_name
                , q_marks = q_marks
            )
        , rows
    )

    connection.commit( )
    connection.close( )


def ocr_capture( file_path ):
    pytesseract.pytesseract.tesseract_cmd = Path( "C:/Program Files/Tesseract-OCR/tesseract" )
    image_file = Path( file_path + ".png" )
    imgl = npy.array( Image.open( image_file ) )
    text = pytesseract.image_to_string( imgl )
    return text



##=================================
##  INSERT CODE HERE
##=================================

bursa_code = "AWC"

working_folder = \
    otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "G"
        , folder_extend = "My Drive/projects/" + proj_class_name
    )

bursa_gsheet_map = "bursa_finstmnt_gsheet_mapping.csv"

fin_stmt_code = [ ]
fin_stmt_name = [
     ( "master_balance_sheet", "A2:J" )
    , ( "master_income_statement", "A2:J" )
    # , ( "balance_sheet_mapping", "A2:N" )
    # , ( "income_statement_mapping", "A2:O" )
]
"""
column_names = [
     "original_sheet_name"
    , "cell_range"
]
"""

df = pds.read_csv( Path( working_folder, bursa_gsheet_map ) )
# print( df.to_string( max_rows = 10 ) )
gsheet_file = df[ "gsheet_file" ].values.tolist( )[ 0 ]
fin_stmt_code.append( df[ "gsheet_sheet_mapbs" ].values.tolist( )[ 0 ] )
fin_stmt_code.append( df[ "gsheet_sheet_mapis" ].values.tolist( )[ 0 ] )
# fin_stmt_code.append( df[ "gsheet_sheet_bs" ].values.tolist( )[ 0 ] )
# fin_stmt_code.append( df[ "gsheet_sheet_is" ].values.tolist( )[ 0 ] )
print( fin_stmt_code )


##start downloading gsheets
open_browser( )
download_folder = Path( os.environ[ "USERPROFILE" ], "Downloads" )
download_files = [ ]
for sheet_code, sheet_details in zip( fin_stmt_code, fin_stmt_name ):
    download_gsheet( gsheet_file, sheet_code, sheet_details[ 1 ] )
    download_file = Path( download_folder, "financial_mapping_template - " + sheet_details[ 0 ] + ".csv" )
    download_files.append( download_file )
# print( download_files )


for file in download_files:
    filename_extract = str( file ).split( "-" )[ -1:: ][ 0 ].replace( ".csv", "" ).strip( )
    if "master" in str( file ):
        table_name = filename_extract
    else:
        table_name = bursa_code.lower( ) + "_" + filename_extract
    print( table_name )

    df = pds.read_csv( file )
    df = df.loc[ :, ~df.columns.str.contains( "^unnamed", case = False ) ]
    df.loc[ :, "uploaded_at" ] = datetime.datetime.now( )
    df.fillna( value = -1, axis = 1, inplace = True )
    df = df.applymap( str )
    # print( df )
    # print( df.dtypes )

    df_final = new_updated_df( table_name, df )

    upload_to_sqlite3( df_final, table_name )


##DELETING DOWNLOADED FILES IN DOWNLOADS FOLDER
for file in download_files:
    os.remove( file )


"""
##OCR CAPTURE IMAGE
file_name = str( Path( working_folder, "ocr_image" ) )
ocr_text = ocr_capture( file_name ).replace( ",", "" )
text_list = ocr_text.split( "\n" )

df_ocr = pds.DataFrame( text_list )
df_ocr.columns = [ "ori" ]
df_ocr[ [ "ori", "split_0", "split_1" ] ] = df_ocr[ "ori" ].str.rsplit( " ", 2, expand = True )
# print( df_ocr )

ocr_csv = Path( working_folder, "ocr_csv" + ".csv" )
df_ocr.to_csv( ocr_csv, index = False, sep = ";" )
"""



##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
