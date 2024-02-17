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


##<< log tracking ===================

proj_class_name = "scuse"
task_name = "financial_statements"
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


def get_company_details( ticker_symbol ):
    '''
    to get company details using ticker symbol
    '''

    connection = sqlite3_connect( proj_class_name )

    sql_query = """
        with
            temp_ticker_sic as(
                select
                     cast( w1.cik as int ) as "cik"
                    , cast( w1.sic as int ) as "sic"
                    , cast( w1.fiscal_year_end as int ) as "fiscal_year_end"
                from
                    ticker_sic w1
            )
            ,
            temp_sic_code as(
                select
                     cast( w1.sic_code as int ) as "sic_code"
                    , w1.office
                    , w1.industry_title
                from
                    sic_code w1
            )
            ,
            temp_ticker_names as(
                select
                     cast( w1.cik as int ) as "cik"
                    , upper( w1.name ) as "company_name"
                    , w1.ticker as "ticker_name"
                    , upper( w1.exchange ) as "exchange"
                from
                    company_tickers_exchange w1
            )
            ,
            temp_full_company_list as(
                select
                     w3.company_name
                    , w1.cik
                    , w3.ticker_name
                    , w3.exchange
                    , w1.sic as "sic_code"
                    , w2.office as "sic_office"
                    , w2.industry_title
                from
                    temp_ticker_sic     w1
                left join
                    temp_sic_code       w2 on
                         w1.sic = w2.sic_code
                left join
                    temp_ticker_names   w3 on
                         w1.cik = w3.cik
                where
                     w2.office is not null
            )
        select
             w1.*
        from
            temp_full_company_list w1
        where
             lower( w1.ticker_name ) = '{ticker_symbol}'
        ;
    """.format\
            (
                ticker_symbol = ticker_symbol.lower( )
            )

    df = pds.read_sql( sql_query, connection )
    return df


def compile_financial_statements( ticker ):
    '''
    to compile all yearly balance sheets and income statements
    '''

    ticker_symbols = [
        ( ticker, "" )
    ]
    '''
    column_names = [
        "ticker_symbol"
        , "unassigned"
    ]
    '''

    for ticker_symbol in ticker_symbols:
        ##extract cik
        try:
            tagging = "extract_cik_" + ticker_symbol[ 0 ]
            lg.log_subscript_start( tagging, "extracting cik for " + ticker_symbol[ 0 ] + "..." )

            df = get_company_details( ticker_symbol[ 0 ] )
            print( df )
            cik = df[ "cik" ].tolist( )[ 0 ]

            lg.log_subscript_finish( tagging, "cik extracted" )
        except Exception as exception:
            lg.log_exception( exception )


        ##set base query
        try:
            tagging = "base_query_" + ticker_symbol[ 0 ]
            lg.log_subscript_start( tagging, "setting base query for " + ticker_symbol[ 0 ] + "..." )

            sql_base_query = """
                with
                    temp_sqlitemaster as(
                        select
                            w1.type
                            , upper( w1.name ) as "ori_name"
                            , upper(
                                replace(
                                     replace(
                                         replace(
                                            lower( w1.name )
                                            , 'operations'
                                            , 'earnings'
                                         )
                                        , 'income'
                                        , 'earnings'
                                     )
                                    , 'balance_sheet'
                                    , 'balance_sheets'
                                )
                              ) as "std_name"
                        from
                            sqlite_master w1
                    )
            """

            lg.log_subscript_finish( tagging, "base query set" )
        except Exception as exception:
            lg.log_exception( exception )


        ##print financial statement tables
        fs_list = [
            ( "balance_sheets_", "balance_sheet", "bs" )
            , ( "earnings_", "income_statement", "is" )
        ]
        '''
        column_names = [
            "financial statement keywords"
            , "financial statement type"
            , "financial statement short name"
        ]
        '''

        for fs_item in fs_list:
            connection = sqlite3_connect( "edgar_transposed" )

            try:
                tagging = "print_tables_" + ticker_symbol[ 0 ] + "_" + fs_item[ 1 ]
                lg.log_subscript_start( tagging, "print tables for " + ticker_symbol[ 0 ] + " " + fs_item[ 1 ] )

                sql_fs_query = """
                    select
                        w1.ori_name as "fs_query"
                    from
                        temp_sqlitemaster   w1
                    left join
                        filing_list         w2 on
                            replace( w1.std_name, rtrim( w1.std_name, replace( w1.std_name, '_', '' ) ), '' )
                            =
                            w2.filing_number
                    where
                        w1.type = 'table'
                    and
                        w1.std_name like '10_K%'
                    and
                        w2.cik = {cik}
                    and
                        lower( w1.std_name ) like '%{fs_item}%'
                    ;
                """.format\
                        (
                            cik = cik
                            , fs_item = fs_item[ 0 ]
                        )

                concat_query = f'{sql_base_query} {sql_fs_query}'
                df = pds.read_sql( concat_query, connection )
                fs_query_list = df[ "fs_query" ].tolist( )
                for x in fs_query_list:
                    print ( x )

                lg.log_subscript_finish( tagging, ticker_symbol[ 0 ] + " " + fs_item[ 1 ] + " tables printed" )
            except Exception as exception:
                lg.log_exception( exception )


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



##=================================
##  INSERT CODE HERE
##=================================

ticker_symbol = "LUV"

# '''
##STEP 1: COMPILE KEY FINANCIAL STATEMENTS FOR MANUAL MAPPING OF KEY ITEMS TO STANDARDISED ANALYSIS TEMPLATE
compile_financial_statements( ticker_symbol )   ##RUN THIS LINE TO FOR MANUAL MAPPING
# '''


# # '''
# ##STEP 2.0: DOWNLOAD FINANCIAL STATEMENT ITEMS MAPPING FROM GOOGLE SHEETS
# ##STEP 2.1: GENERATE SQL STATEMENTS
# try:
#     tagging = "download_prep"
#     lg.log_subscript_start( tagging, "preparing data for downloading..." )

#     working_folder = \
#         otomkdir.otomkdir.auto_create_folder_2(
#             driver_name = "G"
#             ,folder_extend = "My Drive/projects/" + proj_class_name
#             ,subfolder_extend = "financials_working_files"
#         )

#     ticker_gsheet_map = "secticker_gsheet_mapping.csv"

#     fin_stmt_code = [ ]
#     fin_stmt_name = [
#         ##ordering here is very important --> need to match with the order in fin_stmt_code list
#         ( "map_financial_statements_bs", "A4:A" )
#         , ( "map_financial_statements_is", "A4:A" )
#         , ( "map_balance_sheet_columns", "A2:Z" )
#         , ( "map_balance_sheet_parenthetical_columns", "A2:Z" )
#         , ( "map_income_statement_columns", "A2:Z" )
#     ]
#     """
#     column_names = [
#         "original_sheet_name"
#         , "cell_range"
#     ]
#     """

#     df = pds.read_csv( Path( working_folder, ticker_gsheet_map ) )
#     df = df[ df[ "ticker_symbol" ].isin( [ ticker_symbol ] ) ]
#     cik = df[ "cik" ].values.tolist( )[ 0 ]
#     gsheet_file = df[ "gsheet_file" ].values.tolist( )[ 0 ]
#     fin_stmt_code.append( df[ "gsheet_sheet_mapbs" ].values.tolist( )[ 0 ] )
#     fin_stmt_code.append( df[ "gsheet_sheet_mapis" ].values.tolist( )[ 0 ] )
#     fin_stmt_code.append( df[ "gsheet_sheet_bs" ].values.tolist( )[ 0 ] )
#     fin_stmt_code.append( df[ "gsheet_sheet_bs_parent" ].values.tolist( )[ 0 ] )
#     fin_stmt_code.append( df[ "gsheet_sheet_is" ].values.tolist( )[ 0 ] )
#     ticker_cik = f"{ticker_symbol};{cik}"
#     print( ticker_cik )
#     print( fin_stmt_code )

#     lg.log_subscript_finish( tagging, "data prepared for downloading" )
# except Exception as exception:
#     lg.log_exception( exception )



# ##download files
# open_browser( )
# download_folder = Path( os.environ[ "USERPROFILE" ], "Downloads" )
# download_files = [ ]
# for sheet_code, sheet_details in zip( fin_stmt_code, fin_stmt_name ):
#     try:
#         tagging = "download_files"
#         lg.log_subscript_start( tagging, f'downloading {sheet_details[ 0 ]}...' )
        
#         download_gsheet( gsheet_file, sheet_code, sheet_details[ 1 ] )
#         download_file = Path( download_folder, ticker_cik + " - " + sheet_details[ 0 ] + ".csv" )
#         download_files.append( download_file )

#         lg.log_subscript_finish( tagging, f'{sheet_details[ 0 ]} downloaded' )
#     except Exception as exception:
#         lg.log_exception( exception )


# ##generate sql statements
# file_map_list = [
#     ( "balance_sheet_items", ticker_cik + " - map_financial_statements_bs", ticker_cik + " - map_balance_sheet_columns", "bs" )
#     , ( "balance_sheet_items", ticker_cik + " - map_financial_statements_bs", ticker_cik + " - map_balance_sheet_parenthetical_columns", "bs" )
#     , ( "income_statement_items", ticker_cik + " - map_financial_statements_is", ticker_cik + " - map_income_statement_columns", "is" )
# ]
# """
# column_names [
#     "column_name_items"
#     , "master_mapping_file"
#     , "manual_mapping_file"
#     , "financial_statement_shortname"
# ]
# """

# section_line = "-" * 100
# union_all = " union all"
# for file_map_item in file_map_list:
#     try:
#         tagging = "generate_sql"
#         lg.log_subscript_start( tagging, f'generating sql for {file_map_item[ 0 ]}...' )

#         print( section_line )
#         ##generate master financial statement item list
#         df_fs_master = pds.read_csv( Path( download_folder, file_map_item[ 1 ] + ".csv" ) )
#         fs_items = df_fs_master[ file_map_item[ 0 ] ].unique( ).tolist( )

#         ##generate company financial statement item list
#         df_fs_columns = pds.read_csv( Path( download_folder, file_map_item[ 2 ] + ".csv" ) )
#         # df_fs_columns = pds.read_csv( r"C:\Users\User\Downloads\HSY;47111 - map_income_statement_columns.csv" )
#         df_fs_columns = df_fs_columns[ df_fs_columns[ "map_columns" ].notna( ) ]
#         df_fs_columns[ "sign" ].replace( { "positive" : "+", "negative" : "-" }, inplace = True )
#         df_fs_columns[ "express_units" ].replace( { "unit" : "1000000", "thousand" : "1000", "million" : "1" }, inplace = True )    ##to standardise all output to be shown in millions

#         edgar_tables = df_fs_columns[ "edgar_table_name" ].unique( ).tolist( )

#         fs_sql_final = ""
#         for table in edgar_tables:
#             df_fs_columns_filtered = df_fs_columns[ df_fs_columns[ "edgar_table_name" ].isin( [ table ] ) ]
#             map_columns = df_fs_columns_filtered[ "map_columns" ].unique( ).tolist( )

#             ##generate sql statements -- income statement
#             fs_sql = ""
#             for fs_item in fs_items:
#                 if( fs_item in map_columns ):   ##if is_item exists in map_columns then
#                     df_fs_columns_filtered_2 = df_fs_columns_filtered[ df_fs_columns_filtered[ "map_columns" ].isin( [ fs_item ] ) ]
#                     list_columns = df_fs_columns_filtered_2[ [ "sign", file_map_item[ 3 ] +"_clean_columns", "express_units" ] ].values.tolist( )
                    
#                     string = ""
#                     for column in list_columns:
#                         string = f'{string} {column[ 0 ]} cast( coalesce( "{column[ 1 ]}", 0 ) as float )'
#                     full_string = f'( {string} ) / {column[ 2 ]}'
#                 else:                           ##if is_item does not exist in map_columns then
#                     full_string = f'null'
#                 fs_sql = f'{fs_sql} ( {full_string} ) as "{fs_item}",'
#             fs_sql_final = \
#                 """{fs_sql_final} select '{ticker_cik}' as "ticker_cik", '{table}' as "table_name", date as "date", {fs_sql} from "{table}"{union_all}""".\
#                     format(
#                          fs_sql_final = fs_sql_final
#                         , ticker_cik = ticker_cik
#                         , fs_sql = fs_sql[ :-1 ]
#                         , table = table
#                         , union_all = union_all
#                     )
#         print( fs_sql_final[ :-len( union_all ) ] )
#         print( section_line )

#         lg.log_subscript_finish( tagging, f'sql for {file_map_item[ 0 ]} generated' )
#     except Exception as exception:
#         lg.log_exception( exception )


# ##remove downloaded files
# try:
#     tagging = "remove_downloaded_files"
#     lg.log_subscript_start( tagging, f'removing downloaded files...' )

#     for file in download_files:
#         os.remove( file )

#     lg.log_subscript_finish( tagging, f'downloaded files removed' )
# except Exception as exception:
#     lg.log_exception( exception )
# # '''



##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
