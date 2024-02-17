import sys
import os
from pathlib import Path
import time
import datetime
import logtrek.logtrak as lg
import otomkdir

##additional modules
import re
import requests
from bs4 import BeautifulSoup as bs
import pandas as pds
import numpy as npy
import json
import csv


##<< log tracking ===================

proj_class_name = "scuse"
task_name = "tickers"
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
##  INSERT CODE HERE
##=================================

working_folder = \
    otomkdir.otomkdir.auto_create_folder_2(
         driver_name = "D"
        , folder_extend = proj_class_name
        , subfolder_extend = task_name
    )



##OBTAINING SEC TICKER LIST
set_email = "cyehw98@hotmail.com"

headers = {
     "User-Agent": set_email
}


'''
main_website = "https://www.sec.gov"

pages = [
     "company_tickers"
    , "company_tickers_exchange"
]

save_file_keywords = [
     ( "save_files", "saving", "saved" )
    # ,( "backup_files", "backing up", "backed up" )
    ,( "distr_files", "distributing", "distributed" )
]


for idx, page in enumerate( pages ):
    for idy, keyword in enumerate( save_file_keywords ):
        timestamp_01 = datetime.datetime.now( ).strftime( "%Y%m%d-%H%M" )
        try:
            tagging = keyword[ 0 ] + "_" + page + "_" + str( idx ) + "_" + str( idy )
            lg.log_subscript_start( tagging, keyword[ 1 ] + " " + page + "..." )

            output = \
                requests.get(
                     main_website + "/files/{page}.json".\
                         format( page = page )
                    , headers = headers
                )

            output_json = output.json( )
            # print( output_json )


            if keyword[ 0 ] == "backup_files":
                backup_folder = \
                    otomkdir.otomkdir.auto_create_folder_2(
                         driver_name = "D"
                        , folder_extend = proj_class_name + "/" + task_name
                        , subfolder_extend = "backup/" + page + "/history"
                    )

                with open( Path(
                     backup_folder, page + " " + timestamp_01 + ".json" ), "w" ) as out_file:
                    json_formatted = \
                        json.dump(
                             output_json
                            , out_file
                            , indent = 2
                        )
                    out_file.close( )

            elif keyword[ 0 ] == "distr_files":
                distr_folder = \
                    otomkdir.otomkdir.auto_create_folder_2(
                         driver_name = "G"
                        , folder_extend = "My Drive" + "/" + "projects" + "/" + proj_class_name
                    )

                with open( Path( distr_folder, page + ".json" ), "w" ) as out_file:
                    json_formatted = \
                        json.dump(
                             output_json
                            , out_file
                            , indent = 2
                        )
                    out_file.close( )

            elif keyword[ 0 ] == "save_files":
                save_folder = \
                    otomkdir.otomkdir.auto_create_folder_2(
                         driver_name = "D"
                        , folder_extend = proj_class_name + "/" + task_name
                    )

                with open( Path( save_folder, page + ".json" ), "w" ) as out_file:
                    json_formatted = \
                        json.dump(
                             output_json
                            , out_file
                            , indent = 2
                        )
                    out_file.close( )

            lg.log_subscript_finish( tagging, page[ 1 ] + " " + keyword[ 2 ] )
            lg.update_log_status_1( is_log_update_1, 1, tagging )

        except Exception as exception:
            lg.log_exception( exception )
            lg.update_log_status_1( is_log_update_1, 0, tagging )

        time.sleep( 0.5 )







##USING EDGAR SEARCH RESULTS TO PULL OUT SIC CODES
def get_data( url ):
    response = requests.get( url, headers = headers )
    if response.status_code != 200:
        raise ValueError( 'Cannot read the data' )
    return response.text


def get_sic_fiscal( data ):
    soup = bs( data, 'html.parser' )

    # Get the compagny info block
    company_info = soup.find( 'div', { 'class': 'companyInfo' } )

    # Get the acronym tag
    acronym = company_info.find( 'acronym', { 'title': 'Standard Industrial Code' } )

    # find the next url to acronym tag
    try:
        sic = acronym.findNext( 'a' )
        sic = sic.text
        # print( sic )
    except Exception as exception:
        print( exception )
        sic = 0

    # Reduce the search of the fiscal year end only
    # in the compagny info block
    fiscal_year_end = re.search( r'Fiscal Year End:\s+(\d+)', company_info.text )
    if fiscal_year_end:
        return sic, fiscal_year_end.group( 1 )
    return sic, None


filename = "company_tickers_exchange"
tickers_list = Path( "D:", proj_class_name, task_name, filename + ".json" )

with open( tickers_list, "r" ) as read_json:
    rd_json = json.load( read_json )

    json_headers = rd_json[ "fields" ]
    # print( json_headers )

    json_data = rd_json[ "data" ]
    # print( json_data )

df = \
    pds.DataFrame(
         npy.array( json_data )
        , columns = json_headers
    )
# print( df.to_string( max_rows = 7 ) )
df.to_csv(
     Path( working_folder, filename + ".csv" )
    , index = False
)


cik_list = df[ "cik" ].tolist( )
# print( cik_list )



# ##use for testing
# cik_list = [
#      "20"
#     , "320193"
#     , "789019"
#     , "884394"
# ]



with open(
     Path( working_folder, "ticker_sic" + ".csv" )
    , "w"
    , encoding = "utf-8"
    , newline = ""
) as ticker_sic_file:

    ticker_sic_header = [ "cik", "sic", "fiscal_year_end" ]
    writer = csv.writer( ticker_sic_file )
    writer.writerow( ticker_sic_header )

    for cik in cik_list:
        url = \
            'https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik}&owner=exclude&action=getcompany&Find=Search'.\
                format( cik = cik )

        try:
            data = get_data( url )
            sic, fiscal = get_sic_fiscal( data )

            ticker_sic_data = [ cik, sic, fiscal ]

            writer.writerow( ticker_sic_data )
        except Exception as exception:
            print( exception )
            pass
'''


##SEC EDGAR DOWNLOADER -- TO DOWNLOAD REPORTS
from sec_edgar_downloader import Downloader as sec_downloader


ticker_symbols = [
     ( "TSLA", "10-K" )
]
'''
column_names = [
     "ticker_symbol"
    , "filing_type"
]
'''
num_of_recent_rpt = 10

for ticker in ticker_symbols:
    sec_dl = sec_downloader( "G:/My Drive/projects/scuse/companies/" + ticker[ 0 ] + "/" + ticker[ 1 ] )

    sec_dl.get(
         ticker[ 1 ]
        , ticker [ 0 ]
        , amount = num_of_recent_rpt
    )




##=================================
##  CODE ENDS HERE
##=================================

lg.log_script_finish( )
