import io
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import collections  # From Python standard library.
import bson # need to BSON manipulation
from bson.codec_options import CodecOptions
import urllib.request
import shutil
import tempfile

print("The nfl stats web scraper has began... Please Be Patient")
#Global Variables
nfl_qb_url = "http://www.nfl.com/stats/categorystats?tabSeq=1&season=2017&seasonType=REG&Submit=Go&experience=&archive=true&conference=null&d-447263-p=1&statisticPositionCategory=QUARTERBACK&qualified=true"
header_name_list = []
data_list = []
full_data_list = []
player_profile = {} #dict to hold the player profiles
season_years = [] #dict  to hold possible years for the quarterback stats
session_db_name = 'nfl_stats'
player_position = 'QUARTERBACK'
collection_name = 'players' #MongoDB document collection to hold the player stats
season_year = '2018'

#establish connection with the database; verify availability
def db_connect():
    db_client = MongoClient() #use default port
    try:
        # The ismaster command is cheap and does not require auth.
        db_client.admin.command('ismaster')
        print("Howdy, the mongod server is availabe!\n")
    except ConnectionFailure:
        print("Server not available")

    db_names = db_client.list_database_names()
    db_client.server_info()
    print(f"All databases found: {db_names}\n")
    return db_client
db_client = db_connect()

def create_db_Collection(collection_name, session_db_name):
    db_name = db_client.nfl_stats
    players_collection = db_name.collection_name #database.collection
    return db_name
db_name = create_db_Collection(collection_name, session_db_name)


def to_bytes(a_string):
    a_string_encode = a_string.encode('utf-8')
    return a_string_encode


#url definitions for content scraping
nfl_qb_url = "http://www.nfl.com/stats/categorystats?tabSeq=1&season=2017&seasonType=REG&Submit=Go&experience=&archive=true&conference=null&d-447263-p=1&statisticPositionCategory=QUARTERBACK&qualified=true"


def url_builder(season_year, player_position):
    nfl_stats_base_url_start = "http://www.nfl.com/stats/categorystats?tabSeq=1&season="
    nfl_stats_base_url_mid = "&seasonType=REG&Submit=Go&experience=&archive=true&conference=null&d-447263-p=1&statisticPositionCategory="
    nfl_stats_base_url_end = "&qualified=true"
    nfl_stats_base_url_full = nfl_stats_base_url_start + season_year + nfl_stats_base_url_mid + player_position + nfl_stats_base_url_end
    return nfl_stats_base_url_full

def get_season():
    with urllib.request.urlopen(nfl_qb_url) as response:
        html = response.read()
        html_string = html.decode("cp1252") #decode bytes to string cp1252 is Windows - Western Europe encoding
        soup = BeautifulSoup(html_string, "lxml")
        year_select = soup.find_all('select')

        for option in year_select:
            # option_str = str(option)
            season_counter=0
            exists = 'season-dropdown' in str(option)
            if exists:
                # print(f"found it!: {option}\n")
                for year in option:
                    year_formatted = str(year.string).strip()
                    option_length = round(len(option) / 2)
                    # print(f"option length: {option_length}")
                    if year_formatted != '' and season_counter <= option_length - 2:
                        # print(f"{year_formatted}")
                        season_years.append(year_formatted)
                        # print(f"years list: {season_years}")
                        season_counter += 1
                        # print(f"cntr: {season_counter}, {option_length}")
                    elif year_formatted != '' and season_counter <= option_length - 1:
                        # print("last item")
                        # print(f"{year_formatted}")
                        season_years.append(year_formatted)
                        # print(f"years list: {season_years}")
                        season_counter += 1
                        # print(f"cntr: {season_counter}, {option_length}")
                        return season_years

            else:
                pass
season_years = get_season() #returns a list of all seasons in years (i.e. 2018, 2017, etc.)

def get_nfl_stats(url, player_position, season):
    with urllib.request.urlopen(url) as response:
        html = response.read()
        html_string = html.decode("cp1252") #decode bytes to string cp1252 is Windows - Western Europe encoding
        soup = BeautifulSoup(html_string, "lxml")

        #get all table headers from the page
        table_headers = soup.find_all('th')

        # print(f"table_header length: {len(table_headers)}")
        header_length = len(table_headers)
        head_count = 0
        for h in table_headers:
            if h.string == None:
                h_value = h.a['href'].split('-s=')
                h_value_parse1 = h_value[1].split('&tab')
                h_value_full = h_value_parse1[0]
                if h_value_full != '' and head_count <= header_length - 2:
                    header_name_list.append(h_value_full)
                    head_count += 1
                elif head_count == header_length - 1:
                    header_name_list.append(h_value_full)
                    # print(f"table header list: {header_name_list}")
                    head_count += 1
                h_value_slice = h_value[1]
            else:
                header_name_list.append(h.string)
                head_count += 1

        #get all table data from the page
        table_data = soup.find_all('td')
        data_length = len(table_data)
        # print(data_length)
        data_count = 0
        data_length_counter = 0
        data_index = 0
        for t in table_data:
            if t.string == None and data_count <= header_length - 2:
                # print(f"anchor string: {t.a.string}")
                anchor_string_unicode = str(t.a.string)
                anchor_string_unicode_formatted = anchor_string_unicode.strip()
                data_list.append(anchor_string_unicode_formatted)
                data_count += 1
                data_length_counter += 1
            elif data_count <= header_length - 2 and data_length_counter <= data_length - 1:
                table_string_unicode = str(t.string)
                table_string_unicode_formatted = table_string_unicode.strip()
                data_list.append(table_string_unicode_formatted)
                data_count += 1
                data_length_counter += 1
            elif data_count == header_length - 1:
                table_string_unicode = str(t.string)
                table_string_unicode_formatted = table_string_unicode.strip()
                data_list.append(table_string_unicode_formatted)
                data_length_counter += 1
                data_index = data_list[0] + '_' + data_list[2]
                # print(f"Player profile data [Rank_Team] => '{data_index}': {data_list}\n")
                headerkey = header_length - header_length #set key to zero for headers
                #add in a new list item for season (i.e. "year")
                for profile in data_list:
                    if headerkey <= header_length - 2:
                        player_profile[header_name_list[headerkey]] = profile
                        headerkey += 1
                    else:
                        # print("last item reached - wrap up here insert to mongodb")
                        # print(f"header key is: {headerkey}")
                        # print(f"Profile key, value pair '{header_name_list[headerkey]}','{profile}'")
                        player_profile[header_name_list[headerkey]] = profile
                        player_profile.update({'Season':season})
                        # print(f"player_profile dict: {player_profile}\n")
                        #pass BSON 'decoded' (a Pyton Dict) to retrieve data ready for insertion into MongoD

                        decoded_player_profile_data = bson.BSON.encode(player_profile)
                        decoded_player_profile_doc = bson.BSON.decode(decoded_player_profile_data)

                        options = CodecOptions(document_class=collections.OrderedDict)
                        decoded_player_profile_doc = bson.BSON.decode(decoded_player_profile_data, codec_options=options)
                        # print(f"decoded doc type: {type(decoded_player_profile_doc)}\n")

                        result = db_name.players.insert_one(decoded_player_profile_doc)
                        # print(f"insertion result: {result.inserted_id}")

                full_data_list.append(data_list)
                #reset the couner
                data_count = 0
                del data_list[:]
            else:
                continue

def nfl_scrape_main():
    season_years = get_season()
    p = re.compile('[0-9]+') #compile a regular expression defining a number only pattern
    for season in season_years:
        if p.match(season): #match seasons against regular expression pattern to filter out non-numeric characters
            # print(f"match found: {season}; build url")
            nfl_stats_base_url_full = url_builder(season, player_position)
            # print(f"url built: {nfl_stats_base_url_full}\n passing url to get_nfl_stats")
            get_nfl_stats(nfl_stats_base_url_full, player_position, season)
        else:
            pass
    print(f"The nfl stats scraper has completed successfully for the following years: {season_years}")
nfl_scrape_main()
