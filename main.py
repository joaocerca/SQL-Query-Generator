from platform import release
from datetime import timedelta
from loguru import logger
from pandas import read_json
from dotenv import load_dotenv
from os import environ, path

import pandas as pd
import argparse
import json
import sys
import time
import path
import pprint
import requests


"""
    tbl_release:
        releasename [title]
        releaseformat [format][0][name]
        releaseyear [year]
        artistid 
        iscompilation 
        releasedesc [format][0][descriptions]
        recordingtype  
        releasenotes
        releaseid
        releasenotracks (count tracklist)
        releaselength  (      
        
    ex:  Autobahn 	V 	1974 	410 	N 	Coloured Vinyl 	Studio 	Remastered Version (2009) 	V410S2560N 	5 	00:42:48
    
    
    sql_query = f'INSERT INTO tbl_release 
                VALUES ("{releaseName}",
                        "{releaseFormat}",
                        "{releaseYear}",
                        (SELECT artistid FROM tbl_artist WHERE artistname = "{artistName}"),
                        "{isCompilation}",
                        "{releaseDesc}",
                        "{recordingType}",
                        "{releaseNotes}",
                        CONCAT(releaseformat, LEFT(artistid,4), LEFT(recordingtype,1), FLOOR(1 + (RAND() * 9999)), iscompilation),
                        "{releaseNoTracks}",
                        CONVERT("{releaseLength}", TIME));'
    
    ----------------------------------------
    
    tbl_track:
        tracktitle
        trackno
        tracklength
        releaseid
        artistcompid
        trackid
        
    ex: #41 	2 	00:05:38 	CD129L2078Y 	NULL 	CD129L2078Y2
    
    sql_query = f'INSERT INTO tbl_track (trackTitle, trackno, tracklength, releaseid) SELECT "{trackTitle}","{trackNo}",CONVERT("{trackLength}", TIME), releaseid FROM artists_releases_ids WHERE releasename = "{releaseName}" AND artistname = "{artistName}";'
    
    
    -----------------------------------------
    
    tbl_artist:
        artistid
        artistname
        artistcountry
        artistgenre1
        artistsgenre2

"""
basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir,'.env'))

DISCOGS_TOKEN = environ.get("DISCOGS_TOKEN")

global total_duration

def read_json_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)

    return data


def operation_on_tracklist(tracklist):

    updated_track_no = [
        {**track, "position": int(f'{index+1}'), "title":f'{track["title"]}', "duration": f'00:0{track["duration"]}'}
        for index, track in enumerate(tracklist)
    ]

    removed_item = [
        {key: value for key, value in track.items() if key in ['position', 'title', 'duration'] }
        for track in updated_track_no
    ]

    return removed_item





    # release_list = [release['title'], release['formats'][0]['name'], release['year'], release['formats'][0]['descriptions'][3]]

    # return release_list

def create_sql_query_tracks(tracklist, release_title, artist_name, format):

    updated_track_no = [
        {**track, "position": int(f'{index+1}'), "title":f'{track["title"]}', "duration": f'00:0{track["duration"]}'}
        for index, track in enumerate(tracklist)
    ]

    updated_tracklist = [
        {key: value for key, value in track.items() if key in ['position', 'title', 'duration'] }
        for track in updated_track_no
    ]

    if 'Vinyl' == format:
        format = 'V'


    for track in updated_tracklist:

        print(f'INSERT INTO tbl_track (trackTitle, trackno, tracklength, releaseid) SELECT "{track["title"]}",{track["position"]},CONVERT("{track["duration"]}", TIME), releaseid FROM artists_releases_ids WHERE releasename = "{release_title}" AND artistname = "{artist_name}" AND releaseID LIKE "{format}%";')

    # logger.info("Tracks sql")


def create_sql_query_release(year, artist, release_title, format, description, compilation, release_type, no_tracks, total_duration):

    if 'Vinyl' == format:
        format = 'V'


    pprint.pp(f'INSERT INTO tbl_release VALUES("{release_title}", "{format}", "{year}", (SELECT artistid FROM tbl_artist WHERE artistname = "{artist}"), "{compilation}", "{description}", "{release_type}", "releaseNotes",  CONCAT(releaseformat, LEFT(artistid, 4), LEFT(recordingtype, 1), FLOOR(1 + (RAND() * 9999)), iscompilation), \
               "{no_tracks}", CONVERT("{total_duration}", TIME));')

def get_artist(artist_list):

    return str(artist_list['artists'][0]['name'])

def get_info_from_master(url, choice):

    r = requests.get(url)

    try:
        description = r.json()['formats'][0]['text']
        formats = r.json()['formats'][0]['name']
    except KeyError:
        description = 'None'
        formats = 'None'

    artist = r.json()['artists'][0]['name']
    release_title = r.json()['title']

    year = r.json()['year']
    tracklist = r.json()['tracklist']
    no_tracks = len(tracklist)
    total_duration = calc_total_duration(tracklist)


    if choice == 1:
        compilation = input('Compilação (Y/N)? ')
        release_type = input("Studio/Live album: ")

        create_sql_query_release(year, artist, release_title, formats, description, compilation, release_type, no_tracks, total_duration)

    elif choice == 2:

        create_sql_query_tracks(tracklist, release_title, artist, formats)


    # else:
    #     logger.info("This entry does not have track duration")
    #     logger.info(r.json()['tracklist'][0]['duration'])

def calc_total_duration(tracklist):

    total_hours = 0
    total_minutes = 0
    total_seconds = 0


    for track in tracklist:
        print(track['duration'])

        result = track['duration'].split(':')

        minutes = int(result[0])
        seconds = int(result[1])


        total_minutes += minutes
        total_seconds += seconds

    return timedelta(minutes=total_minutes, seconds=total_seconds)

def get_main_results(code):

    r = requests.get(f'https://api.discogs.com/database/search?barcode={code}&token={DISCOGS_TOKEN}')

    return r.json()


def main():
    parser = argparse.ArgumentParser(description="Script to create SQL Queries")
    parser.add_argument('barcode', help='Barcode to be processed')

    args = parser.parse_args()

    file_raw_dict = get_main_results(args.barcode)

    pprint.pp(file_raw_dict)

    if len(file_raw_dict['results']) > 1:

        pprint.pp(f'Há {len(file_raw_dict["results"])} entradas.')
        entry = int(input(f'Escolher (1 a {len(file_raw_dict["results"])}): ')) - 1

    else:
        entry = 0



    print('########################')
    print('1. Adicionar album')
    print("2. Adicionar faixas")
    print("0. Sair")

    choice = int(input("Escolha: "))

    match choice:
        case 0:
            exit()
        case 1:
            if entry == 0:
                get_info_from_master(file_raw_dict['results'][entry]['master_url'], 1)
            else:
                get_info_from_master(file_raw_dict['results'][entry]['resource_url'], 1)

        case 2:
            if entry == 0:
                get_info_from_master(file_raw_dict['results'][entry]['master_url'], 2)
            else:
                get_info_from_master(file_raw_dict['results'][entry]['resource_url'], 2)
        case _:
            print("Opção inválida")
            exit()


    # get_info_from_master(file_raw_dict['results'][entry]['resource_url'], choice)


    # get_general_results(file_raw_dict['results'])


    # artist_name = get_artist(file_raw_dict)
    #
    # print(artist_name)
    #
    # release_list = operation_on_release(file_raw_dict)
    #
    # new_tracklist = operation_on_tracklist(file_raw_dict['tracklist'])
    #
    # create_sql_query_tracks(new_tracklist, release_list, artist_name)




    # match args.filename:
    #     case 'artists':
    #         logger.info("Here is for the artist")
    #         logger.info("#############")
    #         logger.info(file_raw_dict)
    #     case 'release.json':
    #
    #         logger.info("Here is for the release")
    #
    #
    #
    #         logger.info(f'Releasename - {releasename}')
    #         logger.info(f'Releaseformat - {releaseformat}')
    #         logger.info(f'Releaseyear - {releaseyear}')
    #         logger.info(f'Releasedesc - {releasedesc}')
    #
    #         logger.info("#############")
    #     case 'tracks.json':
    #         logger.info("Here is for the tracks")
    #         logger.info("#############")
    #         tracklist = file_raw_dict['tracklist']
    #
    #         # modified_tracklist = [
    #         #     {**track, "position": track["position"].replace("A", "")}
    #         #     for track in tracklist
    #         # ]
    #
    #         operation_on_tracklist(tracklist)
    #
    #         # logger.info(modified_tracklist)
    #     case _:
    #         logger.info("Nothing to show here!")


if __name__ == "__main__":
    main()
