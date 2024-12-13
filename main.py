__license__   = 'GPL v3'
__copyright__ = '2024, Anareaty <reatymain@gmail.com>'
__docformat__ = 'restructuredtext en'


import json, time, re, os, errno, operator
from calibre_plugins.pocketbook_collections.slpp import slpp as lua
import sqlite3 as sqlite
from contextlib import closing
from calibre.library import db as calibre_db
from calibre_plugins.pocketbook_collections.config import prefs
import xml.etree.ElementTree as ET
from datetime import datetime



def set_globals(data):

    global calibreAPI
    global device_DB_path
    global device_main_storage
    global device_metadata_main
    global device_card
    global device_metadata_card
    global db_row_factory 
    global profile_ID
    global storage_prefix_main
    global storage_prefix_card

    current_db = calibre_db(data["dbpath"])
    calibreAPI = current_db.new_api
    device_DB_path = data["device_DB_path"]
    device_main_storage = data["device_storages"]["main"]
    device_metadata_path_main = device_main_storage + "metadata.calibre"
    with open(device_metadata_path_main, "r") as file_main:
        device_metadata_main = json.load(file_main)

    


    device_card = None
    if data["device_storages"].get("card"):
        device_card = data["device_storages"]["card"]
        device_metadata_path_card = device_card + "metadata.calibre"
        with open(device_metadata_path_card, "r") as file_card:
            device_metadata_card = json.load(file_card)

    storage_prefix_main = "/mnt/ext1"
    storage_prefix_card = None
    if device_card:
        storage_prefix_card = "/mnt/ext2"

    profile_ID = str(get_current_profile_ID())
    db_row_factory = lambda cursor, row: {col[0]: row[i] for i, col in enumerate(cursor.description)}


    global sync_read
    sync_read = None
    if prefs['read_lookup_name'] in current_db.all_field_keys():
        sync_read = True

    global sync_read_kr
    sync_read_kr = None
    if sync_read and prefs['sync_kr_status']:
        sync_read_kr = True

    global sync_fav
    sync_fav = None
    if prefs['fav_lookup_name'] in current_db.all_field_keys():
        sync_fav = True

    global sync_fav_kr
    sync_fav_kr = None
    if sync_fav and prefs['sync_kr_fav']:
        sync_fav_kr = True
    
    global sync_shelf
    sync_shelf = None
    if prefs['shelf_lookup_name'] in current_db.all_field_keys():
        sync_shelf = True

    global sync_shelf_kr
    sync_shelf_kr = None
    if sync_shelf and prefs['sync_kr_shelf']:
        sync_shelf_kr = True

    global sync_rating
    sync_rating = None
    if prefs['rating_lookup_name'] in current_db.all_field_keys():
        sync_rating = True

    global sync_review
    sync_review = None
    if prefs['review_lookup_name'] in current_db.all_field_keys():
        sync_review = True

    global load_an
    load_an = None
    if prefs['an_lookup_name'] in current_db.all_field_keys():
        load_an = True

    global load_an_pb
    load_an_pb = None
    if load_an and prefs['sync_pb_an']:
        load_an_pb = True

    global load_an_kr
    load_an_kr = None
    if load_an and prefs['sync_kr_an']:
        load_an_kr = True

    global load_an_cr
    load_an_cr = None
    if load_an and prefs['sync_cr_an']:
        load_an_cr = True

    global sync_pos
    sync_pos = None
    if prefs['position_lookup_name'] in current_db.all_field_keys():
        sync_pos = True

    global sync_pos_pb
    sync_pos_pb = None
    if sync_pos and prefs['sync_pb_pos']:
        sync_pos_pb = True

    global sync_pos_kr
    sync_pos_kr = None
    if sync_pos and prefs['sync_kr_pos']:
        sync_pos_kr = True

    global sync_pos_cr
    sync_pos_cr = None
    if sync_pos and prefs['sync_cr_pos']:
        sync_pos_cr = True

    global sync_statuses
    sync_statuses = None
    if sync_read or sync_fav:
        sync_statuses = True
    
    global sync_all
    sync_all = None
    if sync_shelf or sync_statuses or sync_rating or sync_review:
        sync_all = True

    global sync_db
    sync_db = None
    if sync_shelf or sync_statuses:
        sync_db = True

    global sync_kr 
    sync_kr = None
    if sync_read_kr or sync_rating or sync_review:
        sync_kr = True

    global sync_kr_collections
    sync_kr_collections = None
    if sync_shelf_kr or sync_fav_kr:
        sync_kr_collections = True

    global pref_fav_kr
    pref_fav_kr = None
    if sync_fav_kr and prefs['prefer_kr_fav']:
        pref_fav_kr = True

    global pref_shelf_kr
    pref_shelf_kr = None
    if sync_shelf_kr and prefs['prefer_kr_shelf']:
        pref_shelf_kr = True

    





def send_all(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."

    if sync_all:
        if sync_db:
            db = prepare_explorer_db()
        if sync_shelf:
            create_shelfs_dicts(db)
        if sync_kr_collections:
            get_kr_collections()
            create_missing_kr_collections()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata and sync_db:
                book.process_db(db)
                if book.book_row:
                    if sync_shelf:
                        book.send_book_collections(db)
                    if sync_statuses:
                        book.send_statuses(db)
                if sync_kr:
                    book.get_kr_metadata()
                if sync_shelf_kr:
                    book.send_book_collections_kr()
                if sync_read_kr:
                    book.send_read_kr()
                if sync_fav_kr:
                    book.send_fav_kr()
                if sync_rating:
                    book.send_rating_kr()
                if sync_review:
                    book.send_review_kr()

        if sync_db:
            db.close()
        if kr_collections:
            update_kr_collections(kr_collections)
    done_msg = "Sending metadata finished"
    return None, done_msg



def send_collections(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."

    if sync_shelf:
        db = prepare_explorer_db()
        create_shelfs_dicts(db)
        if sync_kr_collections:
            get_kr_collections()
            create_missing_kr_collections()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                book.process_db(db)
                if book.book_row:
                    book.send_book_collections(db)
                if sync_shelf_kr:
                    book.get_kr_metadata()
                    book.send_book_collections_kr()

        db.close()
        if sync_shelf_kr:
            update_kr_collections(kr_collections)
    done_msg = "Sending collections finished"
    return None, done_msg








def send_read(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."

    if sync_read:
        db = prepare_explorer_db()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                book.process_db(db)
                if book.book_row:
                    book.send_read_status(db)

                if sync_read_kr:
                    book.get_kr_metadata()
                    book.send_read_kr()

        db.close()
    done_msg = "Sending read finished"
    return None, done_msg






def send_favorite(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."

    if sync_fav:
        db = prepare_explorer_db()
        if sync_fav_kr:
            get_kr_collections()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                book.process_db(db)
                if book.book_row:
                    book.send_favorite_status(db)
                if sync_fav_kr:
                    book.send_fav_kr()

        db.close()
        if sync_fav_kr:
            update_kr_collections(kr_collections)
    done_msg = "Sending favorite finished"
    return None, done_msg






def send_ratings(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."
    if sync_rating:
        #prepare_explorer_db()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                book.get_kr_metadata()
                book.send_rating_kr()
    done_msg = "Sending ratings finished"
    return None, done_msg





def send_reviews(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."
    if sync_review:
        #prepare_explorer_db()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                book.get_kr_metadata()
                book.send_review_kr()
    done_msg = "Sending reviews finished"
    return None, done_msg





def load_all(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."

    to_load = {
        "shelf": {},
        "read": {},
        "fav": {},
        "ratings": {},
        "reviews": {},
        "books_to_refresh": []
    }


    if sync_all:
        if sync_db:
            db = prepare_explorer_db()
        if sync_shelf:
            create_shelfs_dicts(db)
        if sync_kr_collections:
            get_kr_collections()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata: 
                to_load_shelf = None
                to_load_read = None
                to_load_fav = None
                to_load_rating = None
                to_load_review = None
                if sync_db:
                    book.process_db(db)
                if sync_kr:
                    book.get_kr_metadata()
                if book.book_row:
                    if sync_shelf:
                        if pref_shelf_kr:
                            to_load_shelf = book.load_book_collections(db)
                        else:
                            to_load_shelf = book.load_shelf_kr()
                    if sync_statuses:
                        to_load_statuses = book.load_statuses(db)
                        if to_load_statuses["read"]:
                            to_load_read = to_load_statuses["read"]
                        if to_load_statuses["fav"]:
                            to_load_fav = to_load_statuses["fav"]
                if sync_read_kr:
                    book.get_kr_metadata()
                    to_load_read_kr = book.load_read_kr()
                    if to_load_read_kr:
                        to_load_read = to_load_read_kr
                if pref_fav_kr:
                    to_load_fav = book.load_fav_kr()
                if sync_rating:
                    to_load_rating = book.load_rating_kr()
                if sync_review:
                    to_load_review = book.load_review_kr()

                if to_load_shelf or to_load_read or to_load_fav or to_load_rating or to_load_review:
                    to_load["books_to_refresh"].append(calibre_book_ID)
                if to_load_shelf:
                    to_load["shelf"][calibre_book_ID] = to_load_shelf
                if to_load_read:
                    to_load["read"][calibre_book_ID] = to_load_read["status"]
                if to_load_fav:
                    to_load["fav"][calibre_book_ID] = to_load_fav["status"]
                if to_load_rating:
                    to_load["ratings"][calibre_book_ID] = to_load_rating
                if to_load_review:
                    to_load["reviews"][calibre_book_ID] = to_load_review
                  

        if sync_db:
            db.close()
    done_msg = "Sending metadata finished"
    return to_load, done_msg

















def load_collections(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."
    
    to_load = {
        "shelf": {},
        "books_to_refresh": []
    }

    if sync_shelf:
        if not pref_shelf_kr:
            db = prepare_explorer_db()
            create_shelfs_dicts(db)
        else:
            get_kr_collections()
        
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                to_load_shelf = None
                if not pref_shelf_kr:
                    book.process_db(db)
                    if book.book_row:
                        to_load_shelf = book.load_book_collections(db)
                else:
                    to_load_shelf = book.load_shelf_kr()
                if to_load_shelf:
                    to_load["shelf"][calibre_book_ID] = to_load_shelf
                    to_load["books_to_refresh"].append(calibre_book_ID)
        if not pref_fav_kr:
            db.close()
    done_msg = "Loaded favorite"
    return to_load, done_msg






def load_read(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."
    
    to_load = {
        "read": {},
        "books_to_refresh": []
    }
    
    if sync_read:
        db = prepare_explorer_db()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                to_load_read = None
                book.process_db(db)
                if book.book_row:
                    to_load_read = book.load_read_status(db)
                if sync_read_kr:
                    book.get_kr_metadata()
                    to_load_read_kr = book.load_read_kr()
                    if to_load_read_kr:
                        to_load_read = to_load_read_kr
                if to_load_read:
                    to_load["read"][calibre_book_ID] = to_load_read["status"]
                    to_load["books_to_refresh"].append(calibre_book_ID)
        
        db.close()
    done_msg = "Loaded read"
    return to_load, done_msg








def load_favorite(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."
    
    to_load = {
        "fav": {},
        "books_to_refresh": []
    }
    
    if sync_fav:
        if not pref_fav_kr:
            db = prepare_explorer_db()
        else:
            get_kr_collections()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                to_load_fav = None
                if not pref_fav_kr:
                    book.process_db(db)
                    if book.book_row:
                        to_load_fav = book.load_favorite_status(db)
                else:
                    to_load_fav = book.load_fav_kr()
                if to_load_fav:
                    to_load["fav"][calibre_book_ID] = to_load_fav["status"]
                    to_load["books_to_refresh"].append(calibre_book_ID)
        
        if not pref_fav_kr:
            db.close()
    done_msg = "Loaded favorite"
    return to_load, done_msg












def load_reviews(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."

    to_load = {
        "reviews": {},
        "books_to_refresh": []
    }

    if sync_review:
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                to_load_review = None
                book.get_kr_metadata()
                to_load_review = book.load_review_kr()
                if to_load_review:
                    to_load["reviews"][calibre_book_ID] = to_load_review
                    to_load["books_to_refresh"].append(calibre_book_ID)

    done_msg = "Loaded reviews"
    return to_load, done_msg






def load_ratings(data):
    set_globals(data)

    to_load = {
        "ratings": {},
        "books_to_refresh": []
    }

    if sync_rating:
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                to_load_rating = None
                book.get_kr_metadata()
                to_load_rating = book.load_rating_kr()
                if to_load_rating:
                    to_load["ratings"][calibre_book_ID] = to_load_rating
                    to_load["books_to_refresh"].append(calibre_book_ID)
                    
    done_msg = "Loaded ratings"
    return to_load, done_msg








def sync_position(data):
    set_globals(data)

    to_load = {
        "position": {},
        "books_to_refresh": []
    }
    if sync_position:
        if sync_pos_pb:
            db = prepare_explorer_db()
        if sync_pos_cr:
            get_cr3hist_path()

        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                positions = {}
                if sync_pos_pb:
                    book.process_db(db)
                    to_load_position_pb = book.pb_sync_position(db)
                    if to_load_position_pb:
                        positions["pb"] = to_load_position_pb
                if sync_pos_kr:
                    book.get_kr_metadata()
                    to_load_position_kr = book.kr_sync_position()
                    if to_load_position_kr:
                        positions["kr"] = to_load_position_kr
                if sync_pos_cr:
                    to_load_position_cr = book.cr_sync_position()
                    if to_load_position_cr:
                        positions["cr"] = to_load_position_cr

                
                if len(positions) > 0:
                    to_load["position"][calibre_book_ID] = str(positions)
                    to_load["books_to_refresh"].append(calibre_book_ID)
    done_msg = "Synced"
    return to_load, done_msg      







def force_position(data):
    set_globals(data)

    if sync_position:
        if sync_pos_pb:
            db = prepare_explorer_db()
        if sync_pos_cr:
            get_cr3hist_path()

        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                if sync_pos_pb:
                    book.process_db(db)
                    book.pb_force_position(db)
                if sync_pos_kr:
                    book.get_kr_metadata()
                    book.kr_force_position()
                if sync_pos_cr:
                    book.cr_force_position()
    
    done_msg = "Synced"
    return None, done_msg      







def extract_annotations(data):
    try:
        set_globals(data)
    except:
        return "error", "Can not reach device metadata, it is probably still updating. Try again later."

    to_load = {
        "annotations": {},
        "books_to_refresh": []
    }

    if load_an:
        if load_an_pb:
            db = prepare_books_db()
        if load_an_cr:
            get_cr3hist_path()
        calibre_book_IDs = calibreAPI.all_book_ids()

        for calibre_book_ID in calibre_book_IDs:
            book = SyncBook(calibre_book_ID)
            if book.device_book_metadata:
                if load_an_pb:
                    book.get_pb_annotations(db)
                if load_an_kr:
                    book.get_kr_annotations()
                if load_an_cr:
                    book.get_cr_annotations()
                if len(book.new_annotations) > 0:
                    book.generate_annotations_html()
                
            if book.annotations_html:
                try:
                    book.annotations_html = book.annotations_html.decode("utf-8")
                except:
                    pass

                to_load["annotations"][calibre_book_ID] = book.annotations_html
                to_load["books_to_refresh"].append(calibre_book_ID)

    done_msg = "Loading annotations finished"
    return to_load, done_msg              
                







def prepare_explorer_db():
    db = sqlite.connect(device_DB_path)
    db.row_factory = db_row_factory
    clean_database(db)
    #set_storage_prefixes(db)
    get_books_missing_author_ids(db)
    return db




def prepare_books_db():
    books_db_path = os.path.join(device_main_storage, "system", "config", "books.db")
    db = sqlite.connect(books_db_path)
    db.row_factory = lambda cursor, row: {col[0]: row[i] for i, col in enumerate(cursor.description)}
    return db


def clean_database(db):
    cursor = db.cursor()
    cursor.execute("DELETE FROM books_settings WHERE bookid IN (SELECT id FROM books_impl WHERE id NOT IN (SELECT book_id FROM files))")
    cursor.execute("DELETE FROM booktogenre WHERE bookid IN (SELECT id FROM books_impl WHERE id NOT IN (SELECT book_id FROM files))")
    cursor.execute("DELETE FROM social WHERE bookid IN (SELECT id FROM books_impl WHERE id NOT IN (SELECT book_id FROM files))")
    cursor.execute("DELETE FROM books_impl WHERE id NOT IN (SELECT book_id FROM files)")
    cursor.execute("DELETE FROM books_fast_hashes WHERE book_id NOT IN (SELECT book_id FROM files)")
    db.commit()


def get_current_profile_ID():
    profileLinkPath = os.path.join(device_main_storage, "system", "profiles", ".current.lnk")
    if os.path.exists(profileLinkPath):
        with open(profileLinkPath,'rb') as file:
            link = str(file.read())
            profileName = re.sub(r".*/", "", link).replace("'", "")
        with closing(sqlite.connect(device_DB_path)) as db:
            db.row_factory = lambda cursor, row: {col[0]: row[i] for i, col in enumerate(cursor.description)}
            cursor = db.cursor()
            profile_ID = cursor.execute("SELECT id FROM profiles WHERE name = '" + profileName + "'").fetchone()["id"]
    else:
        profile_ID = 1    
    return profile_ID


def get_books_missing_author_ids(db):
    cursor = db.cursor()
    books_missing_authors = cursor.execute("SELECT id FROM books_impl WHERE author = ''").fetchall()
    global books_missing_authors_IDs
    books_missing_authors_IDs = [book["id"] for book in books_missing_authors]


def get_int_timestamp(datetime):
    try:
        timestamp = int(datetime.timestamp())
    except:
        timestamp = int(time.mktime(datetime.timetuple()))
    return timestamp



def create_shelfs_dicts(db):
    global shelf_dicts
    cursor = db.cursor()

    shelf_dicts = {
        "shelfsReaderToCalibre": {},
        "shelfsNameToReader": {},
    }

    calibreShelfIDs = calibreAPI.all_field_ids(prefs["shelf_lookup_name"])
    if calibreShelfIDs == None:
        calibreShelfIDs = []

    readerShelfRows = cursor.execute("SELECT id, name FROM bookshelfs").fetchall()

    for calibreShelfID in calibreShelfIDs:
        shelfName = calibreAPI.get_item_name(prefs["shelf_lookup_name"], calibreShelfID)
        readerShelfID = None

        i = 0
        while i < len(readerShelfRows):
            readerShelf = readerShelfRows[i]
            if readerShelf['name'] == shelfName:
                readerShelfID = readerShelf["id"]
                shelf_dicts["shelfsReaderToCalibre"][readerShelfID] = {"calibreShelfID": calibreShelfID, "name": shelfName}
                del readerShelfRows[i]
            else:
                i += 1

        shelf_dicts["shelfsNameToReader"][shelfName] = readerShelfID
            
    for readerShelf in readerShelfRows:
        readerShelfID = readerShelf["id"]
        shelfName = readerShelf["name"]
        shelf_dicts["shelfsNameToReader"][shelfName] = readerShelfID

        if readerShelfID != None:
            shelf_dicts["shelfsReaderToCalibre"][readerShelfID] = {"calibreShelfID": None, "name": shelfName}





def get_kr_collections():
    global kr_collections
    kr_collections = None
    kr_collections_path = device_main_storage + "applications/koreader/settings/collection.lua"

    if os.path.exists(kr_collections_path):
        with open(kr_collections_path, 'r') as file:
            content = str(file.read())
        lua_content = re.sub('^[^{]*', '', content).strip()
        try:
            kr_collections = lua.decode(lua_content)
            if kr_collections == None:
                kr_collections = {}
        except:
            pass



def create_missing_kr_collections():
    global kr_collections
    if kr_collections != None:
        shelfs = calibreAPI.all_field_names(prefs["shelf_lookup_name"])
        kr_collections_list = list(kr_collections)

        for shelf in shelfs:
            if shelf not in kr_collections_list:
                kr_collections[shelf] = {}



def update_kr_collections(kr_collections):
    
    kr_collections_path = device_main_storage + "applications/koreader/settings/collection.lua"
    sidecar_lua = lua.encode(kr_collections)
    sidecar_lua_formatted = "-- updated from Calibre\nreturn " + sidecar_lua + "\n"

    try:
        with open(kr_collections_path, "w", encoding="utf-8") as f:
            f.write(sidecar_lua_formatted)
    except:
        sidecar_lua_formatted = sidecar_lua_formatted.encode("utf-8")
        with open(kr_collections_path, "w") as f:
            f.write(sidecar_lua_formatted)




def get_cr3hist_path():
    global cr3hist_path
    profileLinkPath = os.path.join(device_main_storage, "system", "profiles", ".current.lnk")

    if os.path.exists(profileLinkPath):
        with open(profileLinkPath,'rb') as file:
            link = str(file.read())
            profileName = re.sub(r".*/", "", link).replace("'", "")

            link_split = link.split("/")
            link_storage = "/" + link_split[-5] + "/" + link_split[-4]

            if link_storage == storage_prefix_main:
                cr3hist_path = os.path.join(device_main_storage, 'system', "profiles", profileName,  "state", "cr3", ".cr3hist")

            if link_storage == storage_prefix_card:
                cr3hist_path = os.path.join(device_card, 'system', "profiles", profileName,  "state", "cr3", ".cr3hist")

    else:
        cr3hist_path = os.path.join(device_main_storage, 'system', "state", "cr3", ".cr3hist")



def make_dir(path):
    try:
        os.makedirs(path, exist_ok=True)  # Python>3.2
    except TypeError:
        try:
            os.makedirs(path)
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else: raise



class SyncBook():

    def __init__(self, calibre_book_ID):
        self.calibre_book_ID = calibre_book_ID
        self.book_row = None
        self.get_device_book_metadata()
        self.new_annotations = {}
        self.annotations_html = None
        
        
            

    def get_device_book_metadata(self):
        # Find the book in Calibre metadata objects on device. We must check both the main storage and the card, it it is exist.
        self.device_book_metadata = next((bookData for bookData in device_metadata_main if bookData["application_id"] == self.calibre_book_ID), None)
        storage_prefix = storage_prefix_main                

        if self.device_book_metadata == None and device_card:
            self.device_book_metadata = next((bookData for bookData in device_metadata_card if bookData["application_id"] == self.calibre_book_ID), None)
            storage_prefix = storage_prefix_card

        if self.device_book_metadata:
            bookPath = self.device_book_metadata["lpath"]
            self.book_fullpath = storage_prefix + "/" + bookPath
            self.book_filesize = self.device_book_metadata["size"]

            # Get the filename an folder of the book

            self.bookPath_file = self.book_fullpath.rsplit('/', 1)[1].replace("'", r"''")
            self.bookPath_folder = self.book_fullpath.rsplit('/', 1)[0].replace("'", r"''")

    

    def get_book_explorer_data(self, db):
        cursor = db.cursor()
        self.book_row = None
        
        # If bookData is found, it means it is on device and indexed by Calibre. Proceed to check if it is indexed by Pocketbook
        if self.device_book_metadata:

            # Find the folder in Pocketbook database
            book_folder_row = cursor.execute("SELECT id FROM folders WHERE name = '" + self.bookPath_folder + "'").fetchone()

            if book_folder_row:
                book_folder_id = str(book_folder_row["id"])

                # Find the book in Pocketbook database
                self.book_row = cursor.execute("SELECT book_id as id, filename FROM files WHERE folder_id = " + book_folder_id + " AND filename = '" + self.bookPath_file + "'").fetchone()
            
        # If the book exist and indexed by Pocketbook proceed to sync metadata between the reader and Calibre

        if self.book_row:
            self.reader_book_ID = str(self.book_row["id"])

            # Find the timestamp of the last modification in Calibre
            lastMod = calibreAPI.field_for("last_modified", self.calibre_book_ID)
            self.lastModTS = get_int_timestamp(lastMod)
        


    def fix_missing_authors(self, db):
        cursor = db.cursor()
        if self.book_row and self.book_row["id"] in books_missing_authors_IDs:
            title = calibreAPI.field_for("title", self.calibre_book_ID)
            authors = calibreAPI.field_for("author_sort", self.calibre_book_ID, default_value=[])
            authors_string = authors.replace(",", "").replace(" & ", ", ")
            author_first = authors.split(" & ")[0].split(",")[0]
            author_first_letter = author_first[0]
            book_format = self.bookPath_file.rsplit('.', 1)[1] 

            if book_format == "pdf":
                title = calibreAPI.field_for("title", self.calibre_book_ID)
                cursor.execute("UPDATE books_impl SET title = '" + title + "', author = '" + authors_string + "', firstauthor = '" + author_first + "', first_author_letter = '" + author_first_letter + "' WHERE id = " + self.reader_book_ID)
            else:
                cursor.execute("UPDATE books_impl SET author = '" + authors_string + "', firstauthor = '" + author_first + "', first_author_letter = '" + author_first_letter + "' WHERE id = " + self.reader_book_ID)
            db.commit()
        


    def process_db(self, db):
        self.get_book_explorer_data(db)
        self.fix_missing_authors(db)



    def get_kr_metadata(self):
    
        self.kr_metadata = None
        format = self.book_fullpath.rsplit('.', 1)[1]

        # Look for sidecar in book folder
        sidecar_path_device = self.book_fullpath.rsplit('.', 1)[0] + ".sdr/metadata." + format + ".lua"
        self.sidecar_path = sidecar_path_device.replace(storage_prefix_main + "/", device_main_storage)
        
        if storage_prefix_card:
            self.sidecar_path = sidecar_path_device.replace(storage_prefix_card + "/", device_card)

        if not os.path.exists(self.sidecar_path):
            # Look for sidecar in docsettings folder
            self.sidecar_path = device_main_storage + "applications/koreader/docsettings" + sidecar_path_device

        if os.path.exists(self.sidecar_path):
            with open(self.sidecar_path, 'r') as file:
                content = str(file.read())
            lua_content = re.sub('^[^{]*', '', content).strip()
            try:
                self.kr_metadata = lua.decode(lua_content)
            except:
                pass




    



    def update_kr_sidecar(self):
        
        sidecar_lua = lua.encode(self.kr_metadata)
        sidecar_lua_formatted = "-- updated from Calibre\nreturn " + sidecar_lua + "\n"
        try:
            with open(self.sidecar_path, "w", encoding="utf-8") as f:
                f.write(sidecar_lua_formatted)
        except:
            sidecar_lua_formatted = sidecar_lua_formatted.encode("utf-8")
            with open(self.sidecar_path, "w") as f:
                f.write(sidecar_lua_formatted)




    def generate_kr_sidecar(self, position):
        self.sidecar_path = None
        format = self.book_fullpath.rsplit('.', 1)[1]
        kr_settings_reader_path = device_main_storage + "applications/koreader/settings.reader.lua"

        if os.path.exists(kr_settings_reader_path):
            with open(kr_settings_reader_path, 'r') as file:
                content = str(file.read())
                lua_content = re.sub('^[^{]*', '', content).strip()
            try:
                settings = lua.decode(lua_content)
                metadata_folder = settings["document_metadata_folder"]
                sidecar_path_device = self.book_fullpath.rsplit('.', 1)[0] + ".sdr/metadata." + format + ".lua"
                if metadata_folder == "doc":
                    self.sidecar_path = sidecar_path_device.replace(storage_prefix_main + "/", device_main_storage)
                    if storage_prefix_card:
                        self.sidecar_path = sidecar_path_device.replace(storage_prefix_card + "/", device_card)
                elif metadata_folder == "dir":
                    self.sidecar_path = device_main_storage + "applications/koreader/docsettings" + sidecar_path_device
                else:
                    pass
            except:
                pass

        if self.sidecar_path:
            self.kr_metadata = {
                "cre_dom_version": 20240114,
                "doc_path": self.book_fullpath,
                "last_xpointer": position
            }

            dir = os.path.split(self.sidecar_path)[0]
            make_dir(dir)

            self.update_kr_sidecar()





    def send_book_collections(self, db):
        cursor = db.cursor()

        # Find the list of existing collections names for the book in Calibre
        calibreBookShelfNames = calibreAPI.field_for(prefs["shelf_lookup_name"], self.calibre_book_ID, default_value=[])
    
        # Find the list of existing collections objects for the book on reader
        readerBookShelfs = cursor.execute("SELECT bookshelfid, is_deleted, ts FROM bookshelfs_books WHERE bookid = " + self.reader_book_ID).fetchall()

        # Check every collection in Calibre
        for calibreBookShelfName in calibreBookShelfNames:

            # For each collection in Calibre find corresponding collection on reader
            readerBookShelfID = shelf_dicts["shelfsNameToReader"][calibreBookShelfName]

            # If the collection in reader does not exist we must create it
            if readerBookShelfID == None:
                # First create the collection itself
                cursor.execute("INSERT INTO bookshelfs(name, is_deleted, ts) VALUES('" + calibreBookShelfName + "', 0, " + str(self.lastModTS) + ")")
                db.commit()
                readerBookShelfID = cursor.execute("SELECT id FROM bookshelfs WHERE name = '" + calibreBookShelfName +"'").fetchone()["id"]

                # The add book to collection
                cursor.execute("INSERT INTO bookshelfs_books(bookshelfid, bookid, ts, is_deleted) VALUES(" + str(readerBookShelfID) + ", " + self.reader_book_ID + ", " + str(self.lastModTS) + ", 0)")
                db.commit()
                
            # If the collection in reader does exist, we must check if it has the book
            else:
                # Find the collection in the list of reader collections for the book 
                readerBookShelf = None
                for readerShelfRow in readerBookShelfs:
                    if readerShelfRow["bookshelfid"] == readerBookShelfID:
                        readerBookShelf = readerShelfRow

                # If the book is in collection check if it is marked as deleted
                if readerBookShelf:
                    if readerBookShelf["is_deleted"]:
                        # If deleted mark as not deleted
                        cursor.execute("UPDATE bookshelfs_books SET is_deleted = 0, ts = " + str(self.lastModTS) + " WHERE bookid = " + self.reader_book_ID + " AND bookshelfid = " + str(readerBookShelfID))
                        db.commit()

                # If the book is not in collection, add it to collection
                else:
                    cursor.execute("INSERT INTO bookshelfs_books(bookshelfid, bookid, ts, is_deleted) VALUES(" + str(readerBookShelfID) + ", " + self.reader_book_ID + ", " + str(self.lastModTS) + ", 0)")
                    db.commit()

        # Check every collection in reader
        for readerBookShelfRow in readerBookShelfs:

            # Get collection name and id in reader
            readerBookShelfID = readerBookShelfRow["bookshelfid"]
            shelfName = shelf_dicts["shelfsReaderToCalibre"][readerBookShelfID]["name"]

            # Look for the corresponding collection in Calibre
            calibreShelf = None
            for calibreBookShelfName in calibreBookShelfNames:
                if calibreBookShelfName == shelfName:
                    calibreShelf = calibreBookShelfName

            # If collection is not in Calibre and not marked as deleted in reader, we must mark it as deleted
            if calibreShelf == None and readerBookShelfRow["is_deleted"] != 1:
                cursor.execute("UPDATE bookshelfs_books SET is_deleted = 1, ts = " + str(self.lastModTS) + " WHERE bookid = " + self.reader_book_ID + " AND bookshelfid = " + str(readerBookShelfID))
                db.commit()
            # If collection is in Calibre, it is already processed, so do nothing.




    

    def send_book_collections_kr(self):
        
        global kr_collections
        if kr_collections:
            calibreBookShelfNames = calibreAPI.field_for(prefs["shelf_lookup_name"], self.calibre_book_ID, default_value=[])

            for c in kr_collections:
                collection = kr_collections[c]
                file_key = None
                fixed = {}
                count = 1
                if c == "favorites":
                    pass
                elif c in calibreBookShelfNames:
                    # Проверить, есть ли книга в коллекции, если нет, то добавить
                    if "settings" not in collection:
                        fixed["settings"] = {"order": 1}
                    for key in collection:
                        if key == "settings":
                            fixed["settings"] = collection[key]
                        else:
                            fixed[count] = collection[key]
                            count = count + 1
                            if "file" in collection[key] and collection[key]["file"] == self.book_fullpath:
                                file_key = key
                    if file_key == None:
                        next_key = len(fixed)
                        fixed[next_key] = {"file": self.book_fullpath, "order": next_key}
                    kr_collections[c] = fixed
                else:
                    # Проверить, есть ли книга в коллекции, если да, то удалить
                    for key in collection:
                        if key == "settings":
                            fixed["settings"] = collection[key]
                        elif "file" in collection[key] and collection[key]["file"] == self.book_fullpath:
                            pass
                        else:
                            fixed[count] = collection[key]
                            count = count + 1
                    kr_collections[c] = fixed




    def send_read_status(self, db):
        cursor = db.cursor()

        # Find read status in Calibre

        read = calibreAPI.field_for(prefs["read_lookup_name"], self.calibre_book_ID)
        completed = "0"
        if read: 
            completed = "1"

        # Find read status in reader

        statusRow = cursor.execute("SELECT bookid, completed FROM books_settings WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID).fetchone()

        # If status in reader exist and different from Calibre, update status in reader
        if statusRow:
            if completed != str(statusRow["completed"]):
                cursor.execute("UPDATE books_settings SET completed = " + str(completed) + ", completed_ts = " + str(self.lastModTS) + " WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID)
                db.commit()
        # If status in reader do not exist, insert new status row
        else:
            cursor.execute("INSERT INTO books_settings(bookid, profileid, completed, completed_ts) VALUES(" + self.reader_book_ID + ", " + profile_ID + ", " + str(completed) + ", " + str(self.lastModTS) + ")")
            db.commit()



    def send_read_kr(self):
        read = False
        if self.kr_metadata:
            read = calibreAPI.field_for(prefs['read_lookup_name'], self.calibre_book_ID, default_value=None)
        if read:
            if "summary" not in self.kr_metadata:
                self.kr_metadata["summary"] = {}
            if "status" not in self.kr_metadata["summary"] or self.kr_metadata["summary"]["status"] != "complete":
                self.kr_metadata["summary"]["status"] = "complete"
                self.update_kr_sidecar()



    def send_favorite_status(self, db):
        cursor = db.cursor()

        self.get_book_explorer_data(db)
        self.fix_missing_authors(db)

        # Find favorite status in Calibre

        fav = calibreAPI.field_for(prefs["fav_lookup_name"], self.calibre_book_ID)
        favorite = "0"
        if fav: 
            favorite = "1"

        # Find favorite status in reader

        statusRow = cursor.execute("SELECT bookid, favorite FROM books_settings WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID).fetchone()

        # If status in reader exist and different from Calibre, update status in reader
        if statusRow:
            if favorite != str(statusRow["favorite"]):
                cursor.execute("UPDATE books_settings SET favorite = " + str(favorite) + ", favorite_ts = " + str(self.lastModTS) + " WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID)
                db.commit()
        # If status in reader do not exist, insert new status row
        else:
            cursor.execute("INSERT INTO books_settings(bookid, profileid, favorite, favorite_ts) VALUES(" + self.reader_book_ID + ", " + profile_ID + ", " + str(favorite) + ", " + str(self.lastModTS) + ")")
            db.commit()




    def send_fav_kr(self):
        global kr_collections
        if kr_collections:
            favorite = calibreAPI.field_for(prefs['fav_lookup_name'], self.calibre_book_ID, default_value=None)
            if "favorites" in kr_collections:
                favorites = kr_collections["favorites"]
            else:
                favorites = {}
            file_key = None
            favorites_fixed = {}
            count = 1
            if "settings" not in favorites:
                favorites_fixed["settings"] = {"order": 1}
            for key in favorites:
                if key == "settings":
                    favorites_fixed["settings"] = favorites[key]
                elif "file" in favorites[key] and favorites[key]["file"] == self.book_fullpath:
                    file_key = key
                    if favorite:
                        favorites_fixed[count] = favorites[key]
                        count = count + 1
                    else:
                        pass
                else:
                    favorites_fixed[count] = favorites[key]
                    count = count + 1

            if file_key == None and favorite:
                next_key = len(favorites_fixed)
                favorites_fixed[next_key] = {"file": self.book_fullpath, "order": next_key}
                
            kr_collections["favorites"] = favorites_fixed



    def send_statuses(self, db):
        cursor = db.cursor()
        completed = "0"
        favorite = "0"

        read = calibreAPI.field_for(prefs["read_lookup_name"], self.calibre_book_ID)
        if read: 
            completed = "1"

        fav = calibreAPI.field_for(prefs["fav_lookup_name"], self.calibre_book_ID)
        if fav: 
            favorite = "1"
        
        # Find statuses in reader

        statusRow = cursor.execute("SELECT bookid, completed, favorite FROM books_settings WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID).fetchone()

        # If any status in reader exist and different from Calibre, update statuses in reader
        if statusRow:
            if completed != str(statusRow["completed"]) or favorite != str(statusRow["favorite"]):
                cursor.execute("UPDATE books_settings SET completed = " + str(completed) + ", completed_ts = " + str(self.lastModTS) + ", favorite = " + str(favorite) + ", favorite_ts = " + str(self.lastModTS) + " WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID)
                db.commit()
        # If statuses in reader do not exist, insert new status row
        else:
            cursor.execute("INSERT INTO books_settings(bookid, profileid, completed, completed_ts, favorite, favorite_ts) VALUES(" + self.reader_book_ID + ", " + profile_ID + ", " + str(completed) + ", " + str(self.lastModTS) + ", " + str(favorite) + ", " + str(self.lastModTS) + ")")
            db.commit()



    def send_rating_kr(self):
        rating = None
        if self.kr_metadata:
            rating = calibreAPI.field_for(prefs['rating_lookup_name'], self.calibre_book_ID, default_value=None)
            if rating:
                rating = rating/2
                if rating % 1 != 0:
                    rating = None
                else: 
                    rating = int(rating)
        if rating:
            if self.kr_metadata["summary"] == None:
                self.kr_metadata["summary"] = {}
            if "rating" not in self.kr_metadata["summary"] or self.kr_metadata["summary"]["rating"] != rating:
                self.kr_metadata["summary"]["rating"] = rating
                self.update_kr_sidecar()



    def send_review_kr(self):
        review = None
        if self.kr_metadata:
            review = calibreAPI.field_for(prefs["review_lookup_name"], self.calibre_book_ID, default_value=None)
        if review:
            review = review.replace("\n", "\\\n")
            if self.kr_metadata["summary"] == None:
                self.kr_metadata["summary"] = {}
            if "note" not in self.kr_metadata["summary"] or self.kr_metadata["summary"]["note"] != review:
                self.kr_metadata["summary"]["note"] = review
                self.update_kr_sidecar()






    def load_book_collections(self, db):
        # Create the list of collections that must be loaded for this book
        to_load_shelf = None

        # Sync must only work if custom column for collections is exist
        cursor = db.cursor()

        # Variable to show if collections must be updated
        update_shelfs_in_Calibre = False

        # Find the list of existing collections names for the book in Calibre
        calibreBookShelfNames = calibreAPI.field_for(prefs["shelf_lookup_name"], self.calibre_book_ID, default_value=[])

        # Create the editable copy of the collections in Calibre
        calibreBookShelfNamesList = list(calibreBookShelfNames)

        # Find the list of existing collections objects for the book on reader
        readerBookShelfs = cursor.execute("SELECT bookshelfid, is_deleted, ts FROM bookshelfs_books WHERE bookid = " + self.reader_book_ID).fetchall()


        # Check every collection in Calibre
        for calibreBookShelfName in calibreBookShelfNames:
            # For each collection in Calibre find corresponding collection in reader
            readerBookShelfID = shelf_dicts["shelfsNameToReader"][calibreBookShelfName]

            # If the collection in reader does not exist, delete collection in Calibre
            if readerBookShelfID == None:
                update_shelfs_in_Calibre = True
                calibreBookShelfNamesList.remove(calibreBookShelfName)
                
            # If the collection in reader does exist, we must check if it has the book
            else:
                # Find the collection in the list of reader collections for the book 
                readerBookShelf = None
                for readerShelfRow in readerBookShelfs:
                    if readerShelfRow["bookshelfid"] == readerBookShelfID:
                        readerBookShelf = readerShelfRow

                # If the book is in collection check if it is marked as deleted
                if readerBookShelf:
                    # If deleted, delete collection from Calibre collections list
                    if readerBookShelf["is_deleted"]:
                        update_shelfs_in_Calibre = True
                        calibreBookShelfNamesList.remove(calibreBookShelfName)

                # If the book is not in collection, delete collection from Calibre collections list
                else:
                    update_shelfs_in_Calibre = True
                    calibreBookShelfNamesList.remove(calibreBookShelfName)

        # Check every collection in reader
        for readerBookShelfRow in readerBookShelfs:

            # Get collection name and id in reader
            readerBookShelfID = readerBookShelfRow["bookshelfid"]
            shelfName = shelf_dicts["shelfsReaderToCalibre"][readerBookShelfID]["name"]

            # Look for the corresponding collection in Calibre
            calibreShelf = None
            for calibreBookShelfName in calibreBookShelfNames:
                if calibreBookShelfName == shelfName:
                    calibreShelf = calibreBookShelfName

            # If collection is not in Calibre and not marked as deleted, add collection to Calibre collections list
            if calibreShelf == None and readerBookShelfRow["is_deleted"] != 1:
                update_shelfs_in_Calibre = True
                calibreBookShelfNamesList.append(shelfName)
            # If collection is in Calibre, it is already processed, so do nothing.

        # If Calibre collections list is changed, we must save the object with changed list and book id for updating 
        if update_shelfs_in_Calibre:
            to_load_shelf = calibreBookShelfNamesList
        
        return to_load_shelf




    def load_shelf_kr(self):
        to_load_shelf = None
        
        if kr_collections:
            update_shelfs_in_Calibre = False
            kr_collections_list = []
            calibreBookShelfNames = calibreAPI.field_for(prefs["shelf_lookup_name"], self.calibre_book_ID, default_value=[])
            calibreBookShelfNamesList = list(calibreBookShelfNames)

            for c in kr_collections:
                collection = kr_collections[c]
                if c == "favorites":
                    pass
                else:
                    for key in collection:
                        if "file" in collection[key] and collection[key]["file"] == self.book_fullpath:
                            try:
                                kr_collections_list.append(c.decode("utf-8"))
                            except:
                                kr_collections_list.append(c)
                            
                            if c not in calibreBookShelfNamesList:
                                update_shelfs_in_Calibre = True
                            else:
                                calibreBookShelfNamesList.remove(c)
            if len(calibreBookShelfNamesList) > 0:
                update_shelfs_in_Calibre = True

            if update_shelfs_in_Calibre:
                to_load_shelf = kr_collections_list

        return to_load_shelf





    def load_read_status(self, db):
        to_load_read = None

        if sync_read:
            cursor = db.cursor()

            # Find read status in reader
            statusRow = cursor.execute("SELECT bookid, completed FROM books_settings WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID).fetchone()

            # If status exist, check the status in Calibre
            read = calibreAPI.field_for(prefs["read_lookup_name"], self.calibre_book_ID)
            completed = "0"
            if read: 
                completed = "1"

            if statusRow:
                # If status in Calibre is different from reader,  save status to update Calibre
                if completed != str(statusRow["completed"]):
                    completed = str(statusRow["completed"])

                    if str(completed) == "1":
                        read = True
                    else:
                        read = False

                    to_load_read = {"status": read}
                    
            # If status in reader not exist delete status in Calibre
            elif completed == "1":
                to_load_read = {"status": None}
            
        return to_load_read
    



    def load_read_kr(self):
        to_load_read = None
        if self.kr_metadata:
            status = False
            if "summary" in self.kr_metadata:
                if "status" in self.kr_metadata["summary"]:
                    if self.kr_metadata["summary"]["status"] == "complete":
                        status = True
                    else:
                        status = False
            read = calibreAPI.field_for(prefs["read_lookup_name"], self.calibre_book_ID)
            if read != status:
                to_load_read = {"status": status}
        return to_load_read










    
    def load_favorite_status(self, db):
        to_load_fav = None

        if sync_fav:
            cursor = db.cursor()

            # Find favorite status in reader
            statusRow = cursor.execute("SELECT bookid, favorite FROM books_settings WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID).fetchone()

            # If status exist, check the status in Calibre
            fav = calibreAPI.field_for(prefs["fav_lookup_name"], self.calibre_book_ID)
            favorite = "0"
            if fav: 
                favorite = "1"

            if statusRow:
                # If status in Calibre is different from reader,  save status to update Calibre
                if favorite != str(statusRow["favorite"]):
                    favorite = str(statusRow["favorite"])

                    if str(favorite) == "1":
                        fav = True
                    else:
                        fav = False
                    to_load_fav = {"status": fav}

            # If status in reader not exist delete status in Calibre
            elif favorite == "1":
                to_load_fav = {"status": None}
            
        return to_load_fav






    def load_fav_kr(self):
        to_load_fav = None
        if kr_collections: 
            status = False 
            if "favorites" in kr_collections:
                favorites = kr_collections["favorites"]
                for key in favorites:
                    if "file" in favorites[key] and favorites[key]["file"] == self.book_fullpath:
                        status = True
            fav = calibreAPI.field_for(prefs["fav_lookup_name"], self.calibre_book_ID)
            if fav != status:
                to_load_fav = {"status": status}
        return to_load_fav







    def load_statuses(self, db):
        cursor = db.cursor()
        to_load_statuses = {
            "read": None,
            "fav": None
        }

        # Find statuses in reader
        statusRow = cursor.execute("SELECT bookid, completed, favorite FROM books_settings WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID).fetchone()

        # If at least one status exist, check statuses in Calibre
        completed = "0"
        favorite = "0"

        if sync_read:
            read = calibreAPI.field_for(prefs["read_lookup_name"], self.calibre_book_ID)
            if read: 
                completed = "1"

        if sync_fav:
            fav = calibreAPI.field_for(prefs["fav_lookup_name"], self.calibre_book_ID)
            if fav: 
                favorite = "1"

        if statusRow:
            loadStatusFromDevice = False

            # If statuses in Calibre are different from reader,  save statuses to update Calibre
            if sync_read and completed != str(statusRow["completed"]):
                completed = str(statusRow["completed"])
                loadStatusFromDevice = True

            if sync_fav and favorite != str(statusRow["favorite"]):
                favorite = str(statusRow["favorite"])
                loadStatusFromDevice = True
            
            if loadStatusFromDevice:
                if str(completed) == "1":
                    read = True
                else:
                    read = False

                if str(favorite) == "1":
                    fav = True
                else:
                    fav = False

                to_load_statuses["read"] = {"status": read}
                to_load_statuses["fav"] = {"status": fav}
        
        # If statuses in reader not exist delete status in Calibre
        else:
            if completed == "1":
                to_load_statuses["read"] = {"status": None}
            if favorite == "1":
                to_load_statuses["fav"] = {"status": None}
        return to_load_statuses










    
    def load_rating_kr(self):
        to_load_rating = None
        if self.kr_metadata:
            if "summary" in self.kr_metadata:
                if "rating" in self.kr_metadata["summary"]:
                    rating = self.kr_metadata["summary"]["rating"] * 2
                    calibre_rating = calibreAPI.field_for(prefs['rating_lookup_name'], self.calibre_book_ID, default_value=None)
                    if rating != calibre_rating:
                        to_load_rating = rating
        return to_load_rating










    def load_review_kr(self):
        to_load_review = None
        if self.kr_metadata:
            if "summary" in self.kr_metadata:
                if "note" in self.kr_metadata["summary"]:
                    try:
                        review = self.kr_metadata["summary"]["note"].decode("utf-8").replace("\\\n", "\n")
                    except:
                        review = self.kr_metadata["summary"]["note"].replace("\\\n", "\n")
                    calibre_review = calibreAPI.field_for(prefs['review_lookup_name'], self.calibre_book_ID, default_value=None)
                    if review != calibre_review:
                        to_load_review = review
        return to_load_review







    def get_pb_annotations(self, db):
        cursor = db.cursor()


        books_metadata_query = (
            '''
            SELECT BookID FROM Files
            WHERE Name = ?
            AND PathID IN (SELECT OID FROM Paths WHERE Path = ?)
            '''
        )

        annotation_data_query = (
            '''
            SELECT i.OID AS item_oid, i.TimeAlt, t.TagID, t.Val FROM Items i
            LEFT JOIN Tags t ON i.OID=t.ItemID
            WHERE ParentID = ? AND State = 0
            ORDER BY i.OID, t.TagID
            '''
        )



        book = cursor.execute(books_metadata_query, (self.bookPath_file, self.bookPath_folder + "/")).fetchone()

        
        if book:
            book_oid = book["BookID"]
            an_rows = cursor.execute(annotation_data_query, (book_oid,)).fetchall()
            annotations = {}
    
            for row in an_rows:
                val = row["Val"]
                oid = row['item_oid']
                tagid = row['TagID']
                timestamp = row['TimeAlt']
                if oid not in annotations:
                    annotations[oid] = {}
                else:
                    if "timestamp" not in annotations[oid]:
                        annotations[oid]["timestamp"] = timestamp
                if tagid == 101:
                    anchor = json.loads(val).get('anchor', "")
                    page = int((re.findall(r'(?<=page=)\d*', anchor) or ["0"])[0])
                    offs = int((re.findall(r'(?<=offs=)\d*', anchor) or ["0"])[0])
                    annotations[oid]['title'] = "<span style='white-space: nowrap'>p. " + str(page) + "</span>"
                    annotations[oid]['location_sort'] = page * 10000 + offs

                if tagid == 102:
                    annotations[oid]["type"] = val
                if tagid == 104:
                    annotations[oid]["text"] = json.loads(val).get('text', None)
                if tagid == 105:
                    annotations[oid]["note"] = json.loads(val).get('text', None)
                if tagid == 106:
                    annotations[oid]["color"] = val

            for oid in annotations:
                annotation = annotations[oid]
                annotation["id"] = "pb_" + str(annotation["timestamp"])
                
                if annotation["text"]:
                    annotation["text"] = annotation["text"].replace(u"\u2029", "<br>")
                
                if annotation["type"] == "note" or annotation["type"] == "highlight":
                    self.new_annotations[annotation["id"]] = annotation

                    







    def get_kr_annotations(self):
        self.get_kr_metadata()

        if self.kr_metadata:
            annotations = self.kr_metadata["annotations"]
            pages = self.kr_metadata["doc_pages"]

            for key in annotations:
                if 'pos0' in annotations[key] or 'note' in annotations[key]:
                    annotation = {}

                    for a_key in ['text', 'note', 'color']:
                        if a_key in annotations[key]:
                            annotation[a_key] = annotations[key][a_key]
                    
                    if 'datetime' in annotations[key]:
                        datestring = annotations[key]["datetime"]
                        timestamp = get_int_timestamp(datetime.strptime(datestring,'%Y-%m-%d %H:%M:%S'))
                        annotation["timestamp"] = timestamp
                    
                    if 'pageno' in annotations[key]:
                        page = annotations[key]['pageno']
                    else:
                        page = 0
                    
                    if 'chapter' in annotations[key]:
                        chapter = annotations[key]['chapter']
                    else:
                        chapter = ""


                    start_pos_num = int(annotations[key]["page"].rsplit('.', 1)[1])

                    annotation["text"] = annotation["text"].replace("\\\n", "<br>")
                    annotation["title"] = chapter + " - <span style='white-space: nowrap'>p. " + str(page) + "</span>"
                    annotation["location_sort"] = int(page * 100000 / pages) * 10000 + start_pos_num
                    annotation["id"] = "kr_" + str(annotation["timestamp"])
                    
                    self.new_annotations[annotation["id"]] = annotation





    





    def get_cr_annotations(self):
        if os.path.exists(cr3hist_path):

            tree = ET.parse(cr3hist_path)
            root = tree.getroot()

            book_file = None

            for file in root:
                filename = file.find("file-info/doc-filename").text
                filepath = file.find("file-info/doc-filepath").text
                fullpath = filepath + filename

                if fullpath == self.book_fullpath:
                    book_file = file
                    break
            
            if book_file != None:
                bookmarks = book_file.findall("bookmark-list/bookmark[@type='comment']")

                for bookmark in bookmarks:
                    annotation = {}

                    text = bookmark.find("selection-text").text
                    annotation['text'] = text.replace("\n", "<br>")

                    chapter = bookmark.find("header-text").text
                    percent = bookmark.get('percent')
                    start_point_num = bookmark.find("start-point").text.rsplit('.', 1)[1]

                    if chapter:
                        annotation["title"] = chapter + " - <span style='white-space: nowrap'>" + percent + "</span>"
                    else:
                        annotation["title"] = "<span style='white-space: nowrap'>" + percent + "</span>"

                    lastpos = book_file.find("bookmark-list/bookmark[@type='lastpos']")
                    if lastpos != None:
                        annotation["timestamp"] = int(lastpos.get('timestamp'))
                    else:
                        annotation["timestamp"] = int(time.time())

                    annotation["location_sort"] = int(percent.replace(".", "").replace("%", "")) * 100000 + int(start_point_num)
                    annotation["color"] = "yellow"
                    annotation["id"] = "cr_" + percent.replace(".", "").replace("%", "") + start_point_num
                    
                    self.new_annotations[annotation["id"]] = annotation


    def generate_annotations_html(self):
        self.get_existing_annotations()

        pb_annotations = []
        kr_annotations = []
        cr_annotations = []

        for id in self.existing_annotations:
            if id not in self.new_annotations:
                an_obj = self.existing_annotations[id]

                app = id[0:2]

                if app == "pb":
                    pb_annotations.append(an_obj)

                if app == "kr":
                    kr_annotations.append(an_obj)

                if app == "cr":
                    cr_annotations.append(an_obj)





        for id in self.new_annotations:
            annotation = self.new_annotations[id]
            datestring = datetime.fromtimestamp(annotation["timestamp"]).strftime('%c').rsplit(":", 1)[0]
            an_el = ''
            an_el += '<div id="' + id + '" data-sort="' + str(annotation["location_sort"]) + '">\n'
            an_el += '<table width="100%" bgcolor="#f4e681"><tbody>\n'
            an_el += '<tr>\n'
            an_el += '<td><p><strong>' + annotation["title"] + '</strong></p></td>\n'
            an_el += '<td align="right"><p><strong>' + datestring + '</strong></p></td>\n'
            an_el += '</tr>\n'
            an_el += '</tbody></table>\n'
            an_el += '<p style="margin-left: 15px">' + annotation["text"] + '</p>\n' 
            if "note" in annotation:
                an_el += '<p><i>' + annotation["note"] + '</i></p>\n'
            an_el += '<hr width="80%" style="background-color:#777;"></div>\n'

            an_obj = {
                "location_sort": annotation["location_sort"],
                "html": an_el
            }

            app = id[0:2]


            if app == "pb":
                pb_annotations.append(an_obj)

            if app == "kr":
                kr_annotations.append(an_obj)

            if app == "cr":
                cr_annotations.append(an_obj)

        pb_annotations.sort(key=operator.itemgetter('location_sort'))
        kr_annotations.sort(key=operator.itemgetter('location_sort'))
        cr_annotations.sort(key=operator.itemgetter('location_sort'))

        self.annotations_html = "<div>\n"

        for an_obj in pb_annotations:
            self.annotations_html += an_obj["html"]

        for an_obj in kr_annotations:
            self.annotations_html += an_obj["html"]

        for an_obj in cr_annotations:
            self.annotations_html += an_obj["html"]
        
        self.annotations_html += "</div>"


        

















    def get_existing_annotations(self):
        self.existing_annotations = {}
        raw_annotations = calibreAPI.field_for(prefs["an_lookup_name"], self.calibre_book_ID)

        if (raw_annotations):
            raw_annotations = raw_annotations.replace("<br>", "<br></br>").replace('<hr width="80%" style="background-color:#777;">', '<hr width="80%" style="background-color:#777;"></hr>')
            root = ET.fromstring(raw_annotations)

            for an in root:
                id = an.get('id')
                location_sort = int(an.get('data-sort'))
   
                try:
                    an_str = ET.tostring(an, encoding='utf-8').replace("</br>", "").replace("</hr>", "")
                except:
                    an_str = ET.tostring(an, encoding='unicode').replace("</br>", "").replace("</hr>", "")

                an_obj = {
                    "location_sort": location_sort,
                    "html": an_str
                }
                self.existing_annotations[id] = an_obj










    def pb_sync_position(self, db):

        calibre_position_string = calibreAPI.field_for(prefs["position_lookup_name"], self.calibre_book_ID)
        calibre_position_pb = None

        if calibre_position_string != None:
            calibre_position_dir = eval(calibre_position_string)
            if "pb" in calibre_position_dir:
                calibre_position_pb = calibre_position_dir["pb"]
                calibre_position = calibre_position_pb.split("_TIMESTAMP_")[0]
                calibre_ts = calibre_position_pb.split("_TIMESTAMP_")[1]
                calibre_ts_int = int(calibre_ts)

        cursor = db.cursor()
        reader_position_row = cursor.execute("SELECT position, position_ts FROM books_settings WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID).fetchone()
        reader_position_string = None


        if reader_position_row and reader_position_row["position"]:
            reader_ts_int = reader_position_row["position_ts"]
            reader_ts = str(reader_ts_int)
            reader_position_string = reader_position_row["position"] + "_TIMESTAMP_" + reader_ts
            
            
        if reader_position_row == None and calibre_position_pb:
            cursor.execute("INSERT INTO books_settings(bookid, profileid, position, position_ts) VALUES(" + self.reader_book_ID + ", " + profile_ID + ", '" + calibre_position + "', " + calibre_ts + ")")
            db.commit()
            return None

        elif reader_position_row and reader_position_row["position"] and calibre_position_pb == None:
            return reader_position_string

        elif reader_position_row and calibre_position_pb and calibre_position_pb != reader_position_string:

            if reader_position_row["position"] == None or calibre_ts_int > reader_ts_int:
                cursor.execute("UPDATE books_settings SET position = '" + calibre_position + "', position_ts = " + calibre_ts + " WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID)
                db.commit()
                return None
            
            elif calibre_ts_int < reader_ts_int:
                return reader_position_string
        
        else:
            return None



    def kr_sync_position(self):

        calibre_position_string = calibreAPI.field_for(prefs["position_lookup_name"], self.calibre_book_ID)
        calibre_position_kr = None
        position = None

        if calibre_position_string != None:
            calibre_position_dir = eval(calibre_position_string)
            if "kr" in calibre_position_dir:
                calibre_position_kr = calibre_position_dir["kr"]
                calibre_position_text = calibre_position_kr.split("_TIMESTAMP_")[0]
                calibre_ts = int(calibre_position_kr.split("_TIMESTAMP_")[1])

        if self.kr_metadata:
            position = self.kr_metadata["last_xpointer"]
            reader_ts = int(os.path.getmtime(self.sidecar_path))
            reader_position_string = position + "_TIMESTAMP_" + str(reader_ts)

        if position and calibre_position_kr == None:
            return reader_position_string
        
        elif not os.path.exists(self.sidecar_path) and calibre_position_kr:
            self.generate_kr_sidecar(calibre_position_text)
        
        elif self.kr_metadata and position == None and calibre_position_kr:
            self.kr_metadata["last_xpointer"] = calibre_position_text
            self.update_kr_sidecar()

        elif position and calibre_position_kr and calibre_position_kr != reader_position_string:
            if calibre_ts > reader_ts:
                self.kr_metadata["last_xpointer"] = calibre_position_text
                self.update_kr_sidecar()
            elif calibre_ts < reader_ts:
                return reader_position_string
            
        return None
            


    def cr_sync_position(self):

        if not os.path.exists(cr3hist_path):
            cr3hist_dir = os.path.split(cr3hist_path)[0]
            make_dir(cr3hist_dir)
            root = ET.Element("FictionBookMarks")
            tree = ET.ElementTree(root)
            tree.write(cr3hist_path)

        # Get position from Calibre

        calibre_position_string = calibreAPI.field_for(prefs["position_lookup_name"], self.calibre_book_ID)
        calibre_position_cr = None

        if calibre_position_string != None:
            calibre_position_dir = eval(calibre_position_string)
            if "cr" in calibre_position_dir:
                calibre_position_cr = calibre_position_dir["cr"]


        # Get position string from reader

        bookPath_folder = self.bookPath_folder.replace("''", "'") + "/"
        bookPath_file = self.bookPath_file.replace("''", "'")
        book_file = None
        position_bookmark = None
        tree = ET.parse(cr3hist_path)
        root = tree.getroot()

        
        for file in root:
            filename = file.find("file-info/doc-filename").text
            filepath = file.find("file-info/doc-filepath").text
            if filename == bookPath_file and filepath == bookPath_folder:
                book_file = file
                break

        if book_file != None:
            position_bookmark = book_file.find("bookmark-list/bookmark[@type='lastpos']")

        # Compare positions


        if position_bookmark != None:
            start_point = position_bookmark.find("start-point")
            position = start_point.text     
            reader_ts = position_bookmark.get('timestamp')
            reader_position_string = position + "_TIMESTAMP_" + reader_ts

        if position_bookmark != None and calibre_position_cr == None:
            # Load position to calibre
            return reader_position_string


        elif position_bookmark == None and calibre_position_cr != None:
            # Send position to reader (add new file element to xml)

            calibre_ts = int(calibre_position_cr.split("_TIMESTAMP_")[1])
            calibre_position_text = calibre_position_cr.split("_TIMESTAMP_")[0]
            
            file = ET.SubElement(root, "file")
            fileinfo = ET.SubElement(file, "file-info")
            filename = ET.SubElement(fileinfo, "doc-filename")
            filename.text = bookPath_file
            filepath = ET.SubElement(fileinfo, "doc-filepath")
            filepath.text = bookPath_folder

            filesize = ET.SubElement(fileinfo, "doc-filesize")
            filesize.text = str(self.book_filesize)
            bookmark_list = ET.SubElement(file, "bookmark-list")
            bookmark = ET.SubElement(bookmark_list, "bookmark")
            bookmark.set("type", "lastpos")
            bookmark.set("timestamp", str(calibre_ts))
            start_point = ET.SubElement(bookmark, "start-point")
            start_point.text = calibre_position_text
            tree.write(cr3hist_path)

            return None

        elif position_bookmark != None and calibre_position_cr != None and calibre_position_cr != reader_position_string:
            # Compare position timestamps

            reader_ts = int(position_bookmark.get('timestamp'))
            calibre_ts = int(calibre_position_cr.split("_TIMESTAMP_")[1])

            if calibre_ts > reader_ts:
                # Send position to reader (edit element in xml)
                calibre_position_text = calibre_position_cr.split("_TIMESTAMP_")[0]
                start_point.text = calibre_position_text
                position_bookmark.set('timestamp', str(calibre_ts))
                tree.write(cr3hist_path)

                return None
                

            elif calibre_ts < reader_ts:
                # Load position to calibre
                return reader_position_string

        return None








    def pb_force_position(self, db):
        calibre_position_string = calibreAPI.field_for(prefs["position_lookup_name"], self.calibre_book_ID)
        calibre_position_pb = None

        if calibre_position_string != None:
            calibre_position_dir = eval(calibre_position_string)
            if "pb" in calibre_position_dir:
                calibre_position_pb = calibre_position_dir["pb"]
                calibre_position = calibre_position_pb.split("_TIMESTAMP_")[0]
                calibre_ts = calibre_position_pb.split("_TIMESTAMP_")[1]

        cursor = db.cursor()
        reader_position_row = cursor.execute("SELECT position, position_ts FROM books_settings WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID).fetchone()
        

        if reader_position_row == None and calibre_position_pb:
            cursor.execute("INSERT INTO books_settings(bookid, profileid, position, position_ts) VALUES(" + self.reader_book_ID + ", " + profile_ID + ", '" + calibre_position + "', " + calibre_ts + ")")
            db.commit()

        elif reader_position_row and calibre_position_pb and calibre_position != reader_position_row["position"]:
            cursor.execute("UPDATE books_settings SET position = '" + calibre_position + "', position_ts = " + calibre_ts + " WHERE bookid = " + self.reader_book_ID + " AND profileid = " + profile_ID)
            db.commit()
            
        return None



    def kr_force_position(self):

        calibre_position_string = calibreAPI.field_for(prefs["position_lookup_name"], self.calibre_book_ID)
        calibre_position_kr = None
        position = None

        if calibre_position_string != None:
            calibre_position_dir = eval(calibre_position_string)
            if "kr" in calibre_position_dir:
                calibre_position_kr = calibre_position_dir["kr"]
                calibre_position_text = calibre_position_kr.split("_TIMESTAMP_")[0]

        if self.kr_metadata:
            position = self.kr_metadata["last_xpointer"]
        
            if calibre_position_kr and position != calibre_position_text:
                self.kr_metadata["last_xpointer"] = calibre_position_text
                self.update_kr_sidecar()

        elif calibre_position_kr:
            self.generate_kr_sidecar(calibre_position_text)

        return None
            




    def cr_force_position(self):

        if not os.path.exists(cr3hist_path):
            cr3hist_dir = os.path.split(cr3hist_path)[0]
            make_dir(cr3hist_dir)
            root = ET.Element("FictionBookMarks")
            tree = ET.ElementTree(root)
            tree.write(cr3hist_path)

        # Get position from Calibre

        calibre_position_string = calibreAPI.field_for(prefs["position_lookup_name"], self.calibre_book_ID)
        calibre_position_cr = None

        if calibre_position_string != None:
            calibre_position_dir = eval(calibre_position_string)
            if "cr" in calibre_position_dir:
                calibre_position_cr = calibre_position_dir["cr"]


        # Get position string from reader

        bookPath_folder = self.bookPath_folder.replace("''", "'") + "/"
        bookPath_file = self.bookPath_file.replace("''", "'")
        book_file = None
        position_bookmark = None
        tree = ET.parse(cr3hist_path)
        root = tree.getroot()

        
        for file in root:
            filename = file.find("file-info/doc-filename").text
            filepath = file.find("file-info/doc-filepath").text
            if filename == bookPath_file and filepath == bookPath_folder:
                book_file = file
                break

        if book_file != None:
            position_bookmark = book_file.find("bookmark-list/bookmark[@type='lastpos']")

 
        if position_bookmark != None:
            start_point = position_bookmark.find("start-point")
            position = start_point.text     
            reader_ts = position_bookmark.get('timestamp')
            reader_position_string = position + "_TIMESTAMP_" + reader_ts


        if position_bookmark == None and calibre_position_cr != None:

            calibre_ts = int(calibre_position_cr.split("_TIMESTAMP_")[1])
            calibre_position_text = calibre_position_cr.split("_TIMESTAMP_")[0]
            
            file = ET.SubElement(root, "file")
            fileinfo = ET.SubElement(file, "file-info")
            filename = ET.SubElement(fileinfo, "doc-filename")
            filename.text = bookPath_file
            filepath = ET.SubElement(fileinfo, "doc-filepath")
            filepath.text = bookPath_folder

            filesize = ET.SubElement(fileinfo, "doc-filesize")
            filesize.text = str(self.book_filesize)
            bookmark_list = ET.SubElement(file, "bookmark-list")
            bookmark = ET.SubElement(bookmark_list, "bookmark")
            bookmark.set("type", "lastpos")
            bookmark.set("timestamp", str(calibre_ts))
            start_point = ET.SubElement(bookmark, "start-point")
            start_point.text = calibre_position_text
            tree.write(cr3hist_path)

        elif position_bookmark != None and calibre_position_cr != None and calibre_position_cr != reader_position_string:
            calibre_ts = int(calibre_position_cr.split("_TIMESTAMP_")[1])
            calibre_position_text = calibre_position_cr.split("_TIMESTAMP_")[0]
            start_point.text = calibre_position_text
            position_bookmark.set('timestamp', str(calibre_ts))
            tree.write(cr3hist_path)

        return None


