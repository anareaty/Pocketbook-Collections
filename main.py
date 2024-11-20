
import json
import sqlite3 as sqlite
from contextlib import closing
from calibre.library import db as calibre_db
from calibre_plugins.pocketbook_collections.config import prefs



# The main function to start syncing any metadata. It is run as a job.

def sync_metadata(data, command, done_msg):
    print("PB-COLLECTIONS: Start syncing metadata")

    # Create the dict to return after syncing
    to_load = {
        "read": [],
        "fav": [],
        "shelf": []
    }

    
    # Check if columns are set up and exist

    current_db = calibre_db(data["dbpath"])
    calibreAPI = current_db.new_api



    # Check if device is connected
    device_DB_path = data["device_DB_path"]

    if device_DB_path == False:
        return to_load, "No Pocketbook device connected or current device is not supported"
    
    else:

        # Connect to the Pocketbook database

        with closing(sqlite.connect(device_DB_path)) as db:
            db.row_factory = lambda cursor, row: {col[0]: row[i] for i, col in enumerate(cursor.description)}
            cursor = db.cursor()


            # Find prexixes of the reader storages

            storage_id_main = cursor.execute("SELECT id FROM storages WHERE name = 'InternalStorage'").fetchone()["id"]
            storage_prefix_main_split = cursor.execute("SELECT name FROM folders WHERE storageid = " + str(storage_id_main)).fetchone()["name"].split("/")
            storage_prefix_main = "/" + storage_prefix_main_split[1] + "/" + storage_prefix_main_split[2]

            storage_id_card = cursor.execute("SELECT id FROM storages WHERE name LIKE 'SDCard%'").fetchone()["id"]
            storage_prefix_card_split = cursor.execute("SELECT name FROM folders WHERE storageid = " + str(storage_id_card)).fetchone()["name"].split("/")
            storage_prefix_card = "/" + storage_prefix_card_split[1] + "/" + storage_prefix_card_split[2]



            # Find calibre metadata in device storages
              
            device_main_storage = data["device_storages"]["main"]
            device_card = data["device_storages"]["card"]

            device_metadata_path_main = device_main_storage + "metadata.calibre"
            device_metadata_path_card = device_card + "metadata.calibre"

            with open(device_metadata_path_main, "r") as file:
                data_main = json.load(file)

            with open(device_metadata_path_card, "r") as file:
                data_card = json.load(file)



            # Get IDs of all books in Calibre
                
            calibre_book_IDs = calibreAPI.all_book_ids()

            # Try to check and sync every book

            for calibre_book_ID in calibre_book_IDs:

                # Find the book in Calibre metadata objects on device. We must check both the main storage and the card, it it is exist.

                bookData = next((bookData for bookData in data_main if bookData["application_id"] == calibre_book_ID), None)
                storage_prefix = storage_prefix_main                

                if bookData == None:
                    bookData = next((bookData for bookData in data_card if bookData["application_id"] == calibre_book_ID), None)
                    storage_prefix = storage_prefix_card
                
                
                # If bookData is found, it means it is on device and indexed by Calibre. Proceed to check if it is indexed by Pocketbook.
                    
                book_row = None
                
                if bookData:

                    # Find the path to the book on the device

                    bookPath = bookData["lpath"]

                    # Get the filename an folder of the book

                    bookPath_split = bookPath.split("/")
                    bookPath_file = bookPath_split[-1].replace("'", r"''")
                    bookPath_split.pop()
                    bookPath_folder = storage_prefix + "/" + "/".join(bookPath_split)
                    bookPath_folder = bookPath_folder.replace("'", r"''")


                    # Find the folder in Pocketbook database

                    book_folder_row = cursor.execute("SELECT id FROM folders WHERE name = '" + bookPath_folder + "'").fetchone()

                    if book_folder_row:
                        book_folder_id = str(book_folder_row["id"])

                        # Find the book in Pocketbook database

                        book_row = cursor.execute("SELECT book_id as id, filename FROM files WHERE folder_id = " + book_folder_id + " AND filename = '" + bookPath_file + "'").fetchone()
                    

                # If the book exist and indexed by Pocketbook proceed to sync metadata between the reader and Calibre

                if book_row:

                    reader_book_ID = str(book_row["id"])

                    # Find the timestamp of the last modification in Calibre
                    lastMod = calibreAPI.field_for("last_modified", calibre_book_ID)
                    lastModTS = int(lastMod.timestamp())
                    
                    
                    # Run functions to send or load selected metadata based on specified command
                    
                    if command == "send all":
                        send_statuses(data, db, calibreAPI, calibre_book_ID, reader_book_ID, lastModTS)
                        send_collections(data, db, calibreAPI, calibre_book_ID, reader_book_ID, lastModTS)

                    if command == "send collections":
                        send_collections(data, db, calibreAPI, calibre_book_ID, reader_book_ID, lastModTS)

                    elif command == "send read":
                        send_read_status(data, db, calibreAPI, calibre_book_ID, reader_book_ID, lastModTS)

                    elif command == "send favorite":
                        send_favorite_status(data, db, calibreAPI, calibre_book_ID, reader_book_ID, lastModTS)

                    if command == "load all":
                        to_load_statuses = load_statuses(data, db, calibreAPI, calibre_book_ID, reader_book_ID)
                        if to_load_statuses["read"]:
                            to_load["read"].append(to_load_statuses["read"])
                        if to_load_statuses["fav"]:
                            to_load["fav"].append(to_load_statuses["fav"])
                        
                        to_load_shelf = load_collections(data, db, calibreAPI, calibre_book_ID, reader_book_ID)
                        if to_load_shelf:
                            to_load["shelf"].append(to_load_shelf)

                    elif command == "load collections":
                        to_load_shelf = load_collections(data, db, calibreAPI, calibre_book_ID, reader_book_ID)
                        if to_load_shelf:
                            to_load["shelf"].append(to_load_shelf)

                    elif command == "load read":
                        to_load_read = load_read_status(data, db, calibreAPI, calibre_book_ID, reader_book_ID)
                        if to_load_read:
                            to_load["read"].append(to_load_read)

                    elif command == "load favorite":
                        to_load_fav = load_favorite_status(data, db, calibreAPI, calibre_book_ID, reader_book_ID)
                        if to_load_fav:
                            to_load["fav"].append(to_load_fav)

                    
        print("PB-COLLECTIONS: End syncing metadata")

    # We must return the dict containing all the loaded data that should be changed in Calibre
    return to_load, done_msg




# Function to send collections from Calibre to the reader

def send_collections(data, db, calibreAPI, calibre_book_ID, reader_book_ID, lastModTS):
    # Sync must only work if custom column for collections is exist

    if data["has_shelf_column"]:
        cursor = db.cursor()

        # For it to be easier to compare collections between the reader and Calibre we must create the dict of relations between the collections names and their IDs in both locations. 
        shelf_dicts = create_shelfs_dicts(data, db, calibreAPI)

        # Find the list of existing collections names for the book in Calibre
        calibreBookShelfNames = calibreAPI.field_for(prefs["shelf_lookup_name"], calibre_book_ID, default_value=[])
    
        # Find the list of existing collections objects for the book on reader
        readerBookShelfs = cursor.execute("SELECT bookshelfid, is_deleted, ts FROM bookshelfs_books WHERE bookid = " + reader_book_ID).fetchall()


        # Check every collection in Calibre

        for calibreBookShelfName in calibreBookShelfNames:

            # For each collection in Calibre find corresponding collection on reader

            readerBookShelfID = shelf_dicts["shelfsNameToReader"][calibreBookShelfName]


            # If the collection in reader does not exist we must create it
            if readerBookShelfID == None:
                # First create the collection itself
                cursor.execute("INSERT INTO bookshelfs(name, is_deleted, ts) VALUES('" + calibreBookShelfName + "', 0, " + str(lastModTS) + ")")
                db.commit()
                readerBookShelfID = cursor.execute("SELECT id FROM bookshelfs WHERE name = '" + calibreBookShelfName +"'").fetchone()["id"]

                # The add book to collection
                cursor.execute("INSERT INTO bookshelfs_books(bookshelfid, bookid, ts, is_deleted) VALUES(" + str(readerBookShelfID) + ", " + reader_book_ID + ", " + str(lastModTS) + ", 0)")
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
                        cursor.execute("UPDATE bookshelfs_books SET is_deleted = 0, ts = " + str(lastModTS) + " WHERE bookid = " + reader_book_ID + " AND bookshelfid = " + str(readerBookShelfID))
                        db.commit()

                # If the book is not in collection, add it to collection
                else:
                    cursor.execute("INSERT INTO bookshelfs_books(bookshelfid, bookid, ts, is_deleted) VALUES(" + str(readerBookShelfID) + ", " + reader_book_ID + ", " + str(lastModTS) + ", 0)")
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
                cursor.execute("UPDATE bookshelfs_books SET is_deleted = 1, ts = " + str(lastModTS) + " WHERE bookid = " + reader_book_ID + " AND bookshelfid = " + str(readerBookShelfID))
                db.commit()

            # If collection is in Calibre, it is already processed, so do nothing.




# Function to send read statuses from Calibre to the reader

def send_read_status(data, db, calibreAPI, calibre_book_ID, reader_book_ID, lastModTS):

    if data["has_read_column"]:
        cursor = db.cursor()

        # Find read status in Calibre

        read = calibreAPI.field_for(prefs["read_lookup_name"], calibre_book_ID)
        completed = "0"
        if read: 
            completed = "1"

        # Find read status in reader

        statusRow = cursor.execute("SELECT bookid, completed FROM books_settings WHERE bookid = " + reader_book_ID).fetchone()

        # If status in reader exist and different from Calibre, update status in reader
        if statusRow:
            if completed != str(statusRow["completed"]):
                cursor.execute("UPDATE books_settings SET completed = " + str(completed) + ", completed_ts = " + str(lastModTS) + " WHERE bookid = " + reader_book_ID)
                db.commit()
        # If status in reader do not exist, insert new status row
        else:
            cursor.execute("INSERT INTO books_settings(bookid, profileid, completed, completed_ts) VALUES(" + reader_book_ID + ", 1, " + str(completed) + ", " + str(lastModTS) + ")")
            db.commit()




# Function to send favorite statuses from Calibre to the reader

def send_favorite_status(data, db, calibreAPI, calibre_book_ID, reader_book_ID, lastModTS):

    if data["has_fav_column"]:
        cursor = db.cursor()

        # Find favorite status in Calibre

        fav = calibreAPI.field_for(prefs["fav_lookup_name"], calibre_book_ID)
        favorite = "0"
        if fav: 
            favorite = "1"

        # Find favorite status in reader

        statusRow = cursor.execute("SELECT bookid, favorite FROM books_settings WHERE bookid = " + reader_book_ID).fetchone()

        # If status in reader exist and different from Calibre, update status in reader
        if statusRow:
            if favorite != str(statusRow["favorite"]):
                cursor.execute("UPDATE books_settings SET favorite = " + str(favorite) + ", favorite_ts = " + str(lastModTS) + " WHERE bookid = " + reader_book_ID)
                db.commit()
        # If status in reader do not exist, insert new status row
        else:
            cursor.execute("INSERT INTO books_settings(bookid, profileid, favorite, favorite_ts) VALUES(" + reader_book_ID + ", 1, " + str(favorite) + ", " + str(lastModTS) + ")")
            db.commit()




# Function to send both read and favorite statuses from Calibre to the reader

def send_statuses(data, db, calibreAPI, calibre_book_ID, reader_book_ID, lastModTS):
    
    if data["has_read_column"] or data["has_fav_column"]:
        cursor = db.cursor()

        # Find statuses in Calibre
        completed = "0"
        favorite = "0"

        if data["has_read_column"]:
            read = calibreAPI.field_for(prefs["read_lookup_name"], calibre_book_ID)
            if read: 
                completed = "1"

        if data["has_fav_column"]:
            fav = calibreAPI.field_for(prefs["fav_lookup_name"], calibre_book_ID)
            if fav: 
                favorite = "1"
        
        # Find statuses in reader

        statusRow = cursor.execute("SELECT bookid, completed, favorite FROM books_settings WHERE bookid = " + reader_book_ID).fetchone()

        # If any status in reader exist and different from Calibre, update statuses in reader
        if statusRow:
            if (data["has_read_column"] and completed != str(statusRow["completed"])) or (data["has_fav_column"] and favorite != str(statusRow["favorite"])):
                cursor.execute("UPDATE books_settings SET profileid = 1, completed = " + str(completed) + ", completed_ts = " + str(lastModTS) + ", favorite = " + str(favorite) + ", favorite_ts = " + str(lastModTS) + " WHERE bookid = " + reader_book_ID)
                db.commit()
        # If statuses in reader do not exist, insert new status row
        else:
            cursor.execute("INSERT INTO books_settings(bookid, profileid, completed, completed_ts, favorite, favorite_ts) VALUES(" + reader_book_ID + ", 1, " + str(completed) + ", " + str(lastModTS) + ", " + str(favorite) + ", " + str(lastModTS) + ")")
            db.commit()




# Function to load collections from reader to Calibre
# We don't update Calibre metadata directly from this module, because it would not refresh the GUI. 
# Instead we collect all the data that need to be updated and return it to later update in sync_done function in ui module.

def load_collections(data, db, calibreAPI, calibre_book_ID, reader_book_ID):

    # Create the list of collections that must be loaded for this book
    to_load_shelf = None

    # Sync must only work if custom column for collections is exist

    if data["has_shelf_column"]:
        cursor = db.cursor()

        # For it to be easier to compare collections between the reader and Calibre we must create the dict of relations between the collections names and their IDs in both locations.
        shelf_dicts = create_shelfs_dicts(data, db, calibreAPI)

        # Variable to show if collections must be updated
        update_shelfs_in_Calibre = False

        # Find the list of existing collections names for the book in Calibre
        calibreBookShelfNames = calibreAPI.field_for(prefs["shelf_lookup_name"], calibre_book_ID, default_value=[])

        # Create the editable copy of the collections in Calibre
        calibreBookShelfNamesList = list(calibreBookShelfNames)

        # Find the list of existing collections objects for the book on reader
        readerBookShelfs = cursor.execute("SELECT bookshelfid, is_deleted, ts FROM bookshelfs_books WHERE bookid = " + reader_book_ID).fetchall()


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
            to_load_shelf = {calibre_book_ID: calibreBookShelfNamesList}
    
    return to_load_shelf



# Function to load read statuses from reader to Calibre
# We don't update Calibre metadata directly from this module, because it would not refresh the GUI. 
# Instead we collect all the data that need to be updated and return it to later update in sync_done function in ui module.

def load_read_status(data, db, calibreAPI, calibre_book_ID, reader_book_ID):
    
    to_load_read = None

    if data["has_read_column"]:
        cursor = db.cursor()

        # Find read status in reader

        statusRow = cursor.execute("SELECT bookid, completed FROM books_settings WHERE bookid = " + reader_book_ID).fetchone()

        # If status exist, check the status in Calibre

        if statusRow:
            read = calibreAPI.field_for(prefs["read_lookup_name"], calibre_book_ID)
            completed = "0"
            if read: 
                completed = "1"

            # If status in Calibre is different from reader,  save status to update Calibre
            if completed != str(statusRow["completed"]):
                completed = str(statusRow["completed"])

                if str(completed) == "1":
                    read = True
                else:
                    read = False

                to_load_read = {calibre_book_ID: read}
                
        # If status in reader not exist delete status in Calibre
        else:
            to_load_read = {calibre_book_ID: None}
        
    return to_load_read
       


    
# Function to load favorite statuses from reader to Calibre
# We don't update Calibre metadata directly from this module, because it would not refresh the GUI. 
# Instead we collect all the data that need to be updated and return it to later update in sync_done function in ui module.

def load_favorite_status(data, db, calibreAPI, calibre_book_ID, reader_book_ID):
    
    to_load_fav = None

    if data["has_fav_column"]:
        cursor = db.cursor()

        # Find favorite status in reader
        
        statusRow = cursor.execute("SELECT bookid, favorite FROM books_settings WHERE bookid = " + reader_book_ID).fetchone()

        # If status exist, check the status in Calibre

        if statusRow:
            fav = calibreAPI.field_for(prefs["fav_lookup_name"], calibre_book_ID)
            favorite = "0"
            if fav: 
                favorite = "1"

            # If status in Calibre is different from reader,  save status to update Calibre
            if favorite != str(statusRow["favorite"]):
                favorite = str(statusRow["favorite"])

                if str(favorite) == "1":
                    fav = True
                else:
                    fav = False

                to_load_fav = {calibre_book_ID: fav}

        # If status in reader not exist delete status in Calibre
        else:
            to_load_fav = {calibre_book_ID: None}

    return to_load_fav




# Function to load both statuses from reader to Calibre
# We don't update Calibre metadata directly from this module, because it would not refresh the GUI. 
# Instead we collect all the data that need to be updated and return it to later update in sync_done function in ui module.
    
def load_statuses(data, db, calibreAPI, calibre_book_ID, reader_book_ID):
    
    cursor = db.cursor()
    to_load_statuses = {
        "read": None,
        "fav": None
    }

    if data["has_read_column"] or data["has_fav_column"]:

        # Find statuses in reader

        statusRow = cursor.execute("SELECT bookid, completed, favorite FROM books_settings WHERE bookid = " + reader_book_ID).fetchone()

        # If at least one status exist, check statuses in Calibre

        if statusRow:
            completed = "0"
            favorite = "0"

            if data["has_read_column"]:
                read = calibreAPI.field_for(prefs["read_lookup_name"], calibre_book_ID)
                if read: 
                    completed = "1"

            if data["has_fav_column"]:
                fav = calibreAPI.field_for(prefs["fav_lookup_name"], calibre_book_ID)
                if fav: 
                    favorite = "1"

            loadStatusFromDevice = False

            # If statuses in Calibre are different from reader,  save statuses to update Calibre
            
            if data["has_read_column"] and completed != str(statusRow["completed"]):
                completed = str(statusRow["completed"])
                loadStatusFromDevice = True

            if data["has_fav_column"] and favorite != str(statusRow["favorite"]):
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

                to_load_statuses["read"] = {calibre_book_ID: read}
                to_load_statuses["fav"] = {calibre_book_ID: fav}
        
        # If statuses in reader not exist delete status in Calibre
        else:
            to_load_statuses["read"] = {calibre_book_ID: None}
            to_load_statuses["fav"] = {calibre_book_ID: None}


    return to_load_statuses


def create_shelfs_dicts(data, db, calibreAPI):
    cursor = db.cursor()

    shelf_dicts = {
        "shelfsReaderToCalibre": {},
        "shelfsNameToReader": {}
    }

    if data["has_shelf_column"]:
        calibreShelfIDs = calibreAPI.all_field_ids(prefs["shelf_lookup_name"])
        if calibreShelfIDs == None:
            calibreShelfIDs = []
    else:
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

    return shelf_dicts



    
