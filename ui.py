__license__   = 'GPL v3'
__copyright__ = '2024, Anareaty <reatymain@gmail.com>'
__docformat__ = 'restructuredtext en'


from calibre.gui2.actions import InterfaceAction
from calibre_plugins.pocketbook_collections.__init__ import PLUGIN_NAME
from calibre_plugins.pocketbook_collections.config import prefs
from calibre.devices.usbms.driver import debug_print
from calibre.gui2 import error_dialog
import os

try:
    from qt.core import QMenu
except:
    try:
        from PyQt5.Qt import QMenu
    except:
        from PyQt4.Qt import QMenu



PLUGIN_ICONS = ['images/pocketbook.png', 
                'images/forward.png', 
                'images/back.png', 
                'images/config.png', 
                'images/catalog.png', 
                'images/ok.png', 
                'images/heart.png',
                'images/auto-reload.png',
                'images/rating.png',
                'images/edit_input.png',
                'images/edit-redo.png',
                'images/highlight.png'
                ]




class InterfacePlugin(InterfaceAction):
    name = PLUGIN_NAME
    action_spec = (PLUGIN_NAME, None, PLUGIN_NAME, None)
    action_type = 'current'
    dont_add_to = frozenset(['toolbar'])


    def genesis(self):
        main_menu = QMenu(self.gui)
        self.menu = main_menu
        self.qaction.setMenu(main_menu)


    def initialization_complete(self):
        self.rebuild_menus()

        
    # Set menus
        
    def rebuild_menus(self):
        fm = self.gui.current_db.field_metadata.custom_field_metadata()
        self.has_read_column = prefs["read_lookup_name"] in fm and fm[prefs["read_lookup_name"]]['datatype'] == 'bool'
        self.has_fav_column = prefs["fav_lookup_name"] in fm and fm[prefs["fav_lookup_name"]]['datatype'] == 'bool'
        self.has_shelf_column = prefs["shelf_lookup_name"] in fm and fm[prefs["shelf_lookup_name"]]['datatype'] == 'text'
        #self.has_pb_position_column = prefs["pb_position_lookup_name"] in fm and fm[prefs["pb_position_lookup_name"]]['datatype'] in ["text", "comments"]
        #self.has_cr3_position_column = prefs["cr3_position_lookup_name"] in fm and fm[prefs["cr3_position_lookup_name"]]['datatype'] in ["text", "comments"]
        
        self.has_an_column = prefs["an_lookup_name"] in fm and fm[prefs["an_lookup_name"]]['datatype'] == 'comments'

        main_menu = self.menu
        main_menu.clear()
        
        self.qaction.setIcon(self.get_icon('pocketbook'))

        main_menu.addAction(self.get_icon('forward'), ('Send all metadata to Pocketbook'), self.send_all)
        sendMenu = main_menu.addMenu(('Send selected'))
        sendMenu.addAction(self.get_icon('catalog'), ('Send collections'), self.send_collections)
        sendMenu.addAction(self.get_icon('ok'), ('Send read statuses'), self.send_read)
        sendMenu.addAction(self.get_icon('heart'), ('Send favorite statuses'), self.send_favorite)
        sendMenu.addAction(self.get_icon('rating'), ('Send ratings (KOReader)'), self.send_ratings)
        sendMenu.addAction(self.get_icon('edit_input'), ('Send reviews (KOReader)'), self.send_reviews)

        main_menu.addAction(self.get_icon('back'), ('Load all metadata from Pocketbook'), self.load_all)
        loadMenu = main_menu.addMenu(('Load selected'))
        loadMenu.addAction(self.get_icon('catalog'), ('Load collections'), self.load_collections)
        loadMenu.addAction(self.get_icon('ok'), ('Load read statuses'), self.load_read)
        loadMenu.addAction(self.get_icon('heart'), ('Load favorite statuses'), self.load_favorite)
        loadMenu.addAction(self.get_icon('rating'), ('Load ratings (KOReader)'), self.load_ratings)
        loadMenu.addAction(self.get_icon('edit_input'), ('Load reviews (KOReader)'), self.load_reviews)

        main_menu.addAction(self.get_icon('highlight'), ('Extract annotations'), self.extract_annotations)

        main_menu.addAction(self.get_icon('auto-reload'), ('Sync reading positions'), self.sync_position)
        main_menu.addAction(self.get_icon('edit-redo'), ('Force update reading positions from Calibre'), self.force_position)

        

        main_menu.addAction(self.get_icon('config'), ('Configure plugin'), self.open_settings)
        
    
    # Menu commands functions

    def send_all(self):
        self.run_sync_job("send_all", "Sending all metadata to Pocketbook")


    def send_collections(self):
        self.run_sync_job("send_collections", "Sending collections to Pocketbook")
 

    def send_read(self):
        self.run_sync_job("send_read", "Sending read statuses to Pocketbook")


    def send_favorite(self):
        self.run_sync_job("send_favorite", "Sending favorite statuses to Pocketbook")


    def send_ratings(self):
        self.run_sync_job("send_ratings", "Sending ratings to Pocketbook (KOReader)")


    def send_reviews(self):
        self.run_sync_job("send_reviews", "Sending ratings to Pocketbook (KOReader)")



    def load_all(self):
        self.run_sync_job("load_all", "Loading all metadata from Pocketbook")



    def load_collections(self):
        self.run_sync_job("load_collections", "Loading collections from Pocketbook")


    def load_read(self):
        self.run_sync_job("load_read", "Loading read statuses from Pocketbook")


    def load_favorite(self):
        self.run_sync_job("load_favorite", "Loading favorite statuses from Pocketbook")


    def load_ratings(self):
        self.run_sync_job("load_ratings", "Loading favorite statuses from Pocketbook")


    def load_reviews(self):
        self.run_sync_job("load_reviews", "Loading favorite statuses from Pocketbook")



    def sync_position(self):
        self.run_sync_job("sync_position", "Syncing with reading positions")


    def force_position(self):
        self.run_sync_job("force_position", "Syncing with reading positions")


    def extract_annotations(self):
        self.run_sync_job("extract_annotations", "Extracting annotations")





    
    # Run job to sync any metadata
        
    def run_sync_job(self, command, desc):
        device_DB_path = self.get_device_DB_path()
        if device_DB_path:
            print("PB-COLLECTIONS: Start syncing metadata")

            # data in a dict where all additianal variables used by a job are stored
            data = {
                "dbpath": self.gui.current_db.library_path,
                "device_DB_path": device_DB_path,
                "device_storages": self.get_device_storages()
            }        
            
            args = ['calibre_plugins.pocketbook_collections.main', command, (data, )]
            self.gui.job_manager.run_job(self.Dispatcher(self.sync_done), "arbitrary", args=args, description=desc)
        else:
            error_dialog(self.gui, "Database not found", "No device collected or current device is not supported.", show=True)


    # After the job finished we must update downloaded metadata in calibre. It should not be updated from inside the job, because then GUI would not be refreshed.

    def sync_done(self, job):
        result = job.result
        try:
            to_load, done_msg = result
            if to_load == "error":
                error_dialog(self.gui, "Error", done_msg, show=True)
            elif to_load == None:
                pass
            else:
                

                if "read" in to_load and len(to_load["read"]) > 0:
                    self.gui.current_db.new_api.set_field(prefs["read_lookup_name"], to_load["read"])

                if "fav" in to_load and len(to_load["fav"]) > 0:
                    self.gui.current_db.new_api.set_field(prefs["fav_lookup_name"], to_load["fav"])

                if "shelf" in to_load and len(to_load["shelf"]) > 0:
                    self.gui.current_db.new_api.set_field(prefs["shelf_lookup_name"], to_load["shelf"])

                if "ratings" in to_load and len(to_load["ratings"]) > 0:
                    self.gui.current_db.new_api.set_field(prefs["rating_lookup_name"], to_load["ratings"])

                if "reviews" in to_load and len(to_load["reviews"]) > 0:
                    self.gui.current_db.new_api.set_field(prefs["review_lookup_name"], to_load["reviews"])

                if "annotations" in to_load and len(to_load["annotations"]) > 0:
                    self.gui.current_db.new_api.set_field(prefs["an_lookup_name"], to_load["annotations"])

                if "position" in to_load and len(to_load["position"]) > 0:
                    self.gui.current_db.new_api.set_field(prefs["position_lookup_name"], to_load["position"])

                if "books_to_refresh" in to_load and len(to_load["books_to_refresh"]) > 0: 
                    self.gui.iactions['Edit Metadata'].refresh_gui(to_load["books_to_refresh"], covers_changed=False)

            print(done_msg)
            print("PB-COLLECTIONS: End syncing metadata")
        except:
            error_dialog(self.gui, "Error", "Unknown error.", show=True)



            




    def open_settings(self):
        self.interface_action_base_plugin.do_user_config(self.gui)

        

    def get_icon(self, icon_name):
        icons = get_icons(PLUGIN_ICONS)
        icon_path = "images/" + icon_name + ".png"
        return icons[icon_path]



    def get_device_DB_path(self):
        device = self.gui.library_view.model().device_connected
        if device:
            devicePath = self.gui.device_manager.connected_device._main_prefix

            for version in (3, 2):
                explorer = 'explorer-%i' % version
                dbPath = os.path.join(devicePath, 'system', explorer, explorer + '.db')
                if os.path.exists(dbPath):
                    return dbPath
        return False
    


    def get_device_storages(self):
        storages = {}
        device = self.gui.library_view.model().device_connected
        if device:
            main_storage = self.gui.device_manager.connected_device._main_prefix
            storages["main"] = main_storage

            card_a = self.gui.device_manager.connected_device._card_a_prefix
            card_b = self.gui.device_manager.connected_device._card_b_prefix

            if(card_a):
                storages["card"] = card_a
            elif(card_b):
                storages["card"] = card_b

        return storages









    
