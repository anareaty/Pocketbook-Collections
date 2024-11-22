__license__   = 'GPL v3'
__copyright__ = '2024, Anareaty <reatymain@gmail.com>'
__docformat__ = 'restructuredtext en'


from calibre.gui2.actions import InterfaceAction
from calibre_plugins.pocketbook_collections.__init__ import PLUGIN_NAME
from calibre_plugins.pocketbook_collections.config import prefs
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
                'images/donate.png']




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

        main_menu = self.menu
        main_menu.clear()
        
        self.qaction.setIcon(self.get_icon('pocketbook'))

        main_menu.addAction(self.get_icon('forward'), ('Send all metadata to Pocketbook'), self.send_all_command)
        sendMenu = main_menu.addMenu(('Send selected'))
        sendMenu.addAction(self.get_icon('catalog'), ('Send collections'), self.send_collections_command)
        sendMenu.addAction(self.get_icon('ok'), ('Send read statuses'), self.send_read_command)
        sendMenu.addAction(self.get_icon('donate'), ('Send favorite statuses'), self.send_favorite_command)

        main_menu.addAction(self.get_icon('back'), ('Load all metadata from Pocketbook'), self.load_all_command)
        loadMenu = main_menu.addMenu(('Load selected'))
        loadMenu.addAction(self.get_icon('catalog'), ('Load collections'), self.load_collections_command)
        loadMenu.addAction(self.get_icon('ok'), ('Load read statuses'), self.load_read_command)
        loadMenu.addAction(self.get_icon('donate'), ('Load favorite statuses'), self.load_favorite_command)

        main_menu.addAction(self.get_icon('config'), ('Configure plugin'), self.open_settings)
        
    
    # Menu commands functions

    def send_all_command(self):
        if self.has_shelf_column or self.has_fav_column or self.has_read_column:
            self.run_sync_job("send all", "Sending all metadata to Pocketbook", "All statuses and collections have been sent to Pocketbook")
        else:
            error_dialog(self.gui, "Can not send metadata", "Columns " + prefs["shelf_lookup_name"] + ", " + prefs["read_lookup_name"] + " and " + prefs["fav_lookup_name"] + " do not exist", show=True)

    def send_collections_command(self):
        if self.has_shelf_column:
            self.run_sync_job("send collections", "Sending collections to Pocketbook", "All collections have been sent to Pocketbook")
        else:
            error_dialog(self.gui, "Can not send collections", "Column " + prefs["shelf_lookup_name"] + " does not exist", show=True)
        
    def send_read_command(self):
        if self.has_read_column:
            self.run_sync_job("send read", "Sending read statuses to Pocketbook", "All read statuses have been sent to Pocketbook")
        else:
            error_dialog(self.gui, "Can not send read statuses", "Column " + prefs["read_lookup_name"] + " does not exist", show=True)

    def send_favorite_command(self):
        if self.has_fav_column:
            self.run_sync_job("send favorite", "Sending favorite statuses to Pocketbook", "All favorite statuses have been sent to Pocketbook")
        else:
            error_dialog(self.gui, "Can not send favorite statuses", "Column " + prefs["fav_lookup_name"] + " does not exist", show=True)

    def load_all_command(self):
        if self.has_shelf_column or self.has_fav_column or self.has_read_column:
            self.run_sync_job("load all", "Loading all metadata from Pocketbook", "All statuses and collections have been loaded from Pocketbook")
        else:
            error_dialog(self.gui, "Can not load metadata", "Columns " + prefs["shelf_lookup_name"] + ", " + prefs["read_lookup_name"] + " and " + prefs["fav_lookup_name"] + " do not exist", show=True)
        
    def load_collections_command(self):
        if self.has_shelf_column:
            self.run_sync_job("load collections", "Loading collections from Pocketbook", "All collections have been loaded from Pocketbook")
        else:
            error_dialog(self.gui, "Can not load collections", "Column " + prefs["shelf_lookup_name"] + " does not exist", show=True)
        
    def load_read_command(self):
        if self.has_read_column:
            self.run_sync_job("load read", "Loading read statuses from Pocketbook", "All read statuses have been loaded from Pocketbook")
        else:
            error_dialog(self.gui, "Can not load read statuses", "Column " + prefs["read_lookup_name"] + " does not exist", show=True)
        
    def load_favorite_command(self):
        if self.has_fav_column:
            self.run_sync_job("load favorite", "Loading favorite statuses from Pocketbook", "All favorite statuses have been loaded from Pocketbook")
        else:
            error_dialog(self.gui, "Can not load favorite statuses", "Column " + prefs["fav_lookup_name"] + " does not exist", show=True)
        

    
    # Run job to sync any metadata
        
    def run_sync_job(self, command, desc, done_msg):

        # data in a dict where all additianal variables used by a job are stored
        data = {}        
        data["dbpath"] = self.gui.current_db.library_path
        data["device"] = self.gui.library_view.model().device_connected
        data["device_DB_path"] = self.get_device_DB_path()
        data["device_storages"] = self.get_device_storages()
        data["has_read_column"] = self.has_read_column
        data["has_fav_column"] = self.has_fav_column
        data["has_shelf_column"] = self.has_shelf_column

        args = ['calibre_plugins.pocketbook_collections.main','sync_metadata', (data, command, done_msg)]
        self.gui.job_manager.run_job(self.sync_done, "arbitrary", args=args, description=desc)


    # After the job finished we must update downloaded metadata in calibre. It should not be updated from inside the job, because then GUI would not be refreshed.

    def sync_done(self, job):
        to_load, done_msg = job.result

        print("start updating Calibre metadata")

        for obj in to_load["read"]:
            #print("read")
            #print(obj)
            self.gui.current_db.new_api.set_field(prefs["read_lookup_name"], obj)
            self.gui.iactions['Edit Metadata'].refresh_gui(list(obj.keys()), covers_changed=False)

        for obj in to_load["fav"]:
            #print("fav")
            #print(obj)
            self.gui.current_db.new_api.set_field(prefs["fav_lookup_name"], obj)
            self.gui.iactions['Edit Metadata'].refresh_gui(list(obj.keys()), covers_changed=False)

        for obj in to_load["shelf"]:
            print(obj)
            self.gui.current_db.new_api.set_field(prefs["shelf_lookup_name"], obj)
            self.gui.iactions['Edit Metadata'].refresh_gui(list(obj.keys()), covers_changed=False)

        print(done_msg)



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









    
