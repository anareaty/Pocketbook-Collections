

__license__   = 'GPL v3'
__copyright__ = '2024, Anareaty <reatymain@gmail.com>'
__docformat__ = 'restructuredtext en'

try:
    from qt.core import QVBoxLayout, QLabel, QLineEdit, QWidget, QCheckBox, Qt, QGridLayout, QGroupBox, QComboBox
except:
    try:
        from PyQt5.Qt import QVBoxLayout, QLabel, QLineEdit, QWidget, QCheckBox, Qt, QGridLayout, QGroupBox, QComboBox
    except:
        from PyQt4.Qt import QVBoxLayout, QLabel, QLineEdit, QWidget, QCheckBox, Qt,QGridLayout, QGroupBox, QComboBox

from calibre.utils.config import JSONConfig



prefs = JSONConfig('plugins/pocketbook_collections')

prefs.defaults['shelf_lookup_name'] = None
prefs.defaults['an_lookup_name'] = None
prefs.defaults['read_lookup_name'] = None
prefs.defaults['fav_lookup_name'] = None
prefs.defaults['position_lookup_name'] = None
prefs.defaults['rating_lookup_name'] = None
prefs.defaults['review_lookup_name'] = None

prefs.defaults['sync_pb_pos'] = True
prefs.defaults['sync_pb_an'] = True
prefs.defaults['sync_kr_pos'] = False
prefs.defaults['sync_kr_an'] = False
prefs.defaults['sync_kr_shelf'] = False
prefs.defaults['sync_kr_status'] = False
prefs.defaults['sync_kr_fav'] = False
prefs.defaults['sync_cr_pos'] = False
prefs.defaults['sync_cr_an'] = False
prefs.defaults['prefer_kr_shelf'] = False
prefs.defaults['prefer_kr_fav'] = False


class ConfigWidget(QWidget):

    def __init__(self, plugin_action):

        

        self.gui = plugin_action.gui
        self.field_by_name = {"None": None}
        self.index_by_field = {}


        
        QWidget.__init__(self)

        
        

        layout = QVBoxLayout()
        self.setLayout(layout)

        main_group = QGroupBox(self)
        main_group.setTitle("Main settings")
        layout.addWidget(main_group)
        main_layout = QGridLayout(main_group)
        main_layout.setColumnStretch(0,1)
        main_layout.setColumnMinimumWidth(1, 150)

        shelf_label = QLabel('Collections column:')
        shelf_label.setWordWrap(True)
        main_layout.addWidget(shelf_label, 0, 0)
        self.shelf_combo = QComboBox(self)
        self.shelf_combo.addItems(self.get_columns("text", custom=True, multiple=True))
        self.shelf_combo.setCurrentIndex(self.get_index(prefs['shelf_lookup_name']))
        shelf_label.setBuddy(self.shelf_combo)
        main_layout.addWidget(self.shelf_combo, 0, 1)


        read_label = QLabel('Read column:')
        read_label.setWordWrap(True)
        main_layout.addWidget(read_label, 1, 0)
        self.read_combo = QComboBox(self)
        self.read_combo.addItems(self.get_columns("bool", custom=True))
        self.read_combo.setCurrentIndex(self.get_index(prefs['read_lookup_name']))
        read_label.setBuddy(self.read_combo)
        main_layout.addWidget(self.read_combo, 1, 1)


        fav_label = QLabel('Favorite column:')
        fav_label.setWordWrap(True)
        self.fav_combo = QComboBox(self)
        self.fav_combo.addItems(self.get_columns("bool", custom=True))
        self.fav_combo.setCurrentIndex(self.get_index(prefs['fav_lookup_name']))
        fav_label.setBuddy(self.fav_combo)
        main_layout.addWidget(fav_label, 2, 0)
        main_layout.addWidget(self.fav_combo, 2, 1)




        an_label = QLabel('Annotations column:')
        an_label.setWordWrap(True)
        self.an_combo = QComboBox(self)
        self.an_combo.addItems(self.get_columns("comments", custom=True))
        self.an_combo.setCurrentIndex(self.get_index(prefs['an_lookup_name']))
        an_label.setBuddy(self.an_combo)
        main_layout.addWidget(an_label, 3, 0)
        main_layout.addWidget(self.an_combo, 3, 1)

        position_label = QLabel('Column to sync reading positions:')
        position_label.setWordWrap(True)
        self.position_combo = QComboBox(self)
        self.position_combo.addItems(self.get_columns("comments", custom=True))
        self.position_combo.setCurrentIndex(self.get_index(prefs['position_lookup_name']))
        position_label.setBuddy(self.position_combo)
        main_layout.addWidget(position_label, 4, 0)
        main_layout.addWidget(self.position_combo, 4, 1)

        self.sync_pb_pos_checkbox = QCheckBox("Sync main Pocketbook app reading positions")
        self.sync_pb_pos_checkbox.setChecked(prefs['sync_pb_pos'])
        main_layout.addWidget(self.sync_pb_pos_checkbox, 5, 0, 1, 2)

        self.sync_pb_an_checkbox = QCheckBox("Load main Pocketbook app annotations")
        self.sync_pb_an_checkbox.setChecked(prefs['sync_pb_an'])
        main_layout.addWidget(self.sync_pb_an_checkbox, 6, 0, 1, 2)


        # KOReader settings

        kr_group = QGroupBox(self)
        kr_group.setTitle("KOReader app settings")
        kr_group.setToolTip("Only enable this if you have KOReader app installed on your Pocketbook.")
        layout.addWidget(kr_group)
        kr_layout = QGridLayout(kr_group)
        kr_layout.setColumnStretch(0,1)
        kr_layout.setColumnMinimumWidth(1, 150)

        self.sync_kr_pos_checkbox = QCheckBox("Sync KOReader reading positions")
        self.sync_kr_pos_checkbox.setChecked(prefs['sync_kr_pos'])
        kr_layout.addWidget(self.sync_kr_pos_checkbox, 0, 0, 1, 2)

        self.sync_kr_an_checkbox = QCheckBox("Load KOReader annotations")
        self.sync_kr_an_checkbox.setChecked(prefs['sync_kr_an'])
        kr_layout.addWidget(self.sync_kr_an_checkbox, 1, 0, 1, 2)

        self.sync_kr_shelf_checkbox = QCheckBox("Sync collections to KOReader")
        self.sync_kr_shelf_checkbox.setChecked(prefs['sync_kr_shelf'])
        kr_layout.addWidget(self.sync_kr_shelf_checkbox, 2, 0, 1, 2)

        self.prefer_kr_shelf_checkbox = QCheckBox("Prefer KOReader collections when loading collections from device")
        self.prefer_kr_shelf_checkbox.setChecked(prefs['prefer_kr_shelf'])
        kr_layout.addWidget(self.prefer_kr_shelf_checkbox, 3, 0, 1, 2)

        self.sync_kr_status_checkbox = QCheckBox("Sync reading status to KOReader")
        self.sync_kr_status_checkbox.setChecked(prefs['sync_kr_status'])
        kr_layout.addWidget(self.sync_kr_status_checkbox, 4, 0, 1, 2)

        self.sync_kr_fav_checkbox = QCheckBox("Sync favorites to KOReader")
        self.sync_kr_fav_checkbox.setChecked(prefs['sync_kr_fav'])
        kr_layout.addWidget(self.sync_kr_fav_checkbox, 5, 0, 1, 2)

        self.prefer_kr_fav_checkbox = QCheckBox("Prefer KOReader favorite status when loading favorite statuses from device")
        self.prefer_kr_fav_checkbox.setChecked(prefs['prefer_kr_fav'])
        kr_layout.addWidget(self.prefer_kr_fav_checkbox, 6, 0, 1, 2)

        rating_label = QLabel('Column to sync rating with KOReader:')
        rating_label.setWordWrap(True)
        self.rating_combo = QComboBox(self)
        self.rating_combo.addItems(self.get_columns("rating", custom=False))
        self.rating_combo.setCurrentIndex(self.get_index(prefs['rating_lookup_name']))
        rating_label.setBuddy(self.rating_combo)
        kr_layout.addWidget(rating_label, 7, 0)
        kr_layout.addWidget(self.rating_combo, 7, 1)
        
        review_label = QLabel('Column to sync book review with KOReader:')
        review_label.setWordWrap(True)
        self.review_combo = QComboBox(self)
        self.review_combo.addItems(self.get_columns("comments", custom=True))
        self.review_combo.setCurrentIndex(self.get_index(prefs['review_lookup_name']))
        review_label.setBuddy(self.review_combo)
        kr_layout.addWidget(review_label, 8, 0)
        kr_layout.addWidget(self.review_combo, 8, 1)


        # CoolReader settings

        cr_group = QGroupBox(self)
        cr_group.setTitle("CoolReader app settings")
        cr_group.setToolTip("Only enable this if you have CoolReader app installed on your Pocketbook.")
        layout.addWidget(cr_group)
        cr_layout = QVBoxLayout(cr_group)

        self.sync_cr_pos_checkbox = QCheckBox("Sync CoolReader reading positions")
        self.sync_cr_pos_checkbox.setChecked(prefs['sync_cr_pos'])
        cr_layout.addWidget(self.sync_cr_pos_checkbox)

        self.sync_cr_an_checkbox = QCheckBox("Load CoolReader annotations")
        self.sync_cr_an_checkbox.setChecked(prefs['sync_cr_an'])
        cr_layout.addWidget(self.sync_cr_an_checkbox)

        
        self.resize(self.sizeHint())
        

        


    def get_columns(self, type, custom=True, multiple=False):
        db = self.gui.current_db
        if custom:
            fields = db.custom_field_keys()
        else:
            fields = db.all_field_keys()
        type_fields = ["None"]
        for field in fields:
            field_data = db.metadata_for_field(field)
            if field_data['datatype'] == type and bool(field_data['is_multiple']) == multiple and field_data['name']:
                index = len(type_fields)
                type_fields.append(field_data['name'])
                self.field_by_name[field_data['name']] = field
                self.index_by_field[field] = index
        return type_fields
    

    def get_index(self, field):
        if field != None and field in self.index_by_field:
            return self.index_by_field[field]
        else:
            return 0




        



    def save_settings(self):

        prefs['shelf_lookup_name'] = self.field_by_name[self.shelf_combo.currentText()]
        prefs['read_lookup_name'] = self.field_by_name[self.read_combo.currentText()]
        prefs['fav_lookup_name'] = self.field_by_name[self.fav_combo.currentText()]
        prefs['an_lookup_name'] = self.field_by_name[self.an_combo.currentText()]
        prefs['position_lookup_name'] = self.field_by_name[self.position_combo.currentText()]
        prefs['rating_lookup_name'] = self.field_by_name[self.rating_combo.currentText()]
        prefs['review_lookup_name'] = self.field_by_name[self.review_combo.currentText()]
        
        prefs['sync_pb_pos'] = self.sync_pb_pos_checkbox.isChecked()
        prefs['sync_pb_an'] = self.sync_pb_an_checkbox.isChecked()
        prefs['sync_kr_pos'] = self.sync_kr_pos_checkbox.isChecked()
        prefs['sync_kr_an'] = self.sync_kr_an_checkbox.isChecked()
        prefs['sync_kr_shelf'] = self.sync_kr_shelf_checkbox.isChecked()
        prefs['sync_kr_status'] = self.sync_kr_status_checkbox.isChecked()
        prefs['sync_kr_fav'] = self.sync_kr_fav_checkbox.isChecked()
        prefs['sync_cr_pos'] = self.sync_cr_pos_checkbox.isChecked()
        prefs['sync_cr_an'] = self.sync_cr_an_checkbox.isChecked()
        prefs['prefer_kr_shelf'] = self.prefer_kr_shelf_checkbox.isChecked()
        prefs['prefer_kr_fav'] = self.prefer_kr_fav_checkbox.isChecked()


















