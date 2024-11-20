from qt.core import QVBoxLayout, QLabel, QLineEdit, QWidget
from calibre.utils.config import JSONConfig

prefs = JSONConfig('plugins/pocketbook_collections')

prefs.defaults['shelf_lookup_name'] = '#shelf'
prefs.defaults['read_lookup_name'] = '#read'
prefs.defaults['fav_lookup_name'] = '#favorite'


class ConfigWidget(QWidget):

    def __init__(self):
        QWidget.__init__(self)
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.main_label = QLabel('Select the columns to which Pocketbook metadata would sync.')
        self.main_label.setWordWrap(True)
        self.layout.addWidget(self.main_label)

        self.shelf_label = QLabel('Collections column (the type must be "comma separated text"):')
        self.shelf_label.setWordWrap(True)
        self.layout.addWidget(self.shelf_label)

        self.shelf_msg = QLineEdit(self)
        self.shelf_msg.setText(prefs['shelf_lookup_name'])
        self.layout.addWidget(self.shelf_msg)
        self.shelf_label.setBuddy(self.shelf_msg)

        self.read_label = QLabel('Read column (must be yes/no type):')
        self.read_label.setWordWrap(True)
        self.layout.addWidget(self.read_label)

        self.read_msg = QLineEdit(self)
        self.read_msg.setText(prefs['read_lookup_name'])
        self.layout.addWidget(self.read_msg)
        self.read_label.setBuddy(self.read_msg)

        self.fav_label = QLabel('Favorite column (must be yes/no type):')
        self.fav_label.setWordWrap(True)
        self.layout.addWidget(self.fav_label)

        self.fav_msg = QLineEdit(self)
        self.fav_msg.setText(prefs['fav_lookup_name'])
        self.layout.addWidget(self.fav_msg)
        self.fav_label.setBuddy(self.fav_msg)



    def save_settings(self):
        prefs['shelf_lookup_name'] = self.shelf_msg.text()
        prefs['read_lookup_name'] = self.read_msg.text()
        prefs['fav_lookup_name'] = self.fav_msg.text()

    