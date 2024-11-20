__license__   = 'GPL v3'
__copyright__ = '2024, Anareaty <reatymain@gmail.com>'
__docformat__ = 'restructuredtext en'


from calibre.customize import InterfaceActionBase

PLUGIN_NAME = 'Pocketbook Collections'
PLUGIN_VERSION_TUPLE = (1, 0, 0)
PLUGIN_VERSION = '.'.join([str(x) for x in PLUGIN_VERSION_TUPLE])



class PocketbookCollectionsAction(InterfaceActionBase):
    name                    = PLUGIN_NAME
    description             = 'Sync Pocketbook collections, read statuses and favorite statuses with Calibre'
    supported_platforms     = ['windows', 'osx', 'linux']
    author                  = 'anareaty'
    version                 = PLUGIN_VERSION_TUPLE
    minimum_calibre_version = (3, 48, 0)

    actual_plugin           = 'calibre_plugins.pocketbook_collections.ui:InterfacePlugin'

    def is_customizable(self):
        return True

    def config_widget(self):
        from calibre_plugins.pocketbook_collections.config import ConfigWidget
        return ConfigWidget()

    def save_settings(self, config_widget):
        config_widget.save_settings()
        if self.actual_plugin_:
            self.actual_plugin_.rebuild_menus()

