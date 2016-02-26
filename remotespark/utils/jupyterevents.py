# Read the config file that will return the filehandler
# file handler will be one of two local file handler or hdinsighteventshandler

import remotespark.utils.configuration as conf


class JupyterEvents:
    def __init__(self):

        if conf.events_config()["local"] == 1:
            # Create a local events storage
            self.local = 1
            # Create the local file for the events
        else:
            # Load the hdinsighteventshandler class
            self.local = 0
            self._import(conf.events_config()["handler"])

    def _import(self, name):
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod

    def fire_event(self, **kwargs):
        # Store the events
        # Based on the config file which wil control which
