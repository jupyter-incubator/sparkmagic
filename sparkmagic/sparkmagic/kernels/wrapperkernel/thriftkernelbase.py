import sys
import traceback

from sparkmagic.utils.thriftlogger import ThriftLog
from sparkmagic.kernels.wrapperkernel.kernelbase import KernelBase
from sparkmagic.thriftclient.tabcompleter import Completer
import sparkmagic.utils.configuration as conf
from sparkmagic.thriftclient.closablewidget import ClosableWidget

class ThriftKernelBase(KernelBase):
    def __init__(self, implementation, implementation_version, language, language_version, language_info, user_code_parser=None, **kwargs):
        self.logger = ThriftLog(self.__class__, conf.logging_config_debug())
        magics_lib = "sparkmagic.thriftmagics.magics"
        super(ThriftKernelBase, self).__init__(
                implementation,
                implementation_version,
                language,
                language_version,
                language_info,
                magics_lib,
                user_code_parser,
                **kwargs)

        self._completer = Completer()
        self.logger.debug("Initialized '{}'".format(self.__class__))


    # TODO: To remove dead links at restart and shutdown
    def do_shutdown(self, restart):
        self.logger.info('Shutting down with restart={}'.format(restart))
        if conf.use_auto_viz():
            pass
        for w in ClosableWidget.close_widgets:
            self.logger.info('Closing widget...')
            w.close()
        return self._do_shutdown_ipykernel(restart)

    def do_complete(self, code, pos):
        try:
            return self._do_complete(code, pos)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            info = traceback.format_tb(exc_traceback)
            self.logger.error("Failed to 'do_complete' with:\n{}".format("".join(info)))


    def _do_complete(self, code, pos):
        self._completer.complete(code, pos)
        matches = self._completer.suggestions()
        prefix = self._completer.prefix()
        fullword = self._completer.fullword()
        (start_pos, end_pos) = self._completer.cursorpostitions()

        # Append prefix before match to avoid 'auto-swap' at tab when only having one option
        if fullword:
            matches.append(fullword)
            matches.append(prefix)

        if matches is None:
            self.logger.debug("No match found - likely no code or an error occurd")
            conent = {
                'matches' : [],
                'cursor_start' : pos,
                'cursor_end' : pos,
                'metadata' : {},
                'status' : 'ok'
            }
        else:
            self.logger.debug("Found {} matches: {}...".format(len(matches), matches[:10]))
            self.logger.debug("pos, start, end: {}, {}, {}\n".format(pos, start_pos, end_pos))
            content = {
                'matches' : matches,
                'cursor_start' : start_pos,
                'cursor_end' : end_pos,
                'metadata' : {},
                'status' : 'ok'
            }
        return content