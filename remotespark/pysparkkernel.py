# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
from ipykernel.ipkernel import IPythonKernel

from remotespark.livyclientlib.log import Log


class PySparkKernel(IPythonKernel):
    implementation = 'PySpark'
    implementation_version = '1.0'
    language = 'no-op'
    language_version = '0.1'
    language_info = {
        'name': 'pyspark',
        'mimetype': 'text/x-python'
    }
    banner = "PySpark with automatic visualizations"

    logger = Log()

    already_ran_once = False
    spark_controller = None
    use_altair = False
    mode = "normal"

    name = "pys"
    session_language = "python"
    connection_string = "url=https://livy4.cloudapp.net/livy;username=admin;password=Password1!"

    def initialize_magics(self):
        self.already_ran_once = True

        register_magics_code = "%load_ext remotespark\nimport requests\nrequests.packages.urllib3.disable_warnings()"
        self.execute_cell_for_user(register_magics_code, True, False)

        add_endpoint_code = "%spark add {} {} {}".format(self.name, self.session_language, self.connection_string)
        self.execute_cell_for_user(add_endpoint_code, True, False)

    def execute_cell_for_user(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        return super(PySparkKernel, self).do_execute(code, silent, store_history, user_expressions, allow_stdin)

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if not self.already_ran_once:
            self.initialize_magics()

        # Modify code by preppending spark magic text
        if code.lower().startswith("%sql\n") or code.lower().startswith("%sql "):
            code = "%%spark -s True\n{}".format(code[5:])
        else:
            code = "%%spark\n{}".format(code)

        return self.execute_cell_for_user(code, silent, store_history, user_expressions, allow_stdin)

    def do_shutdown(self, restart):
        # Cleanup
        code = "%spark cleanup"
        self.execute_cell_for_user(code, True, False)

        return super(PySparkKernel, self).do_shutdown(restart)


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=PySparkKernel)
