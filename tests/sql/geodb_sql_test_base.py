# The MIT License (MIT)
# Copyright (c) 2025 by the xcube team
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import datetime
import os
import sys
import psycopg2
import signal
from time import sleep

import xcube_geodb.version as version


def get_app_dir():
    import inspect

    # noinspection PyTypeChecker
    version_path = inspect.getfile(version)
    return os.path.dirname(version_path)


class GeoDBSqlTestBase:
    _postgresql = None
    _cursor = None
    _conn = None

    def ignored(self):
        pass

    @classmethod
    def setUp(cls) -> None:
        import psycopg2
        import testing.postgresql

        # for Windows, in case there are multiple installations of Postgres
        # which makes psycopg find initdb like this:
        # c:\path\to\installation-1\initdb.exe\r\nc:\path\to\installation-2\initdb.exe
        # therefore, we split, and use the first installation that has "PostgreSQL" in its path
        # because the PostGIS extension cannot be installed via conda nor pip in a compatible way

        # Also, if you are using Docker Desktop for Windows, you have to do the
        # following (only once, not everytime you are running the tests):
        # dism.exe /Online /Disable-Feature:Microsoft-Hyper-V
        # netsh int ipv4 add excludedportrange protocol=tcp startport=50777 numberofports=1
        # dism.exe /Online /Enable-Feature:Microsoft-Hyper-V /All
        #
        # (copied from
        # https://github.com/docker/for-win/issues/3171#issuecomment-459205576)

        def find_program(name: str) -> str:
            program = testing.postgresql.find_program(name, [])
            return program.split("\r\n")[0]

        if os.name == "nt":
            path = os.environ["PATH"].split(os.pathsep)
            dirs = [directory for directory in path if "PostgreSQL" in directory]
            dirs.extend(
                [directory for directory in path if "PostgreSQL" not in directory]
            )
            os.environ["PATH"] = os.pathsep.join(dirs)

        initdb = find_program("initdb")
        postgres = find_program("postgres")

        # special windows treatment end

        postgresql = testing.postgresql.PostgresqlFactory(
            cache_initialized_db=False,
            initdb=initdb,
            postgres=postgres,
            port=50777,
        )

        cls._postgresql = postgresql()
        dsn = cls._postgresql.dsn()
        if sys.platform == "win32":
            dsn["port"] = 50777
            dsn["password"] = "postgres"
        cls._conn = psycopg2.connect(**dsn)
        cls._cursor = cls._conn.cursor()
        app_path = get_app_dir()
        fn = os.path.join(app_path, "sql", "geodb.sql")
        with open(fn) as sql_file:
            sql_content = sql_file.read()
            sql_content = sql_content.replace("VERSION_PLACEHOLDER", version.version)
            cls._cursor.execute(sql_content)

        fn = os.path.join(app_path, "..", "tests", "sql", "setup.sql")
        with open(fn) as sql_file:
            cls._cursor.execute(sql_file.read())
        cls._conn.commit()

    def tearDown(self) -> None:
        if sys.platform == "win32":
            self.manual_cleanup()
        else:
            self._postgresql.stop()

    def manual_cleanup(self):
        import shutil

        try:
            self._conn.close()
            dsn = self._postgresql.dsn()
            dsn["port"] = 50777
            dsn["password"] = "postgres"
            dsn["database"] = "postgres"
            self._conn = psycopg2.connect(**dsn)
            self._conn.autocommit = True
            self._cursor = self._conn.cursor()
            self._cursor.execute("drop database test;")
            self._cursor.execute("create database test;")
            self.set_role("postgres")
            self._cursor.execute(
                "DROP ROLE IF EXISTS geodb_user ; "
                "DROP ROLE IF EXISTS geodb_user_read_only ; "
                'DROP ROLE IF EXISTS "geodb_user-with-hyphens" ; '
                "DROP ROLE IF EXISTS test_group ; "
                "DROP ROLE IF EXISTS new_group ; "
                "DROP ROLE IF EXISTS test_admin ; "
                "DROP ROLE IF EXISTS test_noadmin ; "
                "DROP ROLE IF EXISTS test_member ; "
                "DROP ROLE IF EXISTS test_member_2 ; "
                "DROP ROLE IF EXISTS test_nomember ;"
                "DROP ROLE IF EXISTS some_group ;"
            )
            self._conn.commit()
            self._conn.close()
            self._postgresql.child_process.send_signal(signal.CTRL_BREAK_EVENT)
            killed_at = datetime.datetime.now()
            while self._postgresql.child_process.poll() is None:
                if (datetime.datetime.now() - killed_at).seconds > 10.0:
                    self._postgresql.child_process.kill()
                    raise RuntimeError(
                        "*** failed to shutdown postgres (timeout) ***\n"
                    )

                sleep(0.1)

        except OSError as e:
            raise e
        shutil.rmtree(self._postgresql.base_dir, ignore_errors=True)

    def tearDownModule(self):
        # clear cached database at end of tests
        self._postgresql.clear_cache()

    def set_role(self, user_name: str):
        sql = f'SET LOCAL ROLE "{user_name}"'
        self._cursor.execute(sql)
