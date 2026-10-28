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
from pathlib import Path
from shutil import which

import xcube_geodb.version as version


def get_app_dir():
    import inspect

    # noinspection PyTypeChecker
    version_path = inspect.getfile(version)
    return os.path.dirname(version_path)


def find_postgresql_programs():
    """Find a matching server/initdb pair in PATH, then the Python environment."""
    directories = [
        *map(Path, os.get_exec_path()),
        Path(sys.prefix) / "Library" / "bin",
        Path(sys.prefix) / "bin",
    ]
    for directory in directories:
        programs = tuple(which(str(directory / name)) for name in ("initdb", "postgres"))
        if all(programs):
            return programs
    raise RuntimeError(
        f"PostgreSQL server programs initdb and postgres were not found together in PATH or {sys.prefix}. "
        "The SQL-test environment needs PostgreSQL and PostGIS."
    )


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

        # Windows/Docker Desktop can reserve the fixed test port 50777 even
        # when no process is listening. Check excluded ranges if binding fails:
        # netsh interface ipv4 show excludedportrange protocol=tcp
        # Historical workaround for a confirmed conflict:
        # dism.exe /Online /Disable-Feature:Microsoft-Hyper-V
        # netsh int ipv4 add excludedportrange protocol=tcp startport=50777 numberofports=1
        # dism.exe /Online /Enable-Feature:Microsoft-Hyper-V /All
        # This workaround requires administrator rights and Windows restarts.
        # https://github.com/docker/for-win/issues/3171#issuecomment-459205576

        initdb, postgres = find_postgresql_programs()

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
