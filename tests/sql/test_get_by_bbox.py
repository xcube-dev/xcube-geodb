import unittest
import psycopg2
import testing.postgresql


class GeoDBSqlTest(unittest.TestCase):
    def setUp(self) -> None:
        self._postgresql = testing.postgresql.Postgresql()
        conn = psycopg2.connect(**self._postgresql.dsn())
        self._cursor = conn.cursor()
        with open('tests/sql/setup.sql') as sql_file:
            self._cursor.execute(sql_file.read())

    def tearDown(self) -> None:
        self._postgresql.stop()

    def test_query_by_bbox(self):
        with open('dcfs_geodb/sql/get_by_bbox.sql') as sql_file:
            sql_create = sql_file.read()
            self._cursor.execute(sql_create)

            sql_filter = "SELECT geodb_get_by_bbox('land_use', 452750.0, 88909.549, 464000.0, 102486.299)"
            res = self._cursor.execute(sql_filter)
            print(res)








