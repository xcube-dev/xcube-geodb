.. highlight:: python

================================
Manage Collections in your GeoDB
================================

The API server is hosted at:

-  Hostname: https://3.120.53.215.nip.io/

Connecting to the GeoDB
~~~~~~~~~~~~~~~~~~~~~~~

Your credentials will be configured already if you have purchased access
to the GeoDB from the EOX Hib market place. Otherwise a client Id and secret will have been
provided to you.::

    >>> from xcube_geodb.core.geodb import GeoDBClient
    >>> geodb = GeoDBClient()
    >>> geodb.whoami
    'thedude'


You are now connected to the GeoDB. List your collections by running::

    >>> geodb.get_collections()


== ===========
\  table_name
== ===========
0  dudes_table
== ===========

Creating collections
~~~~~~~~~~~~~~~~~~~~

Once the connection has been established you will be able to create collections as well.
The table will contain standard properties (id, geometry, created_at, modified_at). Properties
can be added as well by using an OFC conformant json schema.

In the following example a collection is created and custom properties added. ::

    >>> collections = {
            "land_use":
            {
                "crs": 3794,
                "properties":
                {
                    "RABA_PID": "float",
                    "RABA_ID": "float",
                    "D_OD": "date"
                }
            }
        }

    >>> geodb.create_collections(collections)
    {'land_use': {'crs': 3794,
                  'properties': {'D_OD': 'date',
                                 'RABA_ID': 'float',
                                 'RABA_PID': 'float'}}}



Loading data into a dataset
---------------------------

Once the collection has been created, you can load data using GeoDataFrames. The
example below imports a shapefile into a GeoPandas Dataframe.
The attributes of the shapefile correspond to the collection’s properties.

.. code:: ipython3

    >>> import geopandas
    >>> gdf = geopandas.read_file('data/sample/land_use.shp')

.. container::

   === ========= ======= ========== ========================
   \   RABA_PID  RABA_ID D_OD       geometry
   === ========= ======= ========== ========================
   0   4770326.0 1410    2019-03-26 POLYGON ((453952.629 ...
   1   4770325.0 1300    2019-03-26 POLYGON ((453810.376 ...
   ... ...       ...     ...        ...
   === ========= ======= ========== ========================

   9827 rows × 4 columns

.. code:: ipython3

    >>> geodb.insert_into_collection('land_use', gdf)
    Data inserted into land_use


The data has now been loaded. The user can now query the dataset.

.. code:: ipython3

    >>> geodb.get_collection('land_use', query="raba_id=eq.7000")

.. container::

   +-----+-----+-------+-------+-------+-------+-------+-------+
   |     | id  | creat | m     | geo   | rab   | ra    | d_od  |
   |     |     | ed_at | odifi | metry | a_pid | ba_id |       |
   |     |     |       | ed_at |       |       |       |       |
   +=====+=====+=======+=======+=======+=======+=======+=======+
   | 0   | 3   | 2020  | None  | PO    | 23    | 7000  | 2019- |
   |     |     | -01-2 |       | LYGON | 05689 |       | 02-25 |
   |     |     | 9T08: |       | ((    |       |       |       |
   |     |     | 21:05 |       | 45609 |       |       |       |
   |     |     |       |       | 9.635 |       |       |       |
   |     |     |       |       | ...   |       |       |       |
   +-----+-----+-------+-------+-------+-------+-------+-------+
   | 1   | 26  | 2020  | None  | PO    | 23    | 7000  | 2019- |
   |     |     | -01-2 |       | LYGON | 01992 |       | 04-06 |
   |     |     | 9T08: |       | ((    |       |       |       |
   |     |     | 21:05 |       | 45989 |       |       |       |
   |     |     |       |       | 8.930 |       |       |       |
   |     |     |       |       | ...   |       |       |       |
   +-----+-----+-------+-------+-------+-------+-------+-------+
   | ... | ... | ...   | ...   | ...   | ...   | ...   | ...   |
   +-----+-----+-------+-------+-------+-------+-------+-------+

   384 rows × 7 columns


Deleting data from a collection
-------------------------------

Let's delete all collection entries where raba_id=7000 (land_use class).

.. code:: ipython3

    >>> geodb.delete_from_collection('land_use', query="raba_id=eq.7000")
    Data from land_use deleted


The collection does not contain any raba_ids anymore with the value of 7000.

.. code:: ipython3

    >>> geodb.get_collection('land_use', query="raba_id=eq.7000")

.. container::

   == ============
   \  Empty Result
   == ============


Updating data from a collection
-------------------------------

Collections can be updated. Let's set all d_dates to the first of January 2000 for all
features with a raba_id of 1300.

.. code:: ipython3

    >>> geodb.get_collection('land_use', query="raba_id=eq.1300")


.. container::

   +-----+-----+-------+-------+-------+-------+-------+-------+
   |     | id  | creat | m     | geo   | rab   | ra    | d_od  |
   |     |     | ed_at | odifi | metry | a_pid | ba_id |       |
   |     |     |       | ed_at |       |       |       |       |
   +=====+=====+=======+=======+=======+=======+=======+=======+
   | 0   | 2   | 2020  | None  | PO    | 47    | 1300  | 2019- |
   |     |     | -01-3 |       | LYGON | 70325 |       | 03-26 |
   |     |     | 0T16: |       | ((    |       |       |       |
   |     |     | 57:28 |       | 45381 |       |       |       |
   |     |     |       |       | 0.376 |       |       |       |
   |     |     |       |       | 91150 |       |       |       |
   |     |     |       |       | .199, |       |       |       |
   |     |     |       |       | 45381 |       |       |       |
   |     |     |       |       | 2.552 |       |       |       |
   |     |     |       |       | 9     |       |       |       |
   |     |     |       |       | 11... |       |       |       |
   +-----+-----+-------+-------+-------+-------+-------+-------+
   | 1   | 10  | 2020  | None  | PO    | 23    | 1300  | 2019- |
   |     |     | -01-3 |       | LYGON | 18555 |       | 03-14 |
   |     |     | 0T16: |       | ((    |       |       |       |
   |     |     | 57:28 |       | 45654 |       |       |       |
   |     |     |       |       | 7.427 |       |       |       |
   |     |     |       |       | 91543 |       |       |       |
   |     |     |       |       | .640, |       |       |       |
   |     |     |       |       | 45654 |       |       |       |
   |     |     |       |       | 4.255 |       |       |       |
   |     |     |       |       | 9     |       |       |       |
   |     |     |       |       | 15... |       |       |       |
   +-----+-----+-------+-------+-------+-------+-------+-------+
   | ... | ... | ...   | ...   | ...   | ...   | ...   | ...   |
   +-----+-----+-------+-------+-------+-------+-------+-------+

   895 rows × 7 columns

.. code:: ipython3

    >>> geodb.update_collection('land_use', query="raba_id=eq.1300", values={'d_od': '2000-01-01'})
    land_use updated

Now the d_dates have been changed. Please not the changed modified_at date.

.. code:: ipython3

    >>> geodb.get_collection('land_use', query="raba_id=eq.1300")

.. container::

   +-----+-----+-------+-------+-------+-------+-------+-------+
   |     | id  | creat | m     | geo   | rab   | ra    | d_od  |
   |     |     | ed_at | odifi | metry | a_pid | ba_id |       |
   |     |     |       | ed_at |       |       |       |       |
   +=====+=====+=======+=======+=======+=======+=======+=======+
   | 0   | 10  | 2020  | 2020  | PO    | 23    | 1300  | 2000- |
   |     |     | -01-3 | -01-3 | LYGON | 18555 |       | 01-01 |
   |     |     | 0T16: | 0T16: | ((    |       |       |       |
   |     |     | 57:28 | 57:43 | 45654 |       |       |       |
   |     |     |       |       | 7.427 |       |       |       |
   |     |     |       |       | ...   |       |       |       |
   +-----+-----+-------+-------+-------+-------+-------+-------+
   | ... | ... | ...   | ...   | ...   | ...   | ...   | ...   |
   +-----+-----+-------+-------+-------+-------+-------+-------+

   895 rows × 7 columns



Managing Properties
-------------------

.. code:: ipython3

    >>> geodb.get_collections()

.. table::

   == ==========
   \  table_name
   == ==========
   0  land_use
   == ==========


.. code:: ipython3

    >>> geodb.get_properties('land_use')

.. table::

   == ========== =========== ========================
   \  table_name column_name data_type
   == ========== =========== ========================
   0  land_use   id          integer
   1  land_use   created_at  timestamp with time zone
   2  land_use   modified_at timestamp with time zone
   3  land_use   geometry    USER-DEFINED
   4  land_use   raba_pid    double precision
   5  land_use   raba_id     double precision
   6  land_use   d_od        date
   == ========== =========== ========================


.. code:: ipython3

    >>> geodb.add_property('land_use', "test_prop", 'integer')
    Properties added



.. code:: ipython3

    >>> geodb.get_properties('land_use')

.. container::

   == ========== =========== ========================
   \  table_name column_name data_type
   == ========== =========== ========================
   0  land_use   id          integer
   1  land_use   created_at  timestamp with time zone
   2  land_use   modified_at timestamp with time zone
   3  land_use   geometry    USER-DEFINED
   4  land_use   raba_pid    double precision
   5  land_use   raba_id     double precision
   6  land_use   d_od        date
   7  land_use   test_prop   integer
   == ========== =========== ========================


.. code:: ipython3

    >>> geodb.drop_property('land_use', 'test_prop')
    Properties ['test_prop'] dropped from land_use


.. code:: ipython3

    >>> geodb.add_properties('land_use', properties={'test1': 'integer', 'test2': 'date'})
    Properties added

.. code:: ipython3

    >>> geodb.get_properties('land_use')

.. container::

   == ========== =========== ========================
   \  table_name column_name data_type
   == ========== =========== ========================
   0  land_use   id          integer
   1  land_use   created_at  timestamp with time zone
   2  land_use   modified_at timestamp with time zone
   3  land_use   geometry    USER-DEFINED
   4  land_use   raba_pid    double precision
   5  land_use   raba_id     double precision
   6  land_use   d_od        date
   7  land_use   test1       integer
   8  land_use   test2        date
   == ========== =========== ========================

.. code:: ipython3

    >>> geodb.drop_properties('land_use', properties=['test1', 'test2'])
    Properties ['test1', 'test2'] dropped from land_use





