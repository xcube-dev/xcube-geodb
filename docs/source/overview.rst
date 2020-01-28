.. _CF conventions: http://cfconventions.org/cf-conventions/cf-conventions.html
.. _`dask`: https://dask.readthedocs.io/
.. _`JupyterLab`: https://jupyterlab.readthedocs.io/
.. _`WMTS`: https://en.wikipedia.org/wiki/Web_Map_Tile_Service
.. _xarray: http://xarray.pydata.org/
.. _xarray API: http://xarray.pydata.org/en/stable/api.html
.. _xarray.Dataset: http://xarray.pydata.org/en/stable/data-structures.html#dataset
.. _xarray.DataArray: http://xarray.pydata.org/en/stable/data-structures.html#dataarray
.. _`zarr`: https://zarr.readthedocs.io/
.. _`Zarr format`: https://zarr.readthedocs.io/en/stable/spec/v2.html
.. _`Sentinel Hub`: https://www.sentinel-hub.com/
.. _`Chunking and Performance`: http://xarray.pydata.org/en/stable/dask.html#chunking-and-performance

========
Overview
========

*xcube* is an open-source Python package and toolkit that has been developed to provide Earth observation (EO) data in an
analysis-ready form to users. xcube achieves this by carefully converting EO data sources into self-contained *data cubes*
that can be published in the cloud.

Data Cube
=========

The interpretation of the term *data cube* in the EO domain usually depends
on the current context. It may refer to a data service such as `Sentinel Hub`_, to some abstract
API, or to a concrete set of spatial images that form a time-series.

This section briefly explains the specific concept of a data cube used in the xcube project - the *xcube dataset*.

