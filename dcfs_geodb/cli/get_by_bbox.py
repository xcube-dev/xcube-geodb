# The MIT License (MIT)
# Copyright (c) 2019 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import click

__author__ = "Helge Dzierzon (Brockmann Consult GmbH)"


@click.command(name='get_by_bbox')
@click.option('--dataset', '-d', 'dataset', metavar='DATASET', help="Comma-separated list of bbox.")
@click.option('--bbox', '-b', 'bbox', metavar='BBOX', help="Comma-separated list of bbox.")
@click.option('--limit', '-l', 'limit', metavar='LIMIT', help="Limit or rows to be returned")
@click.option('--offset', '-o', 'offset', metavar='OFFSET', help="Starting point")
@click.option('--bbox-mode', '-m', 'bbox-mode', metavar='BBOX_MODE', help="Starting point")
@click.option('--bbox-crs', '-c', 'bbox-crs', metavar='BBOX_CRS', help="Starting point")
def get_by_bbox(dataset, bbox, limit, offset, bbox_mode, bbox_crs):
    from dcfs_geodb.core.geo_db import GeoDB

    bbox = bbox.split(',')

    api = GeoDB()
    api.get_by_bbox(dataset=dataset, minx=bbox[0], miny=bbox[1], maxx=bbox[2], maxy=bbox[3], limit=limit, offset=offset,
                    bbox_mode=bbox_mode, bbox_crs=bbox_crs)



