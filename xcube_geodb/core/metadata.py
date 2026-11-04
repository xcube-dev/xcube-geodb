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
from typing import (
    List,
    Optional,
    Dict,
    Any,
    Union,
    TypeAlias,
    Sequence,
    TYPE_CHECKING,
)

from pystac import Asset, ItemAssetDefinition, Link, Provider

from xcube_geodb.core.db_interface import DbInterface

if TYPE_CHECKING:
    from xcube_geodb.core.geodb import GeoDBClient


# noinspection PyShadowingBuiltins
class Range(dict):  # inheriting from dict to make it JSON serializable
    def __init__(self, min: Union[float, str], max: Union[float, str]):
        dict.__init__(self)
        self["min"] = min
        self["min"] = max


JSONSchema: TypeAlias = str
SpatialExtent: TypeAlias = List[List[float]]
TemporalExtent: TypeAlias = List[List[Optional[str]]]
Summary: TypeAlias = List[Any] | Range | JSONSchema


class MetadataManager:
    def __init__(self, geodb: "GeoDBClient", db_interface: DbInterface):
        self._geodb = geodb
        self._db_interface = db_interface

    def from_json(self, json: Dict[str, Any], collection: str, database: str):
        providers = self._get_providers(json)
        assets = self._get_assets(json)
        item_assets = self._get_item_assets(json)
        links = self._get_links(json)
        summaries = (
            json["basic"]["summaries"]
            if "summaries" in json["basic"] and json["basic"]["summaries"] is not None
            else {}
        )
        stac_extensions = (
            json["basic"]["stac_extensions"]
            if "stac_extensions" in json["basic"]
               and json["basic"]["stac_extensions"] is not None
            else []
        )
        title = (
            json["basic"]["title"]
            if "title" in json["basic"] and json["basic"]["title"] is not None
            else ""
        )
        keywords = (
            json["basic"]["keywords"]
            if "keywords" in json["basic"] and json["basic"]["keywords"] is not None
            else []
        )
        spatial_extent = (
            json["basic"]["spatial_extent"]
            if "spatial_extent" in json["basic"]
               and json["basic"]["spatial_extent"] is not None
               and not json["basic"]["spatial_extent"] == []
            else None
        )
        if not spatial_extent:
            # get from database
            bbox = self._geodb.get_collection_bbox(collection, database)
            if bbox:
                srid = self._geodb.get_collection_srid(collection, database)
                spatial_extent = [[bbox[0], bbox[1], bbox[2], bbox[3]]]
            else:
                srid = "4326"
                spatial_extent = [[-180, -90, 180, 90]]
            self._set_spatial_extent(spatial_extent, collection, database, srid)
        else:
            # reformat what we got from metadata table
            spatial_extent = [
                [se["minx"], se["miny"], se["maxx"], se["maxy"]]
                for se in spatial_extent
            ]
        return Metadata(
            id=collection,
            title=title,
            links=links,
            spatial_extent=spatial_extent,
            temporal_extent=json["basic"]["temporal_extent"],
            description=json["basic"]["description"],
            license=json["basic"]["license"],
            providers=providers,
            stac_extensions=stac_extensions,
            keywords=keywords,
            summaries=summaries,
            assets=assets,
            item_assets=item_assets,
        )

    def _set_spatial_extent(
            self, spatial_extent: Sequence, collection: str, database: str, srid: str
    ):
        path = "/rpc/geodb_set_spatial_extent"
        self._db_interface.post(
            path,
            payload={
                "collection": collection,
                "db": database,
                "se": spatial_extent,
                "srid": int(srid),
            },
        )

    @staticmethod
    def _get_providers(json: Dict[str, Any]) -> Optional[List[Provider]]:
        if "providers" not in json:
            return None
        return [
            Provider.from_dict({
                k: v for k, v in provider_spec.items()
                if k not in {"id", "collection_name", "database"}
            })
            for provider_spec in json["providers"]
        ]

    @staticmethod
    def _get_assets(json: Dict[str, Any]) -> Optional[List[Asset]]:
        if "assets" not in json:
            return None
        return [
            Asset.from_dict({
                k: v for k, v in asset_spec.items()
                if k not in {"id", "collection_name", "database"}
            })
            for asset_spec in json["assets"]
        ]

    @staticmethod
    def _get_item_assets(json: Dict[str, Any]) -> Optional[List[ItemAssetDefinition]]:
        if "item_assets" not in json:
            return None
        return [
            ItemAssetDefinition({
                k: v for k, v in item_asset_spec.items()
                if k not in {"id", "collection_name", "database"}
            })
            for item_asset_spec in json["item_assets"]
        ]

    @staticmethod
    def _get_links(json: Dict[str, Any]) -> List[Link]:
        if "links" not in json:
            return []
        return [
            Link.from_dict({
                k: v for k, v in link_spec.items()
                if k not in {"id", "collection_name", "database"}
            })
            for link_spec in json["links"]
        ]


# noinspection PyShadowingBuiltins
class Metadata:
    """
    This class stores properties for the metadata fields that are specified by the
    STAC collection specification (https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection
    -spec.md).

    Changes in result objects are never reflected in the database. Rather, there is the
    method `set_metadata_field` in the geoDB client. Using that method, changes are
    directly reflected in the database.
    """

    def __init__(
            self,
            id: str,
            title: str,
            links: List[Link],
            spatial_extent: Optional[SpatialExtent],
            temporal_extent: Optional[TemporalExtent],
            description: Optional[str] = None,
            license: Optional[str] = None,
            providers: Optional[List[Provider]] = None,
            stac_extensions: Optional[List[str]] = None,
            keywords: Optional[List[str]] = None,
            summaries: Optional[Dict[str, Summary]] = None,
            assets: Optional[List[Asset]] = None,
            item_assets: Optional[List[ItemAssetDefinition]] = None,
    ):
        self._id = id
        self._title = title
        self._links = links
        self._spatial_extent = spatial_extent
        self._temporal_extent = temporal_extent
        self._description = description if description else "No description available"
        self._stac_extensions = stac_extensions
        self._keywords = keywords if keywords else []
        self._providers = providers if providers else []
        self._license = license if license else "proprietary"
        self._summaries = summaries if summaries else {}
        self._assets = assets if assets else []
        self._item_assets = item_assets if item_assets else []

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"id={self._id}, "
            f"title={self._title}, "
            f"links={self._links} links, "
            f"spatial_extent={self._spatial_extent}, "
            f"temporal_extent={self._temporal_extent}, "
            f"description={self._description}, "
            f"license={self._license}, "
            f"providers={self._providers} providers, "
            f"keywords={self._keywords}, "
            f"assets={self._assets} assets, "
            f"item_assets={self._item_assets} item assets)"
        )

    def __str__(self) -> str:
        return "\n".join(
            [
                f"{self._title} [{self._id}]",
                f"  Description: {self._description}",
                f"  License: {self._license}",
                f"  Providers: {', '.join(p.name for p in self._providers) if self._providers else 'None'}",
                f"  Keywords: {', '.join(self._keywords) if self._keywords else 'None'}",
                f"  Links: {self._links}",
                f"  Assets: {self._assets}",
                f"  Item Assets: {self._item_assets}",
                f"  Spatial extent: {self._spatial_extent}"
                if self._spatial_extent
                else "  Spatial extent: None",
                f"  Temporal extent: {self._temporal_extent}"
                if self._temporal_extent
                else "  Temporal extent: None",
            ]
        )

    @property
    def type(self) -> str:
        return "Collection"

    @property
    def title(self) -> str:
        return self._title

    @property
    def stac_version(self) -> str:
        return "1.1.0"

    @property
    def stac_extensions(self) -> Optional[Sequence[str]]:
        return self._stac_extensions

    @property
    def id(self) -> str:
        return self._id

    @property
    def description(self) -> str:
        return self._description

    @property
    def keywords(self) -> Optional[List[str]]:
        return self._keywords

    @property
    def license(self) -> str:
        return self._license

    @property
    def providers(self) -> Optional[List[Provider]]:
        return self._providers

    @property
    def links(self) -> List[Link]:
        return self._links

    @property
    def summaries(self) -> Optional[Dict[str, List[Any] | Range | JSONSchema]]:
        return self._summaries

    @property
    def spatial_extent(self) -> SpatialExtent:
        return self._spatial_extent

    @property
    def temporal_extent(self) -> TemporalExtent:
        return self._temporal_extent

    @property
    def assets(self) -> Optional[List[Asset]]:
        return self._assets

    @property
    def item_assets(self) -> Optional[List[ItemAssetDefinition]]:
        return self._item_assets
