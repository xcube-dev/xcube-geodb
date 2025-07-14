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
from typing import List, Optional, Dict, Any, Union, TypeAlias, Literal


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
Relation = Literal["self", "root", "parent", "child", "collection", "item"]
valid_roles = ["licensor", "producer", "processor", "host"]


class Provider:
    def __init__(
        self, name: str, description: str = "", roles: List[str] = None, url: str = None
    ):
        self._name = name
        self._description = description
        self._roles = roles if roles else []
        if not set(self._roles).issubset(valid_roles):
            raise ValueError(
                f"Invalid set of roles provided: {roles}; valid roles are: {valid_roles}."
            )
        self._url = url

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def roles(self) -> List[str]:
        return self._roles

    @property
    def url(self) -> str:
        return self._url

    @description.setter
    def description(self, value):
        self._description = value

    @url.setter
    def url(self, value):
        self._url = value

    @roles.setter
    def roles(self, value):
        self._roles = value

    @staticmethod
    def from_json(provider_spec: Dict[str, Union[str, List[str]]]):
        p = Provider(
            provider_spec["name"],
        )
        if "description" in provider_spec:
            p.description = provider_spec["description"]

        if "url" in provider_spec:
            p.url = provider_spec["url"]

        if "roles" in provider_spec:
            p.roles = provider_spec["roles"]

        return p


# noinspection PyShadowingBuiltins
class Link:
    def __init__(
        self,
        href: str,
        rel: Relation,
        type: Optional[str],
        title: Optional[str],
        method: Optional[str],
        headers: Optional[Dict[str, Union[str, List[str]]]],
        body: Optional[Any],
    ):
        self._href = href
        self._rel = rel
        self._type = type
        self._title = title
        self._method = method
        self._headers = headers
        self._body = body

    @staticmethod
    def from_json(link_spec):
        return Link(
            link_spec["href"],
            link_spec["rel"],
            link_spec["type"] if "type" in link_spec else None,
            link_spec["title"] if "title" in link_spec else None,
            link_spec["method"] if "method" in link_spec else None,
            link_spec["headers"] if "headers" in link_spec else None,
            link_spec["body"] if "body" in link_spec else None,
        )

    @property
    def href(self) -> str:
        return self._href

    @property
    def rel(self) -> Relation:
        return self._rel

    @property
    def type(self) -> Optional[str]:
        return self._type

    @property
    def title(self) -> Optional[str]:
        return self._title

    @property
    def method(self) -> Optional[str]:
        return self._method

    @property
    def body(self) -> Optional[Any]:
        return self._body

    @property
    def headers(self) -> Optional[Dict[str, Union[str, List[str]]]]:
        return self._headers


# noinspection PyShadowingBuiltins
class Asset:
    def __init__(
        self,
        href: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        type: Optional[str] = None,
        roles: Optional[List[str]] = None,
    ):
        self._href = href
        self._title = title
        self._description = description
        self._type = type
        self._roles = roles

    @property
    def href(self) -> str:
        return self._href

    @property
    def title(self) -> Optional[str]:
        if self._title:
            return self._title
        return None

    @property
    def description(self) -> Optional[str]:
        if self._description:
            return self._description
        return None

    @description.setter
    def description(self, value):
        self._description = value

    @property
    def type(self) -> Optional[str]:
        if self._type:
            return self._type
        return None

    @property
    def roles(self) -> Optional[List[str]]:
        if self._roles:
            return self._roles
        return None

    @staticmethod
    def from_json(asset_spec: Dict[str, Union[str, List[str]]]):
        asset = Asset(
            asset_spec["href"],
        )
        if "description" in asset_spec:
            asset.description = asset_spec["description"]
        if "title" in asset_spec:
            asset.title = asset_spec["title"]
        if "type" in asset_spec:
            asset.type = asset_spec["type"]
        if "roles" in asset_spec:
            asset.roles = asset_spec["roles"]

        return asset

    @type.setter
    def type(self, value):
        self._type = value

    @roles.setter
    def roles(self, value):
        self._roles = value

    @title.setter
    def title(self, value):
        self._title = value


# noinspection PyShadowingBuiltins
class ItemAsset:
    def __init__(
        self,
        title: Optional[str],
        description: Optional[str],
        type: Optional[str],
        roles: Optional[List[str]],
    ):
        self._title = title
        self._description = description
        self._type = type
        self._roles = roles


# noinspection PyShadowingBuiltins
class Metadata:
    """
    General idea: store properties for the metadata fields that are specified by the
    STAC collection specification (https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection
    -spec.md).

    This class is intended not to contain any logic, rather, its purpose is to reflect the
    database state.


    Disregard this for this class:
    Also, allow for generic extra fields:
        - set_binary_metadata(field_name, blob)
        - set_string_metadata(field_name, string)
        - set_number_metadata(field_name, number)
        - set_time_metadata(field_name, datetime)
        - set_object_metadata(field_name, Anything convertible to JSON)

    Some metadata fields can be extracted automatically (see
    xcube_geodb_openeo.core.geodb_datasource.GeoDBVectorSource.get_metadata), however,
    if they have been deliberately set using one of the set_methods, that value takes
    precedence.
    """

    def __init__(
        self,
        id: str,
        title: str,
        links: List[Link],
        spatial_extent: Optional[SpatialExtent] = None,
        temporal_extent: Optional[TemporalExtent] = None,
        description: str = "No description available",
        license: str = "proprietary",
        providers: Optional[List[Provider]] = None,
        stac_extensions: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        summaries: Optional[Dict[str, Summary]] = None,
        assets: Optional[Dict[str, Asset]] = None,
        item_assets: Optional[Dict[str, ItemAsset]] = None,
    ):
        if spatial_extent is None:
            spatial_extent = [[-180.0, -90.0, 180.0, 90.0]]
        if temporal_extent is None:
            temporal_extent = [[None, None]]
        self._id = id
        self._title = title
        self._links = links
        self._spatial_extent = spatial_extent
        self._temporal_extent = temporal_extent
        self._description = description
        self._stac_extensions = stac_extensions
        self._keywords = keywords if keywords else []
        self._providers = providers if providers else []
        self._license = license
        self._summaries = summaries if summaries else {}
        self._assets = assets if assets else {}
        self._item_assets = item_assets if item_assets else {}

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
    def stac_extensions(self) -> Optional[List[str]]:
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
    def assets(self) -> Optional[Dict[str, Asset]]:
        return self._assets

    @property
    def item_assets(self) -> Optional[Dict[str, ItemAsset]]:
        return self._item_assets

    @staticmethod
    def _get_providers(json: Dict[str, Any]) -> Optional[List[Provider]]:
        if "providers" not in json:
            return None
        result = []
        for provider_spec in json["providers"]:
            result.append(Provider.from_json(provider_spec))
        return result

    @staticmethod
    def _get_assets(json: Dict[str, Any]) -> Optional[Dict[str, Asset]]:
        if "assets" not in json:
            return None
        result: dict[str, Asset] = {}
        for name, asset_spec in json["assets"].items():
            result[name] = Asset.from_json(asset_spec)
        return result

    @staticmethod
    def _get_links(json: Dict[str, Any]) -> List[Link]:
        if "links" not in json:
            return []
        result = []
        for link_spec in json["links"]:
            result.append(Link.from_json(link_spec))
        return result

    @staticmethod
    def from_json(json: Dict[str, Any]):
        providers = Metadata._get_providers(json)
        assets = Metadata._get_assets(json)
        links = Metadata._get_links(json)
        summaries = json["summaries"] if "summaries" in json else {}
        stac_extensions = json["stac_extensions"] if "stac_extensions" in json else []
        title = json["title"] if "title" in json else None
        keywords = json["keywords"] if "keywords" in json else None
        return Metadata(
            id=json["id"],
            title=title,
            links=links,
            spatial_extent=json["spatial_extent"],
            temporal_extent=json["temporal_extent"],
            description=json["description"],
            license=json["license"],
            providers=providers,
            stac_extensions=stac_extensions,
            keywords=keywords,
            summaries=summaries,
            assets=assets,
        )
