import dataclasses

from pydantic import AliasPath, BaseModel, Field


class OsmQueryInfo(BaseModel):
    attribution: str
    licence: str
    query: str
    version: str


class OsmGeocodeData(BaseModel):
    name: str | None
    housenumber: str | None
    street: str | None
    postcode: str | None
    city: str | None
    country: str | None
    country_code: str | None
    locality: str | None
    district: str | None
    osm_id: int
    osm_type: str
    # unused fields excluded
    admin: dict[str, str] | None = Field(exclude=True)
    label: str | None = Field(exclude=True)
    place_id: int | None = Field(exclude=True)
    osm_key: str = Field(exclude=True)
    osm_value: str = Field(exclude=True)
    type: str | None = Field(exclude=True)


class OsmFeature(BaseModel):
    coordinates: tuple[float, float] = Field(validation_alias=AliasPath("geometry", "coordinates"))
    geocoding: OsmGeocodeData = Field(validation_alias=AliasPath("properties", "geocoding"))
    type: str


@dataclasses.dataclass(frozen=True, slots=True)
class OsmAddress:
    longitude: float
    latitude: float
    name: str | None
    housenumber: str | None
    street: str | None
    postcode: str | None
    city: str | None
    country: str | None
    country_code: str | None
    locality: str | None
    district: str | None
    osm_id: int
    osm_type: str


class OsmResponse(BaseModel):
    features: list[OsmFeature]
    geocoding: OsmQueryInfo
    type: str

    def has_results(self) -> bool:
        return bool(self.features)

    def extract_address(self) -> OsmAddress | None:
        if not self.has_results():
            return None
        feature = self.features[0]
        data: dict = {
            "longitude": feature.coordinates[0],
            "latitude": feature.coordinates[1],
            **feature.geocoding.model_dump(),
        }
        return OsmAddress(**data)


### Example response from OSM Nominatim API
# {
#     "features": [
#         {
#             "geometry": {"coordinates": [13.3262522, 52.5098522], "type": "Point"},
#             "properties": {
#                 "geocoding": {
#                     "admin": {
#                         "level10": "Charlottenburg",
#                         "level4": "Berlin",
#                         "level9": "Charlottenburg-Wilmersdorf",
#                     },
#                     "city": "Berlin",
#                     "country": "Deutschland",
#                     "country_code": "de",
#                     "district": "Charlottenburg",
#                     "housenumber": "34",
#                     "label": "Mensa TU Hardenbergstraße, 34, Hardenbergstraße, City West, Charlottenburg, Charlottenburg-Wilmersdorf, Berlin, 10623, Deutschland",
#                     "name": "Mensa TU Hardenbergstraße",
#                     "locality": "City West",
#                     "osm_id": 10742899891,
#                     "osm_key": "amenity",
#                     "osm_type": "node",
#                     "osm_value": "fast_food",
#                     "place_id": 134511793,
#                     "postcode": "10623",
#                     "street": "Hardenbergstraße",
#                     "type": "house",
#                 }
#             },
#             "type": "Feature",
#         }
#     ],
#     "geocoding": {
#         "attribution": "Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright",
#         "licence": "ODbL",
#         "query": "Mensa, Hardenbergstraße 34, Berlin, 10623, Germany",
#         "version": "0.1.0",
#     },
#     "type": "FeatureCollection",
# }
