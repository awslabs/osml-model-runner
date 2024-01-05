"""
This script generates a KML representation of an image, the terrain, and any annotations provided.

python3 visualize_sensor_model.py -i ./foo.ntf -a ./annotations.xml -er ./SRTM -ev 1arc_v3

Annotations are expected in a common XML format:

    <annotation>
        <object>
            <name>building</name>
            <pose>Unspecified</pose>
            <truncated>0</truncated>
            <difficult>0</difficult>
            <bndbox>
                <xmin>694</xmin>
                <ymin>1125</ymin>
                <xmax>762</xmax>
                <ymax>1183</ymax>
            </bndbox>
        </object>
"""

import argparse
import re
import os
from itertools import chain
from math import degrees
from typing import List

from defusedxml import lxml as dlxml
from lxml import etree

from aws_oversightml_model_runner.gdal.gdal_dem_tile_factory import (
    GDALDigitalElevationModelTileFactory,
)
from aws_oversightml_model_runner.gdal.gdal_utils import load_gdal_dataset
from aws_oversightml_model_runner.photogrammetry import (
    ConstantElevationModel,
    DigitalElevationModel,
    GeodeticWorldCoordinate,
    ImageCoordinate,
    SRTMTileSet,
)

KML_NSMAP = {None: "http://www.opengis.net/kml/2.2"}


# NOTE: There are open source libraries we can use that would help with this kml but be careful fo the license.
#       A few of the more common ones had licenses that were not allowed by our open source policy.
def style_element(
    style_id: str, line_color: str, line_width: int, poly_fill: int = 0, poly_outline: int = 1
) -> etree.Element:
    style = etree.Element("Style")
    style.attrib["id"] = style_id

    line_style = etree.Element("LineStyle")
    line_style.append(text_element("color", line_color))
    line_style.append(text_element("width", str(line_width)))
    style.append(line_style)

    poly_style = etree.Element("PolyStyle")
    poly_style.append(text_element("fill", str(poly_fill)))
    poly_style.append(text_element("outline", str(poly_outline)))
    style.append(poly_style)
    return style


def coordinates_element(geodetic_coordinates: List[GeodeticWorldCoordinate]) -> etree.Element:
    coordinates = etree.Element("coordinates")
    text = ""
    for geo_coord in geodetic_coordinates:
        text += f"{degrees(geo_coord.coordinate[0])},{degrees(geo_coord.coordinate[1])},{geo_coord.coordinate[2]} \n"
    coordinates.text = text
    return coordinates


def text_element(element_name: str, text: str) -> etree.Element:
    # Blacklist characters that have special significance in XML syntax
    # and so could be used in an XML injection attack.
    # Note that we only accept XML input from trusted sources (annotations
    # of images given from USG-approved providers) but this provides an additional
    # layer of security on top of that.
    disallowed_pattern = re.compile(r"[<>'&\"]")
    if disallowed_pattern.search(text):
        raise ValueError(f"Disallowed character in text: {text}") 
    element = etree.Element(element_name)
    element.text = text
    return element


def linear_ring_element(geodetic_coordinates: List[GeodeticWorldCoordinate]) -> etree.Element:
    linear_ring = etree.Element("LinearRing")
    linear_ring.append(coordinates_element(geodetic_coordinates))
    return linear_ring


def polygon_element(
    outer_boundary: List[GeodeticWorldCoordinate], altitude_mode: str = "absolute"
) -> etree.Element:
    outer_boundry_is = etree.Element("outerBoundaryIs")
    outer_boundry_is.append(linear_ring_element(outer_boundary))

    polygon = etree.Element("Polygon")

    polygon.append(text_element("altitudeMode", altitude_mode))
    polygon.append(text_element("tessellate", "1"))
    polygon.append(outer_boundry_is)
    return polygon


def line_string_element(
    geodetic_coordinates: List[GeodeticWorldCoordinate], altitude_mode: str = "absolute"
) -> etree.Element:
    line_string = etree.Element("LineString")
    line_string.append(coordinates_element(geodetic_coordinates))
    line_string.append(text_element("altitudeMode", altitude_mode))
    return line_string


def annotation_placemark(
    object_type: str, geo_coordinates: List[GeodeticWorldCoordinate], style_url: str = None
) -> etree.Element:
    placemark = etree.Element("Placemark")
    placemark.append(text_element("name", object_type))
    placemark.append(polygon_element(geo_coordinates))
    if style_url:
        placemark.append(text_element("styleUrl", style_url))
    return placemark


def create_kml_terrain_grid(
    ds, sensor_model, elevation_model=None, style_url: str = None
) -> etree.Element:
    terrain_grid = etree.Element("Folder")
    terrain_grid.append(text_element("name", "Terrain Grid"))

    geo_locations = []
    for row in chain(range(0, ds.RasterYSize, 50), [ds.RasterYSize]):
        geo_locations_row = []
        for column in chain(range(0, ds.RasterXSize, 50), [ds.RasterXSize]):
            geo_locations_row.append(
                sensor_model.image_to_world(
                    ImageCoordinate([column, row]), elevation_model=elevation_model
                )
            )
        geo_locations.append(geo_locations_row)

    for row in range(0, len(geo_locations) - 1):
        for col in range(0, len(geo_locations[0]) - 1):
            placemark = etree.Element("Placemark")
            center_lon = (
                geo_locations[row][col].longitude + geo_locations[row + 1][col + 1].longitude
            ) / 2.0
            center_lat = (
                geo_locations[row][col].latitude + geo_locations[row + 1][col + 1].latitude
            ) / 2.0
            placemark.append(
                text_element("name", f"{degrees(center_lat):0.6f}, {degrees(center_lon):0.6f}")
            )
            placemark.append(
                polygon_element(
                    [
                        geo_locations[row][col],
                        geo_locations[row + 1][col],
                        geo_locations[row + 1][col + 1],
                        geo_locations[row][col + 1],
                        geo_locations[row][col],
                    ]
                )
            )
            if style_url:
                placemark.append(text_element("styleUrl", style_url))
            terrain_grid.append(placemark)
    return terrain_grid


def create_kml_image_footprint(
    ds, sensor_model, elevation_model=None, style_url: str = None
) -> etree.Element:
    image_corners = [
        [0, 0],
        [0, ds.RasterYSize],
        [ds.RasterXSize, ds.RasterYSize],
        [ds.RasterXSize, 0],
    ]
    image_corner_coordinates = [
        sensor_model.image_to_world(ImageCoordinate(corner), elevation_model=elevation_model)
        for corner in image_corners
    ]
    image_corner_coordinates.append(image_corner_coordinates[0])

    placemark = etree.Element("Placemark")
    placemark.append(text_element("name", "Image Footprint"))
    placemark.append(polygon_element(image_corner_coordinates))
    if style_url:
        placemark.append(text_element("styleUrl", style_url))
    return placemark


def create_kml_image_opticalaxis(ds, sensor_model, style_url: str = None) -> etree.Element:
    image_center = [ds.RasterXSize / 2.0, ds.RasterYSize / 2.0]
    image_center_geo = sensor_model.image_to_world(ImageCoordinate(image_center))
    constant_elevation = ConstantElevationModel(image_center_geo.elevation + 1000)
    elevated_geo = sensor_model.image_to_world(
        ImageCoordinate(image_center), elevation_model=constant_elevation
    )

    placemark = etree.Element("Placemark")
    placemark.append(text_element("name", "Optical Axis"))
    placemark.append(line_string_element([image_center_geo, elevated_geo]))
    if style_url:
        placemark.append(text_element("styleUrl", style_url))
    return placemark


def create_kml_annotations(annotations_file_name, dem, sensor_model):
    print(f"Georeferencing annotations from {annotations_file_name}")
    with open(annotations_file_name, "rb") as annotations_file:
        annotations = dlxml.parse(annotations_file).getroot()

    annotations_folder = etree.Element("Folder")
    annotations_folder.append(text_element("name", "Annotations"))
    for obj_annotation in annotations.xpath("object"):
        obj_type = obj_annotation.xpath("name")[0].text
        bndbox_element = obj_annotation.xpath("bndbox")[0]
        xmin = int(bndbox_element.xpath("xmin")[0].text)
        ymin = int(bndbox_element.xpath("ymin")[0].text)
        xmax = int(bndbox_element.xpath("xmax")[0].text)
        ymax = int(bndbox_element.xpath("ymax")[0].text)

        image_coordinates = [
            ImageCoordinate([xmin, ymin]),
            ImageCoordinate([xmin, ymax]),
            ImageCoordinate([xmax, ymax]),
            ImageCoordinate([xmax, ymin]),
        ]
        geo_coordinates = [
            sensor_model.image_to_world(image_coord, elevation_model=dem)
            for image_coord in image_coordinates
        ]
        geo_coordinates.append(geo_coordinates[0])

        annotations_folder.append(
            annotation_placemark(obj_type, geo_coordinates, style_url="#AnnotationStyle")
        )
    return annotations_folder


def run():
    # Open the input image and get the sensor model
    image_path = args.image
    image_name = os.path.basename(image_path)
    ds, sensor_model = load_gdal_dataset(image_path)
    print(f"Opening {image_path}")
    print(
        f"  Format: {ds.GetDriver().ShortName} Size: {ds.RasterXSize}x{ds.RasterYSize} Num Bands: {ds.RasterCount}"
    )
    print(f"  Sensor Model Type: {sensor_model.__class__.__name__}")

    # Load the DEM if available
    dem = None
    if args.elevation_root and args.elevation_version:
        print(f"Using digital elevation model tiles from: {args.elevation_root}")
        dem = DigitalElevationModel(
            SRTMTileSet(version=args.elevation_version),
            GDALDigitalElevationModelTileFactory(args.elevation_root),
        )

    # Create the basic kml document and styles that will be used for each feature
    kml = etree.Element("kml", nsmap=KML_NSMAP)
    kml_doc = etree.Element("Document")
    kml.append(kml_doc)
    kml_doc.append(style_element("FootprintStyle", "ffff00ff", 2))
    kml_doc.append(style_element("AnnotationStyle", "ff0000ff", 2))
    kml_doc.append(style_element("OpticalAxisStyle", "ff00ff00", 2))
    kml_doc.append(style_element("TerrainStyle", "ffffffff", 1))
    image_folder = etree.Element("Folder")
    kml_doc.append(image_folder)
    image_folder.append(text_element("name", image_name))

    # Create visualizations of the various image features, terrain, and annotations
    image_folder.append(
        create_kml_image_footprint(
            ds, sensor_model, elevation_model=dem, style_url="#FootprintStyle"
        )
    )
    image_folder.append(
        create_kml_image_opticalaxis(ds, sensor_model, style_url="#OpticalAxisStyle")
    )
    image_folder.append(
        create_kml_terrain_grid(ds, sensor_model, elevation_model=dem, style_url="#TerrainStyle")
    )
    image_folder.append(create_kml_annotations(args.annotations, dem, sensor_model))

    # Save the KML file
    kml_file_name = f"{image_name}-sensor.kml"
    print(f"Writing results to {kml_file_name}")
    with open(kml_file_name, "wb") as kml_file:
        kml_file.write(etree.tostring(kml, pretty_print=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--image",
        default="/JITC-Test-NITF/20210610-QuickLook-Final/Segments/Test Files/NITF_SYM_POS_05.ntf",
    )
    parser.add_argument("-a", "--annotations", default="/work/NITF_SYM_POS_05.xml")
    parser.add_argument("-er", "--elevation-root", default="/work/SRTM")
    parser.add_argument("-ev", "--elevation-version", default="1arc_v3")
    args = parser.parse_args()

    run()
