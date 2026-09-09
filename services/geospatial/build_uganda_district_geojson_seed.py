#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GEOJSON_PATH = (
    PROJECT_ROOT
    / "data"
    / "geospatial"
    / "uganda_superset.geojson"
)

ISO_SEED_PATH = (
    PROJECT_ROOT
    / "transform"
    / "dbt"
    / "seeds"
    / "uganda_superset_district_iso.csv"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "transform"
    / "dbt"
    / "seeds"
    / "uganda_district_geojson.csv"
)


def normalise(value: str) -> str:
    return (
        value.strip()
        .lower()
        .replace("-", " ")
        .replace("_", " ")
    )


def load_iso_reference() -> dict[str, dict[str, str]]:
    rows = {}

    with ISO_SEED_PATH.open(
        encoding="utf-8",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        for row in reader:
            district = row["district"].strip()

            code = row["superset_district_iso"].strip()
            if code in rows:
                raise ValueError(f"Duplicate ISO code in seed: {code}")

            rows[code] = {
                "district": district,
                "superset_district_iso": code,
            }

    return rows


def main() -> None:
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(GEOJSON_PATH)

    if not ISO_SEED_PATH.exists():
        raise FileNotFoundError(ISO_SEED_PATH)

    with GEOJSON_PATH.open(
        encoding="utf-8",
    ) as handle:
        geojson = json.load(handle)

    if geojson.get("type") != "FeatureCollection":
        raise ValueError(
            "Expected a GeoJSON FeatureCollection."
        )

    iso_reference = load_iso_reference()

    geometries: dict[str, list[dict]] = {}
    unmatched_geometry = []

    for feature in geojson.get("features", []):
        properties = feature.get(
            "properties",
            {},
        )

        district_name = properties.get("NAME_1")
        district_iso = properties.get("ISO")

        if not district_name or not district_iso:
            raise ValueError(
                "GeoJSON feature is missing properties.NAME_1 or properties.ISO"
            )

        reference = iso_reference.get(district_iso)

        if reference is None:
            unmatched_geometry.append(f"{district_name} ({district_iso})")
            continue

        if normalise(district_name) != normalise(reference["district"]):
            raise ValueError(
                f"District name mismatch for {district_iso}: "
                f"{district_name!r} != {reference['district']!r}"
            )

        geometry = feature.get("geometry")

        if not geometry:
            raise ValueError(
                f"{district_name} has no geometry"
            )

        geometries.setdefault(district_iso, []).append(geometry)

    unmatched_iso = [
        reference["district"]
        for code, reference in iso_reference.items()
        if code not in geometries
    ]

    if unmatched_iso:
        raise ValueError(
            "ISO districts without geometry: " + ", ".join(sorted(unmatched_iso))
        )

    matched = []
    for code, reference in iso_reference.items():
        district_geometries = geometries[code]
        if len(district_geometries) == 1:
            geometry = district_geometries[0]
        else:
            polygons = []
            for item in district_geometries:
                if item["type"] == "Polygon":
                    polygons.append(item["coordinates"])
                elif item["type"] == "MultiPolygon":
                    polygons.extend(item["coordinates"])
                else:
                    raise ValueError(
                        f"Unsupported geometry type for {code}: {item['type']}"
                    )
            geometry = {"type": "MultiPolygon", "coordinates": polygons}

        matched.append(
            {
                "district": reference["district"],
                "superset_district_iso": code,
                "geojson_geometry": json.dumps(
                    geometry,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ),
            }
        )

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_PATH.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "district",
                "superset_district_iso",
                "geojson_geometry",
            ],
        )

        writer.writeheader()
        writer.writerows(
            sorted(
                matched,
                key=lambda row: row["district"],
            )
        )

    print()
    print("Uganda Superset district GeoJSON preparation")
    print("=" * 60)
    print(
        f"GeoJSON features : "
        f"{len(geojson.get('features', []))}"
    )
    print(
        f"ISO districts    : "
        f"{len(iso_reference)}"
    )
    print(
        f"Matched          : "
        f"{len(matched)}"
    )
    print(
        f"Unmatched GeoJSON: "
        f"{len(unmatched_geometry)}"
    )
    print(
        f"Unmatched ISO    : "
        f"{len(unmatched_iso)}"
    )

    if unmatched_geometry:
        print()
        print("GeoJSON names not found in ISO seed:")
        for name in sorted(
            unmatched_geometry
        ):
            print(f"  - {name}")

    if unmatched_iso:
        print()
        print("ISO districts without geometry:")
        for name in sorted(
            unmatched_iso
        ):
            print(f"  - {name}")

    print()
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
