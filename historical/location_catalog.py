"""Canonical administrative-location catalog and dataset identity helpers."""

from __future__ import annotations

import calendar
import json
from pathlib import Path
from typing import Any, Iterable


ADMIN_SNAPSHOT = "VN_63_PRE_2025_MERGER"
DATASET_BENCHMARK_20 = "BENCHMARK_20"
DATASET_NATIONWIDE_63 = "NATIONWIDE_63"
HISTORICAL_START_YEAR = 2020
HISTORICAL_END_YEAR = 2025
CATALOG_PATH = Path(__file__).with_name("locations.json")

CENTRAL_CITIES = (
    "Hà Nội",
    "Hải Phòng",
    "Huế",
    "Đà Nẵng",
    "Thành phố Hồ Chí Minh",
    "Cần Thơ",
)

PROVINCES = (
    "An Giang",
    "Bà Rịa - Vũng Tàu",
    "Bắc Giang",
    "Bắc Kạn",
    "Bạc Liêu",
    "Bắc Ninh",
    "Bến Tre",
    "Bình Định",
    "Bình Dương",
    "Bình Phước",
    "Bình Thuận",
    "Cà Mau",
    "Cao Bằng",
    "Đắk Lắk",
    "Đắk Nông",
    "Điện Biên",
    "Đồng Nai",
    "Đồng Tháp",
    "Gia Lai",
    "Hà Giang",
    "Hà Nam",
    "Hà Tĩnh",
    "Hải Dương",
    "Hậu Giang",
    "Hòa Bình",
    "Hưng Yên",
    "Khánh Hòa",
    "Kiên Giang",
    "Kon Tum",
    "Lai Châu",
    "Lâm Đồng",
    "Lạng Sơn",
    "Lào Cai",
    "Long An",
    "Nam Định",
    "Nghệ An",
    "Ninh Bình",
    "Ninh Thuận",
    "Phú Thọ",
    "Phú Yên",
    "Quảng Bình",
    "Quảng Nam",
    "Quảng Ngãi",
    "Quảng Ninh",
    "Quảng Trị",
    "Sóc Trăng",
    "Sơn La",
    "Tây Ninh",
    "Thái Bình",
    "Thái Nguyên",
    "Thanh Hóa",
    "Tiền Giang",
    "Trà Vinh",
    "Tuyên Quang",
    "Vĩnh Long",
    "Vĩnh Phúc",
    "Yên Bái",
)

ORIGINAL_20_IDS = (
    "VN_HCM",
    "VN_HANOI",
    "VN_DANANG",
    "VN_CANTHO",
    "VN_HUE",
    "VN_HAIPHONG",
    "VN_NHATRANG",
    "VN_DALAT",
    "VN_VUNGTAU",
    "VN_QUYNHON",
    "VN_BMT",
    "VN_PLEIKU",
    "VN_PHANTHIET",
    "VN_VINH",
    "VN_THANHHOA",
    "VN_HALONG",
    "VN_BIENHOA",
    "VN_LONGXUYEN",
    "VN_RACHGIA",
    "VN_CAMAU",
)


def expected_hours(year: int) -> int:
    """Return the number of UTC hourly observations in a calendar year."""
    return 8784 if calendar.isleap(year) else 8760


def expected_records(location_count: int, years: Iterable[int] | None = None) -> int:
    """Return expected hourly rows without duplicating year-count constants."""
    if location_count < 0:
        raise ValueError("location_count must be non-negative")
    selected_years = range(
        HISTORICAL_START_YEAR,
        HISTORICAL_END_YEAR + 1,
    ) if years is None else years
    return location_count * sum(expected_hours(year) for year in selected_years)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_catalog(locations: list[dict[str, Any]]) -> dict[str, Any]:
    """Validate the frozen 63-unit catalog and return a JSON-safe summary."""
    errors: list[str] = []
    ids = [item.get("location_id") for item in locations]
    names = [item.get("province_name") for item in locations]
    coordinate_pairs: dict[tuple[float, float], list[str]] = {}

    if len(locations) != 63:
        errors.append(f"expected 63 catalog records, found {len(locations)}")
    if len(set(ids)) != len(ids):
        errors.append("location_id values are not unique")
    if len(set(names)) != len(names):
        errors.append("province_name values are not unique")

    expected_names = set(CENTRAL_CITIES) | set(PROVINCES)
    if set(names) != expected_names:
        missing = sorted(expected_names - set(names))
        unexpected = sorted(set(names) - expected_names)
        errors.append(f"canonical names differ; missing={missing}; unexpected={unexpected}")

    central_city_count = sum(item.get("admin_type") == "CENTRAL_CITY" for item in locations)
    province_count = sum(item.get("admin_type") == "PROVINCE" for item in locations)
    if central_city_count != 6:
        errors.append(f"expected 6 CENTRAL_CITY records, found {central_city_count}")
    if province_count != 57:
        errors.append(f"expected 57 PROVINCE records, found {province_count}")

    for index, item in enumerate(locations):
        prefix = f"record[{index}]"
        required = (
            "location_id",
            "province_name",
            "province_name_ascii",
            "admin_type",
            "representative_place",
            "latitude",
            "longitude",
            "country_code",
            "timezone",
            "admin_snapshot",
            "aliases",
            "dataset_memberships",
        )
        for field in required:
            if field not in item or item[field] is None:
                errors.append(f"{prefix} missing {field}")
        if not isinstance(item.get("location_id"), str) or not item["location_id"].startswith("VN_"):
            errors.append(f"{prefix} location_id must use the VN_ ASCII identifier convention")
        if not item.get("representative_place"):
            errors.append(f"{prefix} representative_place is empty")
        if not item.get("province_name_ascii"):
            errors.append(f"{prefix} province_name_ascii is empty")
        if item.get("admin_type") not in {"PROVINCE", "CENTRAL_CITY"}:
            errors.append(f"{prefix} has invalid admin_type {item.get('admin_type')!r}")
        if item.get("country_code") != "VN":
            errors.append(f"{prefix} country_code must be VN")
        if item.get("timezone") != "UTC":
            errors.append(f"{prefix} timezone must be UTC")
        if item.get("admin_snapshot") != ADMIN_SNAPSHOT:
            errors.append(f"{prefix} admin_snapshot must be {ADMIN_SNAPSHOT}")
        if not isinstance(item.get("aliases"), list):
            errors.append(f"{prefix} aliases must be a list")
        if not isinstance(item.get("dataset_memberships"), list):
            errors.append(f"{prefix} dataset_memberships must be a list")
        if item.get("admin_type") == "CENTRAL_CITY" and item.get("province_name") not in CENTRAL_CITIES:
            errors.append(f"{prefix} CENTRAL_CITY does not match the frozen name list")
        if item.get("admin_type") == "PROVINCE" and item.get("province_name") not in PROVINCES:
            errors.append(f"{prefix} PROVINCE does not match the frozen name list")

        latitude = item.get("latitude")
        longitude = item.get("longitude")
        if not _is_number(latitude) or not _is_number(longitude):
            errors.append(f"{prefix} coordinates must be numeric and non-null")
            continue
        if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
            errors.append(f"{prefix} coordinates are outside WGS84 bounds")
        # Broad sanity range for the mainland administrative seats represented here.
        if not 8 <= latitude <= 24.5 or not 102 <= longitude <= 110:
            errors.append(f"{prefix} coordinate is outside the broad Vietnam seat envelope")
        coordinate_pairs.setdefault((float(latitude), float(longitude)), []).append(
            str(item.get("location_id"))
        )

    duplicated_coordinates = {
        f"{lat:.5f},{lon:.5f}": duplicate_ids
        for (lat, lon), duplicate_ids in coordinate_pairs.items()
        if len(duplicate_ids) > 1
    }
    if duplicated_coordinates:
        errors.append(f"duplicate coordinate pairs: {duplicated_coordinates}")

    catalog_ids = set(ids)
    missing_original = sorted(set(ORIGINAL_20_IDS) - catalog_ids)
    if missing_original:
        errors.append(f"original location IDs missing: {missing_original}")

    benchmark_ids = {
        item.get("location_id")
        for item in locations
        if isinstance(item.get("dataset_memberships"), list)
        and DATASET_BENCHMARK_20 in item["dataset_memberships"]
    }
    if benchmark_ids != set(ORIGINAL_20_IDS):
        errors.append(
            "BENCHMARK_20 membership must contain exactly the preserved original 20 IDs"
        )
    invalid_nationwide = [
        item.get("location_id")
        for item in locations
        if not isinstance(item.get("dataset_memberships"), list)
        or DATASET_NATIONWIDE_63 not in item["dataset_memberships"]
    ]
    if invalid_nationwide:
        errors.append(f"records missing NATIONWIDE_63 membership: {invalid_nationwide}")

    return {
        "status": "PASS" if not errors else "FAIL",
        "admin_snapshot": ADMIN_SNAPSHOT,
        "catalog_count": len(locations),
        "unique_location_ids": len(set(ids)),
        "unique_canonical_names": len(set(names)),
        "central_city_count": central_city_count,
        "province_count": province_count,
        "original_20_preserved": len(ORIGINAL_20_IDS) - len(missing_original),
        "new_location_count": len(set(ids) - set(ORIGINAL_20_IDS)),
        "duplicate_coordinate_pairs": duplicated_coordinates,
        "errors": errors,
    }


def load_catalog(path: str | Path = CATALOG_PATH) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as catalog_file:
        locations = json.load(catalog_file)
    if not isinstance(locations, list):
        raise ValueError("location catalog must be a JSON array")
    report = validate_catalog(locations)
    if report["status"] != "PASS":
        raise ValueError("invalid location catalog: " + "; ".join(report["errors"]))
    return locations


def select_dataset_locations(
    locations: list[dict[str, Any]],
    dataset: str,
) -> list[dict[str, Any]]:
    normalized = dataset.strip().upper().replace("-", "_")
    aliases = {
        "BENCHMARK_20": DATASET_BENCHMARK_20,
        "NATIONWIDE_63": DATASET_NATIONWIDE_63,
    }
    try:
        dataset_id = aliases[normalized]
    except KeyError as exc:
        raise ValueError("dataset must be benchmark-20 or nationwide-63") from exc
    selected = [
        item for item in locations
        if dataset_id in item.get("dataset_memberships", [])
    ]
    expected_count = 20 if dataset_id == DATASET_BENCHMARK_20 else 63
    if len(selected) != expected_count:
        raise ValueError(
            f"{dataset_id} expected {expected_count} locations, found {len(selected)}"
        )
    return selected
