from __future__ import annotations

from pathlib import Path
from typing import Any

from .io import read_json_file, utc_now_iso
from .schema import (
    CompanyData,
    CommodityData,
    DeforestationAssessment,
    NA_VALUE,
    PlotReference,
    ReportV1,
    na_if_missing,
)

try:
    from pyproj import Geod
    from shapely.geometry import shape

    _HAS_GEO = True
except Exception:
    _HAS_GEO = False


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _properties_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _collect_geojson_properties(aoi_json: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if aoi_json.get("type") == "FeatureCollection":
        features = aoi_json.get("features")
        if isinstance(features, list):
            for feature in features:
                props = _properties_dict((feature or {}).get("properties"))
                for key, value in props.items():
                    if key not in out and value is not None and (not isinstance(value, str) or value.strip()):
                        out[key] = value
    elif aoi_json.get("type") == "Feature":
        out.update(_properties_dict(aoi_json.get("properties")))
    return out


def _polygon_count(geometry: dict[str, Any]) -> int:
    geo_type = geometry.get("type")
    coords = geometry.get("coordinates") or []
    if geo_type == "Polygon":
        return 1
    if geo_type == "MultiPolygon":
        return len(coords)
    return 0


def _iter_outer_rings(geometry: dict[str, Any]) -> list[list[list[float]]]:
    geo_type = geometry.get("type")
    coords = geometry.get("coordinates") or []
    if geo_type == "Polygon":
        if not coords:
            return []
        return [coords[0]]
    if geo_type == "MultiPolygon":
        rings: list[list[list[float]]] = []
        for poly in coords:
            if poly:
                rings.append(poly[0])
        return rings
    return []


def _vertex_pairs(geometry: dict[str, Any]) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for ring in _iter_outer_rings(geometry):
        points = [(float(p[0]), float(p[1])) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
        if len(points) >= 2 and points[0] == points[-1]:
            points = points[:-1]
        out.extend(points)
    return out


def _centroid_avg_vertices(geometry: dict[str, Any]) -> tuple[float, float]:
    vertices = _vertex_pairs(geometry)
    if not vertices:
        return 0.0, 0.0
    lon_avg = sum(p[0] for p in vertices) / len(vertices)
    lat_avg = sum(p[1] for p in vertices) / len(vertices)
    return lat_avg, lon_avg


def _approx_area_ha(geometry: dict[str, Any]) -> float:
    import math

    rings = _iter_outer_rings(geometry)
    if not rings:
        return 0.0

    area_m2_total = 0.0
    for ring in rings:
        points = [(float(p[0]), float(p[1])) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
        if len(points) < 3:
            continue
        if points[0] != points[-1]:
            points.append(points[0])
        lat0 = sum(pt[1] for pt in points[:-1]) / max(1, len(points) - 1)
        m_per_deg_lat = 110_574.0
        m_per_deg_lon = 111_320.0 * math.cos(math.radians(lat0))

        xy = [(lon * m_per_deg_lon, lat * m_per_deg_lat) for lon, lat in points]
        shoelace = 0.0
        for i in range(len(xy) - 1):
            shoelace += (xy[i][0] * xy[i + 1][1]) - (xy[i + 1][0] * xy[i][1])
        area_m2_total += abs(shoelace) / 2.0

    return area_m2_total / 10_000.0


def _area_ha(geometry: dict[str, Any]) -> tuple[float, str]:
    if _HAS_GEO:
        geod = Geod(ellps="WGS84")
        geom = shape(geometry)
        area_m2, _ = geod.geometry_area_perimeter(geom)
        return abs(float(area_m2)) / 10_000.0, "geodesic_wgs84_pyproj"
    return _approx_area_ha(geometry), "approx_equirectangular_shoelace"


def _build_plots(aoi_path: Path, aoi_json: dict[str, Any]) -> list[PlotReference]:
    features = aoi_json.get("features") if aoi_json.get("type") == "FeatureCollection" else None
    feature_list = features if isinstance(features, list) else [{"type": "Feature", "properties": {}, "geometry": aoi_json}]

    plots: list[PlotReference] = []
    for idx, feature in enumerate(feature_list, start=1):
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict):
            continue
        lat, lon = _centroid_avg_vertices(geometry)
        area_ha, area_method = _area_ha(geometry)
        plots.append(
            PlotReference(
                plot_name=f"Demo Plot {idx:02d}",
                geojson_name=aoi_path.name,
                centroid_lat=round(lat, 8),
                centroid_lon=round(lon, 8),
                area_ha=round(area_ha, 6),
                area_method=area_method,
                polygon_count=_polygon_count(geometry),
                metadata=_properties_dict(feature.get("properties")),
            )
        )
    return plots


def _build_company(kyc: dict[str, Any] | None, geo_props: dict[str, Any]) -> CompanyData:
    company = _properties_dict((kyc or {}).get("company"))
    operator = _properties_dict((kyc or {}).get("operator"))

    identifiers = _properties_dict(
        _first_present(company.get("identifiers"), operator.get("identifiers"), (kyc or {}).get("identifiers"))
    )
    if not identifiers:
        identifiers = {"value": NA_VALUE}

    return CompanyData(
        operator=str(
            na_if_missing(
                _first_present(
                    company.get("operator_name"),
                    operator.get("name"),
                    company.get("name"),
                    geo_props.get("operator_name"),
                    geo_props.get("operator"),
                )
            )
        ),
        address=str(
            na_if_missing(
                _first_present(company.get("address"), operator.get("address"), geo_props.get("address"))
            )
        ),
        identifiers={k: na_if_missing(v) for k, v in identifiers.items()},
    )


def _build_commodity(
    kyc: dict[str, Any] | None,
    analysis: dict[str, Any] | None,
    geo_props: dict[str, Any],
) -> CommodityData:
    product = _properties_dict((kyc or {}).get("product"))
    supplier = _properties_dict((kyc or {}).get("supplier"))
    analysis_commodity = _properties_dict((analysis or {}).get("commodity"))

    return CommodityData(
        commodity_type=str(
            na_if_missing(
                _first_present(
                    product.get("commodity_type"),
                    product.get("type"),
                    analysis_commodity.get("commodity_type"),
                    (analysis or {}).get("commodity_type"),
                    geo_props.get("commodity_type"),
                )
            )
        ),
        country_region_label=str(
            na_if_missing(
                _first_present(
                    analysis_commodity.get("country_region_label"),
                    (analysis or {}).get("country_region_label"),
                    geo_props.get("country_region_label"),
                )
            )
        ),
        hs_code=str(na_if_missing(_first_present(product.get("hs_code"), product.get("hs")))),
        volume=na_if_missing(product.get("volume")),
        country_of_production=str(
            na_if_missing(
                _first_present(
                    product.get("country_of_production"),
                    product.get("country"),
                    supplier.get("country"),
                    (analysis or {}).get("country_of_production"),
                    geo_props.get("country_of_production"),
                    geo_props.get("country"),
                )
            )
        ),
    )


def _as_yes_no_na(value: Any) -> str:
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if value is None:
        return NA_VALUE
    text = str(value).strip().lower()
    if text in {"yes", "true", "y", "1"}:
        return "Yes"
    if text in {"no", "false", "n", "0"}:
        return "No"
    return NA_VALUE


def _build_deforestation(analysis: dict[str, Any] | None) -> DeforestationAssessment:
    source = analysis or {}
    nested = _properties_dict(source.get("deforestation_assessment"))
    forest_metrics = _properties_dict(source.get("forest_metrics"))
    metrics_block = _properties_dict(source.get("metrics"))

    evidence = _first_present(source.get("evidence_maps"), nested.get("evidence_maps"), [])
    evidence_list = [str(item) for item in evidence] if isinstance(evidence, list) else []

    summary_defaults: dict[str, Any] = {
        "area_forest_ha": NA_VALUE,
        "area_loss_post_2020_ha": NA_VALUE,
        "area_aoi_ha": NA_VALUE,
    }
    summary_source = _first_present(source.get("summary_metrics"), nested.get("summary_metrics"), {})
    if isinstance(summary_source, dict):
        for key, value in summary_source.items():
            summary_defaults[str(key)] = na_if_missing(value)

    if (
        summary_defaults["area_forest_ha"] == NA_VALUE
        and summary_defaults["area_loss_post_2020_ha"] == NA_VALUE
        and summary_defaults["area_aoi_ha"] == NA_VALUE
    ):
        forest_area = _first_present(
            forest_metrics.get("forest_end_year_area_ha"),
            forest_metrics.get("forest_end_year_ha"),
            forest_metrics.get("forest_2024_ha"),
        )
        loss_area = _first_present(
            forest_metrics.get("loss_2021_2024_ha"),
            _properties_dict(metrics_block.get("loss_2021_2024_ha")).get("value"),
        )
        aoi_area = _first_present(
            _properties_dict(metrics_block.get("aoi_area_ha")).get("value"),
            source.get("aoi_area_ha"),
        )

        summary_defaults["area_forest_ha"] = na_if_missing(forest_area)
        summary_defaults["area_loss_post_2020_ha"] = na_if_missing(loss_area)
        summary_defaults["area_aoi_ha"] = na_if_missing(aoi_area)

    detected = _first_present(source.get("deforestation_detected"), nested.get("deforestation_detected"))
    if detected is None:
        loss_area = summary_defaults.get("area_loss_post_2020_ha")
        if isinstance(loss_area, (int, float)):
            detected = loss_area > 0
        elif isinstance(loss_area, str):
            try:
                detected = float(loss_area) > 0
            except ValueError:
                detected = None

    return DeforestationAssessment(
        cutoff_date="2020-12-31",
        deforestation_detected=_as_yes_no_na(detected),
        evidence_maps=evidence_list,
        summary_metrics=summary_defaults,
    )


def build_report_v1(
    *,
    run_id: str,
    plot_id: str,
    aoi_geojson_path: str | Path,
    kyc_json: dict[str, Any] | None = None,
    analysis_json: dict[str, Any] | None = None,
    run_timestamp_utc: str | None = None,
) -> ReportV1:
    aoi_path = Path(aoi_geojson_path)
    aoi_payload = read_json_file(aoi_path)
    geo_props = _collect_geojson_properties(aoi_payload)

    data_sources = [f"AOI GeoJSON: {aoi_path.as_posix()}"]
    extra_sources = (analysis_json or {}).get("data_sources_summary")
    if isinstance(extra_sources, list):
        data_sources.extend(str(item) for item in extra_sources)

    risk_level = na_if_missing((analysis_json or {}).get("risk_level"))
    compliance_readiness = na_if_missing((analysis_json or {}).get("compliance_readiness"))

    return ReportV1(
        report_version="report_v1",
        report_id=f"{run_id}::{plot_id}",
        run_id=run_id,
        plot_id=plot_id,
        generated_at_utc=run_timestamp_utc or utc_now_iso(),
        data_sources_summary=data_sources,
        company=_build_company(kyc_json, geo_props),
        commodity=_build_commodity(kyc_json, analysis_json, geo_props),
        plots=_build_plots(aoi_path, aoi_payload),
        deforestation_assessment=_build_deforestation(analysis_json),
        risk_level=str(risk_level),
        compliance_readiness=str(compliance_readiness),
        artifacts=["report.json", "report.html", "report.pdf"],
        manifest_path="manifest.sha256",
    )
