#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from eudr_dmi.reports.build_report import build_report_v1
from eudr_dmi.reports.io import read_optional_json, safe_slug, write_json_stable, write_manifest_sha256
from eudr_dmi.reports.render_html import render_report_html
from eudr_dmi.reports.render_pdf import render_report_pdf


def _properties_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_outer_rings(geometry: dict[str, Any]) -> list[list[list[float]]]:
    geo_type = geometry.get("type")
    coords = geometry.get("coordinates") or []
    if geo_type == "Polygon":
        return [coords[0]] if coords else []
    if geo_type == "MultiPolygon":
        rings: list[list[list[float]]] = []
        for polygon in coords:
            if polygon:
                rings.append(polygon[0])
        return rings
    return []


def _iter_rings(payload: dict[str, Any]) -> list[list[list[float]]]:
    payload_type = payload.get("type")
    if payload_type == "FeatureCollection":
        out: list[list[list[float]]] = []
        for feature in payload.get("features") or []:
            geometry = _properties_dict((feature or {}).get("geometry"))
            out.extend(_iter_outer_rings(geometry))
        return out
    if payload_type == "Feature":
        return _iter_outer_rings(_properties_dict(payload.get("geometry")))
    return _iter_outer_rings(payload)


def _resolve_static_map_sources(analysis: dict[str, Any], analysis_path: Path) -> tuple[dict[str, Any], dict[str, Path]]:
    map_assets = _properties_dict(analysis.get("map_assets"))
    config_relpath = map_assets.get("config_relpath")
    config: dict[str, Any] = {}
    config_base = analysis_path.parent
    if isinstance(config_relpath, str) and config_relpath.strip():
        config_path = (analysis_path.parent / config_relpath).resolve()
        if config_path.is_file():
            config = _read_json(config_path)
            config_base = config_path.parent

    layers = _properties_dict(config.get("layers") or map_assets.get("layers"))
    layer_paths: dict[str, Path] = {}
    for key in ("aoi_boundary", "forest_end_year", "forest_loss_post_2020"):
        rel = layers.get(key)
        if isinstance(rel, str) and rel.strip():
            candidate = (config_base / rel).resolve()
            if candidate.is_file():
                layer_paths[key] = candidate

    return config, layer_paths


def _bbox_from_rings(rings: list[list[list[float]]]) -> tuple[float, float, float, float] | None:
    lon_values: list[float] = []
    lat_values: list[float] = []
    for ring in rings:
        for point in ring:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                continue
            lon_values.append(float(point[0]))
            lat_values.append(float(point[1]))
    if not lon_values or not lat_values:
        return None
    return min(lon_values), min(lat_values), max(lon_values), max(lat_values)


def _download_esri_satellite_png(
    *,
    bbox: tuple[float, float, float, float],
    width_px: int,
    height_px: int,
    output_path: Path,
) -> bytes | None:
    min_lon, min_lat, max_lon, max_lat = bbox
    lon_pad = max((max_lon - min_lon) * 0.08, 1e-6)
    lat_pad = max((max_lat - min_lat) * 0.08, 1e-6)

    params = {
        "bbox": f"{min_lon - lon_pad},{min_lat - lat_pad},{max_lon + lon_pad},{max_lat + lat_pad}",
        "bboxSR": "4326",
        "imageSR": "4326",
        "size": f"{width_px},{height_px}",
        "format": "png32",
        "f": "image",
    }
    url = (
        "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export?"
        + urlencode(params)
    )

    try:
        with urlopen(url, timeout=20) as response:
            image_bytes = response.read()
        if not image_bytes:
            return None
        output_path.write_bytes(image_bytes)
        return image_bytes
    except Exception:
        return None


def _write_static_deforestation_map_svg(
    *,
    analysis: dict[str, Any],
    analysis_path: Path,
    out_dir: Path,
) -> tuple[str | None, list[str]]:
    config, layer_paths = _resolve_static_map_sources(analysis, analysis_path)
    if "aoi_boundary" not in layer_paths:
        return None, []

    layer_data: dict[str, list[list[list[float]]]] = {}
    for key, path in layer_paths.items():
        layer_data[key] = _iter_rings(_read_json(path))

    all_rings: list[list[list[float]]] = []
    for rings in layer_data.values():
        all_rings.extend(rings)
    if not all_rings:
        return None, []

    cfg_bbox = _properties_dict(config.get("aoi_bbox"))
    bbox = None
    if cfg_bbox:
        try:
            bbox = (
                float(cfg_bbox["min_lon"]),
                float(cfg_bbox["min_lat"]),
                float(cfg_bbox["max_lon"]),
                float(cfg_bbox["max_lat"]),
            )
        except Exception:
            bbox = None
    if bbox is None:
        bbox = _bbox_from_rings(all_rings)
    if bbox is None:
        return None, []

    min_lon, min_lat, max_lon, max_lat = bbox
    span_lon = max(1e-12, max_lon - min_lon)
    span_lat = max(1e-12, max_lat - min_lat)

    width = 1100.0
    height = 760.0
    pad = 40.0
    plot_w = width - (2 * pad)
    plot_h = height - (2 * pad)

    satellite_name = "deforestation_map_satellite.png"
    satellite_path = out_dir / satellite_name
    satellite_bytes = _download_esri_satellite_png(
        bbox=bbox,
        width_px=int(plot_w),
        height_px=int(plot_h),
        output_path=satellite_path,
    )
    has_satellite = satellite_bytes is not None
    satellite_data_uri = None
    if satellite_bytes is not None:
        encoded = base64.b64encode(satellite_bytes).decode("ascii")
        satellite_data_uri = f"data:image/png;base64,{encoded}"

    def project(lon: float, lat: float) -> tuple[float, float]:
        x = pad + ((lon - min_lon) / span_lon) * plot_w
        y = pad + ((max_lat - lat) / span_lat) * plot_h
        return x, y

    def rings_path(rings: list[list[list[float]]]) -> str:
        segments: list[str] = []
        for ring in rings:
            points: list[tuple[float, float]] = []
            for point in ring:
                if not isinstance(point, (list, tuple)) or len(point) < 2:
                    continue
                points.append(project(float(point[0]), float(point[1])))
            if len(points) < 3:
                continue
            path_parts = [f"M {points[0][0]:.2f} {points[0][1]:.2f}"]
            for x, y in points[1:]:
                path_parts.append(f"L {x:.2f} {y:.2f}")
            path_parts.append("Z")
            segments.append(" ".join(path_parts))
        return " ".join(segments)

    forest_path = rings_path(layer_data.get("forest_end_year", []))
    loss_path = rings_path(layer_data.get("forest_loss_post_2020", []))
    aoi_path = rings_path(layer_data.get("aoi_boundary", []))
    latest_year = int(_properties_dict(config).get("latest_year") or 2024)

    svg_lines = [
        "<svg xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" width=\"1100\" height=\"760\" viewBox=\"0 0 1100 760\" role=\"img\" aria-label=\"Deforestation evidence map\">",
        "  <rect x=\"0\" y=\"0\" width=\"1100\" height=\"760\" fill=\"#ffffff\" />",
        "  <defs>",
        "    <clipPath id=\"map-clip\">",
        f"      <rect x=\"{pad:.2f}\" y=\"{pad:.2f}\" width=\"{plot_w:.2f}\" height=\"{plot_h:.2f}\" />",
        "    </clipPath>",
        "  </defs>",
        f"  <rect x=\"{pad:.2f}\" y=\"{pad:.2f}\" width=\"{plot_w:.2f}\" height=\"{plot_h:.2f}\" fill=\"#f4f4f4\" stroke=\"#d0d7de\" stroke-width=\"1\" />",
    ]
    if has_satellite:
        svg_lines.append(
            f"  <image href=\"{satellite_data_uri}\" xlink:href=\"{satellite_data_uri}\" x=\"{pad:.2f}\" y=\"{pad:.2f}\" width=\"{plot_w:.2f}\" height=\"{plot_h:.2f}\" preserveAspectRatio=\"none\" clip-path=\"url(#map-clip)\" />"
        )
    if forest_path:
        svg_lines.append(
            f"  <path d=\"{forest_path}\" fill=\"#1b5e20\" fill-opacity=\"0.30\" stroke=\"#1b5e20\" stroke-width=\"1\" clip-path=\"url(#map-clip)\" />"
        )
    if loss_path:
        svg_lines.append(
            f"  <path d=\"{loss_path}\" fill=\"#c62828\" fill-opacity=\"0.55\" stroke=\"#c62828\" stroke-width=\"1.2\" clip-path=\"url(#map-clip)\" />"
        )
    if aoi_path:
        svg_lines.append(
            f"  <path d=\"{aoi_path}\" fill=\"none\" stroke=\"#00e5ff\" stroke-width=\"2.4\" clip-path=\"url(#map-clip)\" />"
        )

    svg_lines.extend(
        [
            "  <rect x=\"28\" y=\"28\" width=\"340\" height=\"108\" fill=\"#ffffff\" fill-opacity=\"0.92\" stroke=\"#d0d7de\" />",
            "  <text x=\"42\" y=\"52\" font-size=\"18\" font-family=\"Arial, sans-serif\" fill=\"#111\">Static evidence map</text>",
            f"  <text x=\"42\" y=\"76\" font-size=\"14\" font-family=\"Arial, sans-serif\" fill=\"#1b5e20\">■ Forest Cover {latest_year}</text>",
            "  <text x=\"42\" y=\"98\" font-size=\"14\" font-family=\"Arial, sans-serif\" fill=\"#c62828\">■ Forest loss since 2020</text>",
            "  <text x=\"42\" y=\"120\" font-size=\"14\" font-family=\"Arial, sans-serif\" fill=\"#00acc1\">■ AOI boundary</text>",
            "  <text x=\"42\" y=\"142\" font-size=\"11\" font-family=\"Arial, sans-serif\" fill=\"#666\">Basemap: Esri World Imagery</text>",
            "</svg>",
            "",
        ]
    )

    static_name = "deforestation_map.svg"
    (out_dir / static_name).write_text("\n".join(svg_lines), encoding="utf-8")
    extra_artifacts = [satellite_name] if has_satellite else []
    return static_name, extra_artifacts


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate deterministic Report V1 artifacts (JSON, HTML, PDF).")
    p.add_argument("--run-id", required=True)
    p.add_argument("--plot-id", required=True)
    p.add_argument("--aoi-geojson", required=True)
    p.add_argument("--kyc-json")
    p.add_argument("--analysis-json")
    p.add_argument("--out-dir", default="out/reports")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    run_id = safe_slug(args.run_id)
    plot_id = safe_slug(args.plot_id)
    out_dir = Path(args.out_dir) / run_id / plot_id
    out_dir.mkdir(parents=True, exist_ok=True)

    kyc = read_optional_json(args.kyc_json)
    analysis = read_optional_json(args.analysis_json)
    analysis_path = Path(args.analysis_json).resolve() if args.analysis_json else None

    report = build_report_v1(
        run_id=run_id,
        plot_id=plot_id,
        aoi_geojson_path=args.aoi_geojson,
        kyc_json=kyc,
        analysis_json=analysis,
    )

    static_map_name: str | None = None
    static_extra_artifacts: list[str] = []
    if isinstance(analysis, dict) and analysis_path is not None and analysis_path.is_file():
        static_map_name, static_extra_artifacts = _write_static_deforestation_map_svg(
            analysis=analysis,
            analysis_path=analysis_path,
            out_dir=out_dir,
        )

    if static_map_name:
        current_maps = list(report.deforestation_assessment.evidence_maps)
        if static_map_name not in current_maps:
            current_maps = [static_map_name, *current_maps]
        report = replace(
            report,
            deforestation_assessment=replace(report.deforestation_assessment, evidence_maps=current_maps),
            artifacts=[*report.artifacts, static_map_name, *static_extra_artifacts],
        )

    report_json = out_dir / "report.json"
    report_html = out_dir / "report.html"
    report_pdf = out_dir / "report.pdf"

    write_json_stable(report_json, report.to_dict())
    render_report_html(report, report_html)
    render_report_pdf(report, report_pdf)
    write_manifest_sha256(out_dir, report.artifacts)

    print(out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
