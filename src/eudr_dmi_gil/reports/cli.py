from __future__ import annotations

import argparse
import os
import re
import sys
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import csv

from .bundle import bundle_dir as compute_bundle_dir
from .bundle import compute_sha256
from .bundle import resolve_evidence_root, write_manifest
from .determinism import _normalise_floats, canonical_json_bytes, sha256_bytes, write_bytes, write_json
from .report_model import (
    canonical_report_from_aoi_report_v2,
    materialize_evidence_pngs,
    render_canonical_html,
    render_canonical_pdf,
    update_canonical_artifact_hashes,
    write_canonical_metrics_csv,
    write_canonical_report_json,
    write_sha256_manifest,
)
from eudr_dmi_gil.deps.hansen_acquire import (
    DATASET_VERSION_DEFAULT,
    build_entries_from_provenance,
    infer_hansen_latest_year,
    portable_hansen_local_path,
    resolve_hansen_url_template,
)
from eudr_dmi_gil.deps.hansen_tiles import load_aoi_bbox
from eudr_dmi_gil.geo.aoi_area import compute_aoi_geodesic_area_ha
from eudr_dmi_gil.analysis.hansen_parcels import (
    compute_hansen_parcel_stats,
    land_use_designation_counts,
)
from .policy_refs import collect_policy_mapping_refs
from eudr_dmi_gil.commodities.analysis import (
    aoi_country_from_geojson,
    artifact_refs as commodity_artifact_refs,
    commodity_report_block,
    run_commodity_assessment,
)
from eudr_dmi_gil.commodities.config import resolve_commodity_config


def _resolve_generated_at_utc() -> tuple[str, datetime]:
    override = os.environ.get("EUDR_DMI_GENERATED_AT_UTC", "").strip()
    if override:
        try:
            generated_dt = datetime.fromisoformat(override.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                "EUDR_DMI_GENERATED_AT_UTC must be an ISO-8601 timestamp"
            ) from exc
        if generated_dt.tzinfo is None:
            raise ValueError("EUDR_DMI_GENERATED_AT_UTC must include a timezone")
        generated_dt = generated_dt.astimezone(timezone.utc).replace(microsecond=0)
        return generated_dt.isoformat(), generated_dt

    generated_dt = datetime.now(timezone.utc).replace(microsecond=0)
    return generated_dt.isoformat(), generated_dt


@contextmanager
def _timed(label: str) -> Any:
    start = time.perf_counter()
    print(f"[profile] START {label}", flush=True)
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"[profile] DONE  {label} ({elapsed:.2f}s)", flush=True)


def _git_commit() -> str:
    override = os.environ.get("EUDR_DMI_GIT_COMMIT")
    if override is not None:
        return override.strip()
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out
    except Exception:
        return ""


def _sanitize_id(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("empty id")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def _env_int(name: str) -> int | None:
    value = os.environ.get(name, "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parcel_reference_sort_key(parcel: object) -> tuple[float, float, str]:
    forest_area = getattr(parcel, "forest_area_ha", None) or 0.0
    pindala = getattr(parcel, "pindala_m2", None) or 0.0
    geodesic_area_ha = getattr(parcel, "geodesic_area_ha", None) or 0.0
    tie_area = pindala if pindala > 0 else geodesic_area_ha * 10_000.0
    parcel_id = str(getattr(parcel, "parcel_id", ""))
    return (-float(forest_area), -float(tie_area), parcel_id)


def _rel_href(from_path: Path, to_path: Path) -> str:
    rel = os.path.relpath(to_path, start=from_path.parent)
    return Path(rel).as_posix()


def _content_type_for_path(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return "application/json"
    if suffix == ".geojson":
        return "application/geo+json"
    if suffix == ".csv":
        return "text/csv"
    if suffix == ".html":
        return "text/html"
    if suffix == ".pdf":
        return "application/pdf"
    if suffix == ".png":
        return "image/png"
    if suffix == ".wkt":
        return "text/plain"
    return None


def _parcel_table_rows(parcels: list[object]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for parcel in parcels:
        rows.append(
            {
                "parcel_id": getattr(parcel, "parcel_id", ""),
                "hansen_land_area_ha": getattr(parcel, "hansen_land_area_ha", None),
                "maaamet_land_area_ha": getattr(parcel, "maaamet_land_area_ha", None),
                "hansen_forest_area_ha": getattr(parcel, "hansen_forest_area_ha", None),
                "maaamet_forest_area_ha": getattr(parcel, "maaamet_forest_area_ha", None),
                "hansen_forest_loss_ha": getattr(parcel, "hansen_forest_loss_ha", None),
            }
        )
    return rows


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return str(value)


def _build_maaamet_parcel_metadata(parcels: list[object]) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for parcel in parcels:
        props = getattr(parcel, "properties", {}) or {}
        if not isinstance(props, dict):
            props = {}
        designation = props.get("siht1") or props.get("sihtotstarve")
        municipality = props.get("ov_nimi") or props.get("ay_nimi") or props.get("omavalitsus")
        entries.append(
            {
                "parcel_id": getattr(parcel, "parcel_id", ""),
                "land_use_designation": designation,
                "municipality": municipality,
                "pindala_m2": getattr(parcel, "pindala_m2", None),
                "maaamet_land_area_ha": getattr(parcel, "maaamet_land_area_ha", None),
                "maaamet_forest_area_ha": getattr(parcel, "maaamet_forest_area_ha", None),
                "forest_area_ha": getattr(parcel, "forest_area_ha", None),
                "properties": _json_safe(props),
            }
        )

    return {
        "parcel_count": len(entries),
        "land_use_designation_counts": land_use_designation_counts(parcels),
        "parcels": entries,
    }


def _write_map_config(
    *,
    path: Path,
    aoi_bbox: tuple[float, float, float, float],
    latest_year: int,
    layers: dict[str, str],
) -> None:
    payload = {
        "aoi_bbox": {
            "min_lon": aoi_bbox[0],
            "min_lat": aoi_bbox[1],
            "max_lon": aoi_bbox[2],
            "max_lat": aoi_bbox[3],
        },
        "latest_year": latest_year,
        "layers": layers,
    }
    write_json(path, payload)


def _render_html_summary(
    report: dict[str, Any],
    *,
    html_path: Path,
    artifact_paths: list[Path],
    map_config_relpath: str | None = None,
    parcel_rows: list[dict[str, Any]] | None = None,
) -> str:
    def row(k: str, v: str) -> str:
        return f"<tr><th>{k}</th><td>{v}</td></tr>"

    def link_row(label: str, relpath: str) -> str:
        return f"<tr><th>{label}</th><td><a href=\"{relpath}\">{relpath}</a></td></tr>"

    summary = report.get("results_summary", {})
    aoi_area = summary.get("aoi_area", {})
    deforestation = summary.get("deforestation_free_post_2020", {})
    forest_metrics = report.get("forest_metrics", {})

    aoi_id = report.get("aoi_id", "(unknown)")
    bundle_id = report.get("bundle_id", "(unknown)")
    generated = report.get("generated_at_utc", "(unknown)")
    version = report.get("report_version", "(unknown)")
    geom_ref = report.get("aoi_geometry_ref", {})

    bundle_root = html_path.parents[2]
    evidence_artifacts = report.get("evidence_artifacts", [])
    evidence_rows = []
    for item in evidence_artifacts:
        if not isinstance(item, dict):
            continue
        relpath = item.get("relpath")
        if not isinstance(relpath, str):
            continue
        abs_path = bundle_root / relpath
        href = _rel_href(html_path, abs_path)
        role = (item.get("meta") or {}).get("role") if isinstance(item.get("meta"), dict) else ""
        evidence_rows.append(
            f"<tr><td><a href=\"{href}\">{relpath}</a></td><td><code>{item.get('sha256')}</code></td><td>{item.get('size_bytes','')}</td><td>{role}</td></tr>"
        )

    evidence_table = "\n".join(evidence_rows) if evidence_rows else "<tr><td colspan=\"4\">(none)</td></tr>"

    datasets_rows = []
    for ds in report.get("datasets", []) or []:
        if not isinstance(ds, dict):
            continue
        datasets_rows.append(
            f"<tr><td>{ds.get('dataset_id')}</td><td>{ds.get('version','')}</td><td>{ds.get('retrieved_at_utc','')}</td><td>{ds.get('license','')}</td><td>{ds.get('source_url','')}</td></tr>"
        )
    datasets_table = "\n".join(datasets_rows) if datasets_rows else "<tr><td colspan=\"5\">(none)</td></tr>"

    params_rows = []
    for key, value in sorted((report.get("parameters") or {}).items(), key=lambda kv: kv[0]):
        params_rows.append(row(key, str(value)))
    params_table = "\n".join(params_rows) if params_rows else row("(none)", "")

    mapping_rows = []
    for entry in report.get("policy_mapping", []) or []:
        if not isinstance(entry, dict):
            continue
        evidence_fields = ", ".join(entry.get("evidence_fields") or [])
        artifacts = ", ".join(
            f"<a href=\"{_rel_href(html_path, bundle_root / rel)}\">{rel}</a>"
            for rel in entry.get("artifact_relpaths") or []
        )
        mapping_rows.append(
            f"<tr><td>{entry.get('article_ref')}</td><td>{entry.get('requirement')}</td><td>{evidence_fields}</td><td>{artifacts}</td><td>{entry.get('status')}</td></tr>"
        )
    mapping_table = "\n".join(mapping_rows) if mapping_rows else "<tr><td colspan=\"5\">(none)</td></tr>"

    deforestation_rows = []
    if deforestation:
        deforestation_rows.extend(
            [
                row("Definition", "Forest loss after 2020-12-31 (pixel-wise intersection)"),
                row("Forest loss (ha)", str(deforestation.get("forest_loss_post_2020_ha"))),
                row("Percent of AOI", str(deforestation.get("percent_of_aoi"))),
                row("Threshold (ha)", str(deforestation.get("threshold_ha"))),
                row("Status", str(deforestation.get("status"))),
                row("Uncertainty", str(deforestation.get("uncertainty"))),
            ]
        )
    deforestation_table = "\n".join(deforestation_rows) if deforestation_rows else row("(none)", "")

    forest_metrics_rows = []
    if isinstance(forest_metrics, dict) and forest_metrics:
        method_block = forest_metrics.get("method", {}) if isinstance(forest_metrics.get("method"), dict) else {}
        end_year = int(forest_metrics.get("end_year") or 2025)
        loss_recent = forest_metrics.get(f"loss_2021_{end_year}_ha")
        if loss_recent is None:
            loss_recent = forest_metrics.get("loss_2021_2025_ha")
        loss_recent_pct = forest_metrics.get(f"loss_2021_{end_year}_pct_of_rfm")
        if loss_recent_pct is None:
            loss_recent_pct = forest_metrics.get("loss_2021_2025_pct_of_rfm")
        forest_end_year_value = forest_metrics.get("forest_end_year_ha")
        if forest_end_year_value is None:
            forest_end_year_value = forest_metrics.get("forest_end_year_area_ha")
        forest_metrics_rows.extend(
            [
                row("Tree cover threshold (%)", str(forest_metrics.get("canopy_threshold_pct"))),
                row("Reference forest mask year", str(forest_metrics.get("reference_forest_mask_year"))),
                row("RFM area (ha)", str(forest_metrics.get("rfm_area_ha"))),
                row(
                    f"Loss 2021-{end_year} (ha)",
                    f"{loss_recent} ({loss_recent_pct}% of RFM)" if loss_recent is not None else "",
                ),
                row(
                    "Forest end-year area (ha)",
                    str(forest_end_year_value),
                ),
                row("Method summary", str(method_block.get("notes", ""))),
            ]
        )
    forest_metrics_table = (
        "\n".join(forest_metrics_rows) if forest_metrics_rows else row("(none)", "")
    )

    artifacts_for_links = "\n".join(
        f"<li><a href=\"{_rel_href(html_path, p)}\">{_rel_href(html_path, p)}</a></li>"
        for p in sorted(artifact_paths, key=lambda p: p.as_posix())
    )

    parcel_rows = parcel_rows or []
    parcels_table = "".join(
        "".join(
            [
                "<tr>",
                f"<td>{row.get('parcel_id','')}</td>",
                f"<td>{row.get('hansen_land_area_ha','')}</td>",
                f"<td>{row.get('maaamet_land_area_ha','')}</td>",
                f"<td>{row.get('hansen_forest_area_ha','')}</td>",
                f"<td>{row.get('maaamet_forest_area_ha','')}</td>",
                f"<td>{row.get('hansen_forest_loss_ha','')}</td>",
                "</tr>",
            ]
        )
        for row in parcel_rows
    )
    if not parcels_table:
        parcels_table = "<tr><td colspan=\"6\">(none)</td></tr>"

    map_block = ""
    if map_config_relpath:
        map_href = map_config_relpath
        map_block = f"""
    <h2>Map (interactive)</h2>
    <div id=\"map\"></div>
    <p class=\"muted\">Map layers are loaded from <a href=\"{map_href}\">{map_href}</a>.</p>
    <link
        rel=\"stylesheet\"
        href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\"
        integrity=\"sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=\"
        crossorigin=\"\"
    />
    <script
        src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"
        integrity=\"sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=\"
        crossorigin=\"\"
    ></script>
    <script>
        (function () {{
            const map = L.map('map', {{ zoomControl: true }});
            const satellite = L.tileLayer(
                'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{{z}}/{{y}}/{{x}}',
                {{
                    attribution: 'Tiles © Esri — Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community',
                }},
            ).addTo(map);
            const configUrl = '{map_href}';
            fetch(configUrl)
                .then((resp) => resp.json().then((config) => ({{ config, configBaseUrl: resp.url }})))
                .then(({{ config, configBaseUrl }}) => {{
                    const bbox = config.aoi_bbox;
                    const bounds = L.latLngBounds([
                        [bbox.min_lat, bbox.min_lon],
                        [bbox.max_lat, bbox.max_lon],
                    ]);
                    map.fitBounds(bounds);

                    const layerDefs = [
                        {{
                            key: 'forest_2000',
                            label: 'Forest cover 2000',
                            color: '#2e7d32',
                            defaultOn: true,
                            options: {{ style: {{ color: '#2e7d32', weight: 1, fillOpacity: 0.3 }} }},
                        }},
                        {{
                            key: 'forest_end_year',
                            label: `Forest cover ${{config.latest_year}}`,
                            color: '#1b5e20',
                            defaultOn: true,
                            options: {{ style: {{ color: '#1b5e20', weight: 1, fillOpacity: 0.3 }} }},
                        }},
                        {{
                            key: 'forest_loss_post_2020',
                            label: 'Forest loss since 2020',
                            color: '#c62828',
                            defaultOn: true,
                            options: {{ style: {{ color: '#c62828', weight: 2, fillOpacity: 0.55 }} }},
                        }},
                        {{
                            key: 'aoi_boundary',
                            label: 'AOI boundary',
                            color: '#00e5ff',
                            defaultOn: true,
                            options: {{ style: {{ color: '#00e5ff', weight: 4, opacity: 1, fillOpacity: 0 }} }},
                        }},
                        {{
                            key: 'parcels',
                            label: 'Maa-amet parcels',
                            color: '#000000',
                            defaultOn: false,
                            options: {{
                                style: {{ color: '#000000', weight: 3, opacity: 1, fillOpacity: 0 }},
                                onEachFeature: (feature, layer) => {{
                                    const props = feature.properties || {{}};
                                    const label = `${{props.parcel_id || ''}} | forest_ha=${{props.hansen_forest_area_ha ?? ''}} | loss_ha=${{props.hansen_forest_loss_ha ?? ''}}`;
                                    layer.bindTooltip(label, {{ sticky: true }});
                                }},
                            }},
                        }},
                    ];

                    const overlayGroups = {{}};
                    layerDefs.forEach((def) => {{
                        const group = L.layerGroup();
                        overlayGroups[def.key] = group;
                        if (def.defaultOn) {{
                            group.addTo(map);
                        }}
                    }});

                    const addGeoJson = (def, url) => {{
                        if (!url) return;
                        const resolvedUrl = new URL(url, configBaseUrl).toString();
                        fetch(resolvedUrl)
                            .then((r) => r.json())
                            .then((data) => {{
                                L.geoJSON(data, def.options).addTo(overlayGroups[def.key]);
                            }})
                            .catch((err) => {{
                                console.warn(`Layer load failed: ${{def.label}}`, err);
                            }});
                    }};

                    layerDefs.forEach((def) => addGeoJson(def, config.layers?.[def.key]));

                    const legend = L.control({{ position: 'topright' }});
                    legend.onAdd = function () {{
                        const div = L.DomUtil.create('div', 'map-legend');
                        div.innerHTML = '<strong>Map layers</strong>';
                        layerDefs.forEach((def, idx) => {{
                            const hasUrl = Boolean(config.layers?.[def.key]);
                            if (!hasUrl) return;
                            const checked = map.hasLayer(overlayGroups[def.key]) ? 'checked' : '';
                            const item = document.createElement('label');
                            item.className = 'map-legend-item';
                            item.innerHTML = `
                                <input type="checkbox" data-layer-key="${{def.key}}" ${{checked}} />
                                <span class="map-legend-swatch" style="background:${{def.color}}"></span>
                                <span>${{def.label}}</span>
                            `;
                            div.appendChild(item);
                        }});
                        L.DomEvent.disableClickPropagation(div);
                        L.DomEvent.disableScrollPropagation(div);

                        div.querySelectorAll('input[type="checkbox"]').forEach((el) => {{
                            el.addEventListener('change', (event) => {{
                                const key = event.target.getAttribute('data-layer-key');
                                const group = overlayGroups[key];
                                if (!group) return;
                                if (event.target.checked) {{
                                    map.addLayer(group);
                                }} else {{
                                    map.removeLayer(group);
                                }}
                            }});
                        }});
                        return div;
                    }};
                    legend.addTo(map);
                }});
        }})();
    </script>
"""

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>AOI Report — {aoi_id}</title>
    <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 24px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ background: #f6f6f6; text-align: left; width: 240px; }}
    h2 {{ margin-top: 28px; }}
    code {{ background: #f6f6f6; padding: 1px 4px; border-radius: 4px; }}
    .muted {{ color: #666; }}
        .map-legend {{ background: #fff; border: 1px solid #ddd; border-radius: 6px; padding: 8px 10px; box-shadow: 0 1px 6px rgba(0,0,0,0.2); font-size: 12px; line-height: 1.4; }}
        .map-legend-item {{ display: flex; align-items: center; gap: 6px; margin-top: 6px; }}
        .map-legend-item input {{ margin: 0; }}
        .map-legend-swatch {{ display: inline-block; width: 12px; height: 12px; border: 1px solid #333; }}
        #map {{ height: 420px; border: 1px solid #ddd; border-radius: 8px; margin: 12px 0 16px; background: #fafafa; }}
  </style>
</head>
<body>
  <h1>AOI Report</h1>
  <table>
    {row('AOI', str(aoi_id))}
    {row('Bundle', str(bundle_id))}
    {row('Generated (UTC)', str(generated))}
    {row('Report Version', str(version))}
    {row('Geometry Ref', f"{geom_ref.get('kind')}: {geom_ref.get('value')}")}
    {row('AOI area (ha)', str(aoi_area.get('area_ha')))}
    {row('AOI area method', str(aoi_area.get('method')))}
  </table>

  <h2>Deforestation-free (post-2020)</h2>
  <table>
    {deforestation_table}
  </table>

  <h2>Forest baseline / forest mask</h2>
  <table>
    {row('Tree cover threshold (%)', str((report.get('parameters', {}).get('forest_loss_post_2020') or {}).get('canopy_threshold_percent', '')))}
    {row('Baseline year', '2000')}
    {row('Cutoff year', str((report.get('parameters', {}).get('forest_loss_post_2020') or {}).get('cutoff_year', '')))}
  </table>

    <h2>Forest area and loss (pixel-based, AOI intersection)</h2>
    <table>
        {forest_metrics_table}
    </table>

    {map_block}

    <h2>Parcels (top 10 with forest ≥ 3 ha)</h2>
    <table>
        <tr>
            <th>Parcel ID</th>
            <th>Hansen land area (ha)</th>
            <th>Maa-amet land area (ha)</th>
            <th>Hansen forest area (ha)</th>
            <th>Maa-amet forest area (ha)</th>
            <th>Forest loss (ha)</th>
        </tr>
        {parcels_table}
    </table>

  <h2>Data sources & provenance</h2>
  <table>
    <tr><th>dataset_id</th><th>version</th><th>retrieved_at_utc</th><th>license</th><th>source_url</th></tr>
    {datasets_table}
  </table>

  <h2>Methods & parameters</h2>
  <table>
    {params_table}
  </table>

  <h2>Traceability to EUDR Articles</h2>
  <table>
    <tr><th>Article</th><th>Requirement</th><th>Evidence fields</th><th>Artifacts</th><th>Status</th></tr>
    {mapping_table}
  </table>

  <h2>Evidence artifact index</h2>
  <table>
    <tr><th>relpath</th><th>sha256</th><th>size_bytes</th><th>role</th></tr>
    {evidence_table}
  </table>

  <h2>Bundle artifacts</h2>
  <ul>
    {artifacts_for_links}
  </ul>
</body>
</html>
"""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m eudr_dmi_gil.reports.cli",
        description="Generate a deterministic AOI report bundle (JSON + HTML + manifest).",
    )

    p.add_argument("--aoi-id", required=True, help="AOI identifier")

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--aoi-geojson", help="Path to AOI geometry GeoJSON")
    g.add_argument("--aoi-wkt", help="AOI geometry WKT string")

    p.add_argument(
        "--bundle-id",
        help=(
            "Optional bundle id. If omitted, a deterministic id is derived from aoi-id + UTC timestamp."
        ),
    )

    p.add_argument(
        "--out-format",
        choices=["json", "html", "both"],
        default="both",
        help="Which outputs to write into the bundle.",
    )

    p.add_argument(
        "--policy-mapping-ref",
        action="append",
        default=[],
        help="Optional policy-to-evidence spine reference (repeatable).",
    )

    p.add_argument(
        "--policy-mapping-ref-file",
        action="append",
        default=[],
        help=(
            "Path to a file containing newline-separated policy-to-evidence spine references "
            "(repeatable). Lines starting with '#' are ignored."
        ),
    )

    p.add_argument(
        "--dummy-metric",
        default="dummy_metric=1:count",
        help="Metric in the form name=value:unit (for scaffolding).",
    )

    p.add_argument(
        "--metric",
        action="append",
        default=[],
        help=(
            "Metric row in the form variable=value:unit[:source[:notes]] (repeatable). "
            "If omitted, --dummy-metric is used."
        ),
    )

    p.add_argument(
        "--commodity",
        action="append",
        default=[],
        help=(
            "Explicit EUDR commodity id for this single-commodity assessment "
            "(example: coffee). Repeat values are rejected."
        ),
    )

    p.add_argument(
        "--commodity-config",
        help=(
            "Path to a structured commodity provider config JSON. The run may contain "
            "exactly one commodity config."
        ),
    )

    p.add_argument(
        "--evidence-only-assessment",
        action="store_true",
        help="Generate an evidence-only assessment with no commodity layer.",
    )

    p.add_argument(
        "--enable-hansen-post-2020-loss",
        action="store_true",
        help="Enable Hansen post-2020 forest loss pixel-mask computation.",
    )

    p.add_argument(
        "--hansen-tile-dir",
        help="Optional Hansen tile directory (defaults to EUDR_DMI_HANSEN_TILE_DIR).",
    )

    p.add_argument(
        "--hansen-minio-cache",
        action="store_true",
        help="Enable MinIO object-store cache for Hansen tiles.",
    )
    p.add_argument(
        "--hansen-no-minio-cache",
        dest="hansen_minio_cache",
        action="store_false",
        help="Disable MinIO object-store cache for Hansen tiles.",
    )
    env_minio_cache = _env_flag(
        "EUDR_DMI_HANSEN_MINIO_CACHE",
        default=_env_flag("HANSEN_MINIO_CACHE", default=False),
    )
    p.set_defaults(hansen_minio_cache=env_minio_cache)

    p.add_argument(
        "--hansen-canopy-threshold",
        type=int,
        default=10,
        help="Tree cover canopy threshold percent for baseline forest mask (default: 10).",
    )

    p.add_argument(
        "--hansen-cutoff-year",
        type=int,
        default=2020,
        help="Cutoff year for post-loss filter (default: 2020).",
    )

    env_hansen_parcel_top_n = _env_int("EUDR_DMI_HANSEN_PARCEL_TOP_N")
    p.add_argument(
        "--hansen-parcel-top-n",
        type=int,
        default=env_hansen_parcel_top_n if env_hansen_parcel_top_n is not None else 0,
        help=(
            "Optional: compute Hansen parcel stats only for top-N parcels by reference "
            "forest area (0 means all parcels)."
        ),
    )

    p.add_argument(
        "--hansen-reproject-to-projected",
        dest="hansen_reproject_to_projected",
        action="store_true",
        help="Reproject Hansen tiles to a projected CRS for constant pixel area.",
    )
    p.add_argument(
        "--hansen-no-reproject-to-projected",
        dest="hansen_reproject_to_projected",
        action="store_false",
        help="Disable Hansen tile reprojection to a projected CRS.",
    )
    p.set_defaults(hansen_reproject_to_projected=True)

    p.add_argument(
        "--hansen-projected-crs",
        default="EPSG:6933",
        help="Projected CRS for Hansen reprojection (default: EPSG:6933).",
    )

    p.add_argument(
        "--end-year",
        type=int,
        help=(
            "Requested final evidence year for post-2020 loss. Must be >= 2021. "
            "If omitted, the selected loss dataset coverage is used."
        ),
    )

    p.add_argument(
        "--jrc-gfc2020-raster",
        default=os.environ.get("EUDR_DMI_JRC_GFC2020_RASTER"),
        help=(
            "Local JRC Global Forest Cover 2020 raster for the new baseline calculation "
            "(env: EUDR_DMI_JRC_GFC2020_RASTER)."
        ),
    )

    p.add_argument(
        "--hansen-lossyear-raster",
        default=os.environ.get("EUDR_DMI_HANSEN_LOSSYEAR_RASTER"),
        help=(
            "Local Hansen lossyear raster aligned by the JRC baseline calculation "
            "(env: EUDR_DMI_HANSEN_LOSSYEAR_RASTER)."
        ),
    )

    p.add_argument(
        "--hansen-treecover2000-raster",
        default=os.environ.get("EUDR_DMI_HANSEN_TREECOVER2000_RASTER"),
        help=(
            "Optional local Hansen treecover2000 raster. When provided (together with "
            "--hansen-lossyear-raster), computes a second forest baseline "
            "(treecover2000 >= --hansen-canopy-threshold, the FAO/EUDR Art.2 forest definition) "
            "alongside the JRC GFC2020 baseline, reported as separate metrics "
            "(env: EUDR_DMI_HANSEN_TREECOVER2000_RASTER)."
        ),
    )

    p.add_argument(
        "--satellite-baseline-raster",
        default=os.environ.get("EUDR_DMI_SATELLITE_BASELINE_RASTER"),
        help=(
            "Local satellite imagery raster (RGB) for the baseline/cutoff period, used only "
            "as visual evidence context (env: EUDR_DMI_SATELLITE_BASELINE_RASTER)."
        ),
    )
    p.add_argument(
        "--satellite-baseline-date",
        default=os.environ.get("EUDR_DMI_SATELLITE_BASELINE_DATE", ""),
        help="Acquisition date/range label for the baseline satellite raster.",
    )
    p.add_argument(
        "--satellite-recent-raster",
        default=os.environ.get("EUDR_DMI_SATELLITE_RECENT_RASTER"),
        help=(
            "Local satellite imagery raster (RGB) for a recent period, used only as visual "
            "evidence context (env: EUDR_DMI_SATELLITE_RECENT_RASTER)."
        ),
    )
    p.add_argument(
        "--satellite-recent-date",
        default=os.environ.get("EUDR_DMI_SATELLITE_RECENT_DATE", ""),
        help="Acquisition date/range label for the recent satellite raster.",
    )
    p.add_argument(
        "--satellite-regional-raster",
        default=os.environ.get("EUDR_DMI_SATELLITE_REGIONAL_RASTER"),
        help=(
            "Optional wide-context local satellite imagery raster (RGB), covering a much larger "
            "extent than the AOI, used only for the Regional Overview evidence page "
            "(env: EUDR_DMI_SATELLITE_REGIONAL_RASTER)."
        ),
    )
    p.add_argument(
        "--satellite-regional-date",
        default=os.environ.get("EUDR_DMI_SATELLITE_REGIONAL_DATE", ""),
        help="Acquisition date/range label for the regional-overview satellite raster.",
    )
    p.add_argument(
        "--regional-admin-boundaries-geojson",
        default=os.environ.get("EUDR_DMI_REGIONAL_ADMIN_BOUNDARIES_GEOJSON"),
        help=(
            "Optional local GeoJSON of real administrative (state/province) boundaries and "
            "names, clipped to the Regional Overview page's view (see "
            "scripts/fetch_admin_boundaries.py), used only to draw non-fabricated regional "
            "context on that evidence page "
            "(env: EUDR_DMI_REGIONAL_ADMIN_BOUNDARIES_GEOJSON)."
        ),
    )
    p.add_argument(
        "--satellite-dataset-title",
        default=os.environ.get(
            "EUDR_DMI_SATELLITE_DATASET_TITLE", "Sentinel-2 L2A true-color (visual/TCI) composite"
        ),
        help="Dataset title recorded for the satellite imagery evidence layer.",
    )
    p.add_argument(
        "--satellite-dataset-version",
        default=os.environ.get("EUDR_DMI_SATELLITE_DATASET_VERSION", ""),
        help="Dataset version/identifier recorded for the satellite imagery evidence layer.",
    )
    p.add_argument(
        "--satellite-source-url",
        default=os.environ.get(
            "EUDR_DMI_SATELLITE_SOURCE_URL", "https://earth-search.aws.element84.com/v1"
        ),
        help="Source URL/API recorded for the satellite imagery evidence layer.",
    )
    p.add_argument(
        "--satellite-license",
        default=os.environ.get(
            "EUDR_DMI_SATELLITE_LICENSE", "Contains modified Copernicus Sentinel data"
        ),
        help="License string recorded for the satellite imagery evidence layer.",
    )
    p.add_argument(
        "--satellite-selection-method",
        default=os.environ.get("EUDR_DMI_SATELLITE_SELECTION_METHOD", ""),
        help="Free-text description of how the satellite scenes/composite were selected.",
    )

    p.add_argument(
        "--loss-dataset-end-year",
        type=int,
        default=_env_int("EUDR_DMI_LOSS_DATASET_END_YEAR"),
        help=(
            "Latest year actually available in the selected lossyear dataset. "
            "If omitted, inferred from the configured Hansen dataset version/path."
        ),
    )

    p.add_argument(
        "--analysis-target-crs",
        default="EPSG:6933",
        help="Equal-area target CRS for JRC/Hansen raster intersections (default: EPSG:6933).",
    )

    p.add_argument(
        "--analysis-target-resolution-m",
        type=float,
        default=30.0,
        help="Target grid resolution in meters for JRC/Hansen intersections (default: 30).",
    )

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.end_year is not None and args.end_year < 2021:
        raise ValueError("--end-year must be >= 2021")
    commodity_ids = [str(v).strip().lower() for v in (args.commodity or []) if str(v).strip()]
    if len(commodity_ids) > 1:
        raise ValueError("A single report run supports exactly one commodity")
    if args.evidence_only_assessment and (commodity_ids or args.commodity_config):
        raise ValueError("--evidence-only-assessment cannot be combined with commodity inputs")
    commodity_config = None
    if not args.evidence_only_assessment:
        commodity_config = resolve_commodity_config(
            commodity_id=commodity_ids[0] if commodity_ids else None,
            commodity_config_path=args.commodity_config,
        )
    if commodity_config is not None and not args.jrc_gfc2020_raster:
        raise RuntimeError("--jrc-gfc2020-raster is required when --commodity is configured")
    if commodity_config is not None and not args.hansen_lossyear_raster:
        raise RuntimeError("--hansen-lossyear-raster is required when --commodity is configured")
    if bool(args.satellite_baseline_raster) != bool(args.satellite_recent_raster):
        raise RuntimeError(
            "--satellite-baseline-raster and --satellite-recent-raster must be provided together"
        )

    policy_mapping_refs = collect_policy_mapping_refs(
        refs=list(args.policy_mapping_ref or []),
        ref_files=list(args.policy_mapping_ref_file or []),
    )

    aoi_id = _sanitize_id(args.aoi_id)

    generated_at_utc, generated_dt = _resolve_generated_at_utc()

    bundle_id = args.bundle_id
    if not bundle_id:
        # Deterministic derivation from AOI id + timestamp (timestamp is explicit).
        stamp = generated_dt.strftime("%Y%m%dT%H%M%SZ")
        bundle_id = f"{aoi_id}-{stamp}"
    bundle_id = _sanitize_id(bundle_id)

    # Bundle date is UTC date.
    bundle_date = generated_dt.date().strftime("%Y-%m-%d")

    bdir = compute_bundle_dir(bundle_id=bundle_id, bundle_date=bundle_date)
    resolve_evidence_root()

    # Write geometry into the bundle for portability.
    inputs_dir = bdir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    if args.aoi_geojson:
        geojson_src = Path(args.aoi_geojson)
        geo_bytes = geojson_src.read_bytes()
        geo_rel = Path("inputs") / "aoi.geojson"
        geo_kind = "geojson"
    else:
        geo_bytes = (args.aoi_wkt.strip() + "\n").encode("utf-8")
        geo_rel = Path("inputs") / "aoi.wkt"
        geo_kind = "wkt"

    geo_path = bdir / geo_rel
    write_bytes(geo_path, geo_bytes)
    geo_sha = sha256_bytes(geo_bytes)

    satellite_info: dict[str, Any] | None = None
    if args.satellite_baseline_raster and args.satellite_recent_raster:
        if geo_kind != "geojson":
            raise RuntimeError("Satellite imagery requires --aoi-geojson input")
        baseline_src = Path(args.satellite_baseline_raster)
        recent_src = Path(args.satellite_recent_raster)
        if not baseline_src.is_file():
            raise FileNotFoundError(f"Satellite baseline raster not found: {baseline_src}")
        if not recent_src.is_file():
            raise FileNotFoundError(f"Satellite recent raster not found: {recent_src}")
        baseline_dst = inputs_dir / "satellite_baseline.tif"
        recent_dst = inputs_dir / "satellite_recent.tif"
        baseline_dst.write_bytes(baseline_src.read_bytes())
        recent_dst.write_bytes(recent_src.read_bytes())
        satellite_info = {
            "baseline_raster_ref": {
                "relpath": str(baseline_dst.relative_to(bdir)).replace("\\", "/"),
                "sha256": compute_sha256(baseline_dst),
                "content_type": "image/tiff",
            },
            "recent_raster_ref": {
                "relpath": str(recent_dst.relative_to(bdir)).replace("\\", "/"),
                "sha256": compute_sha256(recent_dst),
                "content_type": "image/tiff",
            },
            "baseline_date": args.satellite_baseline_date,
            "recent_date": args.satellite_recent_date,
            "dataset_title": args.satellite_dataset_title,
            "dataset_version": args.satellite_dataset_version,
            "source_url": args.satellite_source_url,
            "license": args.satellite_license,
            "selection_method": args.satellite_selection_method,
        }

    if args.satellite_regional_raster:
        if geo_kind != "geojson":
            raise RuntimeError("Satellite imagery requires --aoi-geojson input")
        regional_src = Path(args.satellite_regional_raster)
        if not regional_src.is_file():
            raise FileNotFoundError(f"Satellite regional raster not found: {regional_src}")
        regional_dst = inputs_dir / "satellite_regional.tif"
        regional_dst.write_bytes(regional_src.read_bytes())
        if satellite_info is None:
            satellite_info = {
                "dataset_title": args.satellite_dataset_title,
                "dataset_version": args.satellite_dataset_version,
                "source_url": args.satellite_source_url,
                "license": args.satellite_license,
                "selection_method": args.satellite_selection_method,
            }
        satellite_info["regional_raster_ref"] = {
            "relpath": str(regional_dst.relative_to(bdir)).replace("\\", "/"),
            "sha256": compute_sha256(regional_dst),
            "content_type": "image/tiff",
        }
        satellite_info["regional_date"] = args.satellite_regional_date

    if args.regional_admin_boundaries_geojson:
        boundaries_src = Path(args.regional_admin_boundaries_geojson)
        if not boundaries_src.is_file():
            raise FileNotFoundError(
                f"Regional admin boundaries GeoJSON not found: {boundaries_src}"
            )
        boundaries_dst = inputs_dir / "regional_admin_boundaries.geojson"
        boundaries_dst.write_bytes(boundaries_src.read_bytes())
        if satellite_info is None:
            satellite_info = {
                "dataset_title": args.satellite_dataset_title,
                "dataset_version": args.satellite_dataset_version,
                "source_url": args.satellite_source_url,
                "license": args.satellite_license,
                "selection_method": args.satellite_selection_method,
            }
        satellite_info["regional_admin_boundaries_ref"] = {
            "relpath": str(boundaries_dst.relative_to(bdir)).replace("\\", "/"),
            "sha256": compute_sha256(boundaries_dst),
            "content_type": "application/geo+json",
        }

    aoi_area_ha: float | None = None
    aoi_area_method = ""
    if geo_kind == "geojson":
        try:
            aoi_area_ha, aoi_area_method = compute_aoi_geodesic_area_ha(geo_path)
        except Exception:
            aoi_area_ha = None
            aoi_area_method = ""

    fallback_dummy = (
        None
        if (args.enable_hansen_post_2020_loss or args.jrc_gfc2020_raster)
        else args.dummy_metric
    )
    metric_rows = _parse_metric_rows(args.metric, fallback_dummy=fallback_dummy)
    if aoi_area_ha is not None:
        metric_rows.append(
            MetricRow(
                variable="aoi_area_ha",
                value=aoi_area_ha,
                unit="ha",
                source="geometry",
                notes=aoi_area_method or "",
            )
        )
        metric_rows = sorted(metric_rows, key=lambda r: r.variable)

    maaamet_top10_result = None
    maaamet_fields_used: list[str] | None = None
    maaamet_parcels_override = None
    maaamet_provider = None
    maaamet_parcels = None
    maaamet_parcels_metadata_path: Path | None = None
    maaamet_land_area_sum: float | None = None
    hansen_land_area_sum: float | None = None
    land_area_diff_ha: float | None = None
    land_area_diff_pct: float | None = None
    parcel_rows: list[dict[str, Any]] = []
    maaamet_wfs_url = os.environ.get("MAAAMET_WFS_URL")
    maaamet_wfs_layer = os.environ.get("MAAAMET_WFS_LAYER") or "kataster:ky_kehtiv"
    maaamet_parcel_limit = None
    env_parcel_limit = os.environ.get("EUDR_DMI_MAAAMET_PARCEL_LIMIT", "").strip()
    maaamet_top10_limit = None
    env_top10_limit = os.environ.get("EUDR_DMI_MAAAMET_TOP10_LIMIT", "").strip()
    if env_parcel_limit:
        try:
            parsed_limit = int(env_parcel_limit)
            if parsed_limit > 0:
                maaamet_parcel_limit = parsed_limit
        except ValueError:
            print(
                f"WARNING: invalid EUDR_DMI_MAAAMET_PARCEL_LIMIT='{env_parcel_limit}' (ignored)",
                flush=True,
            )
    if env_top10_limit:
        try:
            parsed_limit = int(env_top10_limit)
            if parsed_limit > 0:
                maaamet_top10_limit = parsed_limit
        except ValueError:
            print(
                f"WARNING: invalid EUDR_DMI_MAAAMET_TOP10_LIMIT='{env_top10_limit}' (ignored)",
                flush=True,
            )
    if geo_kind == "geojson":
        from eudr_dmi_gil.analysis.maaamet_validation import (
            LocalFileMaaAmetProvider,
            ParcelFeature,
            WfsMaaAmetProvider,
            run_maaamet_top10,
        )

        provider = None
        env_path = os.environ.get("EUDR_DMI_MAAAMET_LOCAL_PATH")
        if env_path:
            provider = LocalFileMaaAmetProvider(Path(env_path))
        elif maaamet_wfs_url:
            provider = WfsMaaAmetProvider(maaamet_wfs_url, maaamet_wfs_layer)

        maaamet_provider = provider
        if provider is not None:
            with _timed("maaamet_fetch_parcels"):
                maaamet_parcels = provider.fetch_parcel_features(aoi_geojson_path=geo_path)
            if maaamet_parcel_limit is not None and maaamet_parcels is not None:
                original_count = len(maaamet_parcels)
                if original_count > maaamet_parcel_limit:
                    maaamet_parcels = maaamet_parcels[:maaamet_parcel_limit]
                    print(
                        "Maa-amet parcel limit applied: "
                        f"{maaamet_parcel_limit}/{original_count}",
                        flush=True,
                    )
            designation_counts = land_use_designation_counts(maaamet_parcels)
            if designation_counts:
                sorted_counts = sorted(
                    designation_counts.items(),
                    key=lambda kv: (-kv[1], kv[0]),
                )
                unique_count = len(sorted_counts)
                preview = ", ".join(
                    f"{name} ({count})" for name, count in sorted_counts[:15]
                )
                print(
                    "Maa-amet land-use designations: "
                    f"{unique_count} (expected 9). Top: {preview}",
                    flush=True,
                )

    forest_loss_threshold_ha = 0.0
    forest_loss_percent_of_aoi: float | None = None
    forest_loss_status = "na"

    hansen_result = None
    hansen_analysis = None
    hansen_methodology_block: dict[str, Any] | None = None
    hansen_computed_block: dict[str, Any] | None = None
    hansen_computed_outputs_block: dict[str, Any] | None = None
    hansen_acceptance_criteria_block: dict[str, Any] | None = None
    hansen_result_block: dict[str, Any] | None = None
    hansen_external_dependencies: list[dict[str, Any]] | None = None
    forest_metrics_block: dict[str, Any] | None = None
    forest_metrics_params_block: dict[str, Any] | None = None
    forest_metrics_debug_block: dict[str, Any] | None = None
    jrc_analysis = None
    commodity_analysis = None
    hansen_canopy_analysis = None
    hansen_canopy_commodity_metrics = None
    if args.enable_hansen_post_2020_loss:
        from eudr_dmi_gil.analysis.forest_loss_post_2020 import run_forest_loss_post_2020
        from eudr_dmi_gil.tasks.forest_loss_post_2020 import load_hansen_config

        if geo_kind != "geojson":
            raise RuntimeError("Hansen post-2020 loss requires --aoi-geojson input")

        hansen_config = load_hansen_config(
            tile_dir=Path(args.hansen_tile_dir) if args.hansen_tile_dir else None,
            canopy_threshold_percent=args.hansen_canopy_threshold,
            cutoff_year=args.hansen_cutoff_year,
            end_year=args.end_year,
            write_masks=True,
            aoi_geojson_path=geo_path,
            minio_cache_enabled=args.hansen_minio_cache,
            reproject_to_projected=args.hansen_reproject_to_projected,
            projected_crs=args.hansen_projected_crs,
        )
        if maaamet_parcels is not None:
            end_year = args.end_year or infer_hansen_latest_year(
                dataset_version=hansen_config.dataset_version,
                tile_dir=hansen_config.tile_dir,
            )
            hansen_stats_parcels = maaamet_parcels
            if args.hansen_parcel_top_n > 0:
                eligible_for_topn = [
                    p for p in maaamet_parcels if (getattr(p, "forest_area_ha", None) or 0.0) >= 3.0
                ]
                ranked_for_topn = sorted(eligible_for_topn, key=_parcel_reference_sort_key)
                hansen_stats_parcels = ranked_for_topn[: args.hansen_parcel_top_n]
                print(
                    "Hansen parcel stats scope: "
                    f"top-{args.hansen_parcel_top_n} parcels by reference forest area "
                    f"({len(hansen_stats_parcels)}/{len(maaamet_parcels)})",
                    flush=True,
                )
            with _timed("hansen_parcel_stats"):
                hansen_stats = compute_hansen_parcel_stats(
                    parcels=hansen_stats_parcels,
                    tile_dir=hansen_config.tile_dir,
                    canopy_threshold_percent=hansen_config.canopy_threshold_percent,
                    end_year=end_year,
                    cutoff_year=hansen_config.cutoff_year,
                )
            updated_parcels = []
            for parcel in hansen_stats_parcels:
                stat = hansen_stats.get(parcel.parcel_id)
                updated_parcels.append(
                    ParcelFeature(
                        parcel_id=parcel.parcel_id,
                        forest_area_ha=parcel.forest_area_ha,
                        reference_source=parcel.reference_source,
                        reference_method=parcel.reference_method,
                        properties=parcel.properties,
                        geometry=parcel.geometry,
                        pindala_m2=parcel.pindala_m2,
                        geodesic_area_ha=parcel.geodesic_area_ha,
                        maaamet_land_area_ha=parcel.maaamet_land_area_ha,
                        maaamet_forest_area_ha=parcel.maaamet_forest_area_ha,
                        hansen_land_area_ha=None if stat is None else round(stat.hansen_land_area_ha, 6),
                        hansen_forest_area_ha=None if stat is None else round(stat.hansen_forest_area_ha, 6),
                        hansen_forest_loss_ha=None if stat is None else round(stat.hansen_forest_loss_ha, 6),
                        fields_considered=parcel.fields_considered,
                        forest_area_key_used=parcel.forest_area_key_used,
                    )
                )
            hansen_land_area_sum = sum(
                p.hansen_land_area_ha
                for p in updated_parcels
                if p.hansen_land_area_ha is not None
            )
            maaamet_land_area_sum = sum(
                p.maaamet_land_area_ha
                for p in updated_parcels
                if p.maaamet_land_area_ha is not None
            )
            if maaamet_land_area_sum and hansen_land_area_sum is not None:
                land_area_diff_ha = hansen_land_area_sum - maaamet_land_area_sum
                land_area_diff_pct = (
                    land_area_diff_ha / maaamet_land_area_sum * 100.0
                    if maaamet_land_area_sum > 0
                    else None
                )
            maaamet_top10_parcels = updated_parcels
            if maaamet_top10_limit is not None:
                original_count = len(updated_parcels)
                if original_count > maaamet_top10_limit:
                    maaamet_top10_parcels = updated_parcels[:maaamet_top10_limit]
                    print(
                        "Maa-amet top10 subset limit applied: "
                        f"{maaamet_top10_limit}/{original_count}",
                        flush=True,
                    )
            with _timed("maaamet_top10"):
                maaamet_top10_result = run_maaamet_top10(
                    aoi_geojson_path=geo_path,
                    output_dir=bdir / "reports" / "aoi_report_v2" / aoi_id / "maaamet",
                    parcels_override=maaamet_top10_parcels,
                    min_forest_ha=3.0,
                    prefer_hansen=True,
                )
        hansen_output_dir = bdir / "reports" / "aoi_report_v2" / aoi_id / "hansen"
        with _timed("hansen_forest_loss_post_2020"):
            hansen_analysis = run_forest_loss_post_2020(
                aoi_geojson_path=geo_path,
                output_dir=hansen_output_dir,
                config=hansen_config,
                aoi_id=aoi_id,
                run_id=bundle_id,
                zone_geom_wgs84=maaamet_top10_result.union_geom
                if maaamet_top10_result is not None
                else None,
                parcel_ids=maaamet_top10_result.parcel_ids
                if maaamet_top10_result is not None
                else None,
            )
        hansen_result = hansen_analysis.raw

    jrc_raster_arg = str(args.jrc_gfc2020_raster or "").strip()
    hansen_lossyear_raster_arg = str(args.hansen_lossyear_raster or "").strip()
    if jrc_raster_arg:
        if geo_kind != "geojson":
            raise RuntimeError("JRC GFC2020 baseline calculation requires --aoi-geojson input")
        if not hansen_lossyear_raster_arg:
            raise RuntimeError(
                "--hansen-lossyear-raster is required when --jrc-gfc2020-raster is provided"
            )

        from eudr_dmi_gil.analysis.jrc_post2020_loss import (
            build_hansen_lossyear_metadata,
            compute_jrc_post2020_loss,
        )
        from eudr_dmi_gil.providers.jrc_gfc2020 import LocalJrcGfc2020Provider

        jrc_raster_path = Path(jrc_raster_arg)
        hansen_lossyear_raster_path = Path(hansen_lossyear_raster_arg)
        if not jrc_raster_path.is_file():
            raise FileNotFoundError(f"JRC GFC2020 raster not found: {jrc_raster_path}")
        if not hansen_lossyear_raster_path.is_file():
            raise FileNotFoundError(
                f"Hansen lossyear raster not found: {hansen_lossyear_raster_path}"
            )

        loss_dataset_version = os.environ.get(
            "EUDR_DMI_HANSEN_DATASET_VERSION", DATASET_VERSION_DEFAULT
        )
        loss_latest_year = args.loss_dataset_end_year or infer_hansen_latest_year(
            dataset_version=loss_dataset_version,
            tile_dir=hansen_lossyear_raster_path.parent,
        )
        requested_end_year = args.end_year or loss_latest_year
        if requested_end_year < 2021:
            raise ValueError("--end-year must be >= 2021")

        baseline_provider = LocalJrcGfc2020Provider(
            jrc_raster_path,
            processed_at_utc=generated_at_utc,
        )
        loss_metadata = build_hansen_lossyear_metadata(
            raster_path=hansen_lossyear_raster_path,
            dataset_version=loss_dataset_version,
            latest_available_year=loss_latest_year,
            processed_at_utc=generated_at_utc,
            source_url=resolve_hansen_url_template(),
            asset_identifier=(
                "UMD/hansen/global_forest_change_"
                + loss_dataset_version.replace("-", "_").replace(".", "_")
            ),
        )
        jrc_output_dir = bdir / "reports" / "aoi_report_v2" / aoi_id / "jrc_gfc2020"
        with _timed("jrc_gfc2020_post2020_loss"):
            jrc_analysis = compute_jrc_post2020_loss(
                aoi_geojson_path=geo_path,
                jrc_gfc2020_raster_path=jrc_raster_path,
                hansen_lossyear_raster_path=hansen_lossyear_raster_path,
                output_dir=jrc_output_dir,
                baseline_metadata=baseline_provider.metadata(),
                loss_metadata=loss_metadata,
                requested_end_year=requested_end_year,
                target_crs=args.analysis_target_crs,
                target_resolution_m=args.analysis_target_resolution_m,
            )

        canonical_metric_names = set(jrc_analysis.metrics.to_metric_rows())
        metric_rows = [r for r in metric_rows if r.variable not in canonical_metric_names]
        metric_rows.extend(
            MetricRow(
                variable=name,
                value=entry["value"],
                unit=entry["unit"],
                source="jrc_gfc2020+hansen_lossyear",
                notes=str(entry.get("notes", "")),
            )
            for name, entry in jrc_analysis.metrics.to_metric_rows().items()
        )
        metric_rows = sorted(metric_rows, key=lambda r: r.variable)

    if commodity_config is not None:
        if geo_kind != "geojson":
            raise RuntimeError("Commodity assessment requires --aoi-geojson input")
        if jrc_analysis is None:
            raise RuntimeError(
                "Commodity assessment requires JRC GFC2020 and Hansen lossyear analysis inputs"
            )
        commodity_output_dir = (
            bdir / "reports" / "aoi_report_v2" / aoi_id / "commodity" / commodity_config.id
        )
        with _timed("commodity_assessment"):
            commodity_analysis = run_commodity_assessment(
                config=commodity_config,
                aoi_geojson_path=geo_path,
                aoi_country=aoi_country_from_geojson(geo_path),
                jrc_gfc2020_raster_path=Path(jrc_raster_arg),
                hansen_lossyear_raster_path=Path(hansen_lossyear_raster_arg),
                output_dir=commodity_output_dir,
                baseline_metadata=jrc_analysis.baseline_metadata,
                loss_metadata=jrc_analysis.loss_metadata,
                requested_end_year=jrc_analysis.metrics.requested_end_year,
                target_crs=args.analysis_target_crs,
                target_resolution_m=args.analysis_target_resolution_m,
            )
        canonical_metric_names = set(commodity_analysis.metrics.to_metric_rows())
        metric_rows = [r for r in metric_rows if r.variable not in canonical_metric_names]
        metric_rows.extend(
            MetricRow(
                variable=name,
                value=entry["value"],
                unit=entry["unit"],
                source=f"commodity:{commodity_config.provider}",
                notes=str(entry.get("notes", "")),
            )
            for name, entry in commodity_analysis.metrics.to_metric_rows().items()
        )
        metric_rows = sorted(metric_rows, key=lambda r: r.variable)

    hansen_treecover_raster_arg = str(args.hansen_treecover2000_raster or "").strip()
    if hansen_treecover_raster_arg:
        if geo_kind != "geojson":
            raise RuntimeError("Hansen canopy baseline calculation requires --aoi-geojson input")
        if jrc_analysis is None:
            raise RuntimeError(
                "--hansen-treecover2000-raster requires --jrc-gfc2020-raster and "
                "--hansen-lossyear-raster to also be provided"
            )

        from eudr_dmi_gil.analysis.hansen_canopy_post2020_loss import (
            compute_hansen_canopy_commodity_overlap,
            compute_hansen_canopy_post2020_loss,
        )
        from eudr_dmi_gil.providers.hansen_treecover2000 import LocalHansenTreecoverProvider

        hansen_treecover_raster_path = Path(hansen_treecover_raster_arg)
        if not hansen_treecover_raster_path.is_file():
            raise FileNotFoundError(
                f"Hansen treecover2000 raster not found: {hansen_treecover_raster_path}"
            )

        hansen_treecover_provider = LocalHansenTreecoverProvider(
            hansen_treecover_raster_path,
            canopy_threshold_percent=args.hansen_canopy_threshold,
            dataset_version=loss_dataset_version,
            processed_at_utc=generated_at_utc,
        )
        hansen_canopy_output_dir = bdir / "reports" / "aoi_report_v2" / aoi_id / "hansen_canopy"
        with _timed("hansen_canopy_post2020_loss"):
            hansen_canopy_analysis = compute_hansen_canopy_post2020_loss(
                aoi_geojson_path=geo_path,
                hansen_treecover_raster_path=hansen_treecover_raster_path,
                hansen_lossyear_raster_path=hansen_lossyear_raster_path,
                output_dir=hansen_canopy_output_dir,
                baseline_metadata=hansen_treecover_provider.metadata(),
                loss_metadata=loss_metadata,
                requested_end_year=jrc_analysis.metrics.requested_end_year,
                canopy_threshold_percent=args.hansen_canopy_threshold,
                target_crs=args.analysis_target_crs,
                target_resolution_m=args.analysis_target_resolution_m,
            )

        canonical_metric_names = set(hansen_canopy_analysis.metrics.to_metric_rows())
        metric_rows = [r for r in metric_rows if r.variable not in canonical_metric_names]
        metric_rows.extend(
            MetricRow(
                variable=name,
                value=entry["value"],
                unit=entry["unit"],
                source="hansen_treecover2000+hansen_lossyear",
                notes=str(entry.get("notes", "")),
            )
            for name, entry in hansen_canopy_analysis.metrics.to_metric_rows().items()
        )

        if commodity_config is not None and commodity_analysis is not None:
            hansen_canopy_commodity_metrics = compute_hansen_canopy_commodity_overlap(
                config=commodity_config,
                aoi_geojson_path=geo_path,
                aoi_country=aoi_country_from_geojson(geo_path),
                hansen_canopy_result=hansen_canopy_analysis,
                output_dir=bdir
                / "reports"
                / "aoi_report_v2"
                / aoi_id
                / "commodity"
                / commodity_config.id,
            )
            if hansen_canopy_commodity_metrics is not None:
                canonical_metric_names = set(hansen_canopy_commodity_metrics.to_metric_rows())
                metric_rows = [r for r in metric_rows if r.variable not in canonical_metric_names]
                metric_rows.extend(
                    MetricRow(
                        variable=name,
                        value=entry["value"],
                        unit=entry["unit"],
                        source=f"commodity:{commodity_config.provider}+hansen_treecover2000",
                        notes=str(entry.get("notes", "")),
                    )
                    for name, entry in hansen_canopy_commodity_metrics.to_metric_rows().items()
                )
        metric_rows = sorted(metric_rows, key=lambda r: r.variable)

    if maaamet_top10_result is None and geo_kind == "geojson":
        maaamet_top10_parcels = maaamet_parcels
        if maaamet_top10_limit is not None and maaamet_parcels is not None:
            original_count = len(maaamet_parcels)
            if original_count > maaamet_top10_limit:
                maaamet_top10_parcels = maaamet_parcels[:maaamet_top10_limit]
                print(
                    "Maa-amet top10 subset limit applied: "
                    f"{maaamet_top10_limit}/{original_count}",
                    flush=True,
                )
        with _timed("maaamet_top10"):
            maaamet_top10_result = run_maaamet_top10(
                aoi_geojson_path=geo_path,
                output_dir=bdir / "reports" / "aoi_report_v2" / aoi_id / "maaamet",
                provider=maaamet_provider,
                parcels_override=maaamet_top10_parcels,
            )

    if maaamet_top10_result is not None:
        maaamet_metadata_dir = bdir / "reports" / "aoi_report_v2" / aoi_id / "maaamet"
        maaamet_parcels_metadata_path = (
            maaamet_metadata_dir / "maaamet_parcels_metadata.json"
        )
        write_json(
            maaamet_parcels_metadata_path,
            _build_maaamet_parcel_metadata(maaamet_top10_result.parcels),
        )

    if maaamet_top10_result is not None:
        maaamet_fields_used = maaamet_top10_result.fields_used
        maaamet_parcels_override = maaamet_top10_result.parcels_all

    if hansen_result is not None:
        if aoi_area_ha:
            forest_loss_percent_of_aoi = (
                hansen_result.forest_loss_post_2020_ha / aoi_area_ha
            ) * 100.0
            forest_loss_status = (
                "pass"
                if hansen_result.forest_loss_post_2020_ha <= forest_loss_threshold_ha
                else "fail"
            )

        metric_rows.extend(
            [
                MetricRow(
                    variable="pixel_forest_loss_post_2020_ha",
                    value=hansen_result.forest_loss_post_2020_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="pixel_mask",
                ),
                MetricRow(
                    variable="pixel_initial_tree_cover_ha",
                    value=hansen_result.initial_tree_cover_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="legacy/deprecated baseline: Hansen treecover2000 pixel_mask",
                ),
                MetricRow(
                    variable="pixel_current_tree_cover_ha",
                    value=hansen_result.current_tree_cover_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="legacy/deprecated Hansen treecover2000 remaining forest pixel_mask",
                ),
                MetricRow(
                    variable="rfm_area_ha",
                    value=hansen_analysis.raw.forest_metrics.rfm_area_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="legacy/deprecated baseline: rfm_mask from Hansen treecover2000",
                ),
                MetricRow(
                    variable="loss_total_ha",
                    value=hansen_analysis.raw.forest_metrics.loss_total_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="rfm_mask & (lossyear > 0)",
                ),
                MetricRow(
                    variable=f"loss_2021_{hansen_analysis.raw.forest_metrics.end_year}_ha",
                    value=hansen_analysis.raw.forest_metrics.loss_2021_2024_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes=f"rfm_mask & (lossyear in 21..{hansen_analysis.raw.forest_metrics.end_year - 2000})",
                ),
                MetricRow(
                    variable=f"forest_{hansen_analysis.raw.forest_metrics.end_year}_ha",
                    value=hansen_analysis.raw.forest_metrics.forest_end_year_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="rfm_mask & (lossyear == 0)",
                ),
                MetricRow(
                    variable="forest_end_year_ha",
                    value=hansen_analysis.raw.forest_metrics.forest_end_year_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="forest_mask_end_year",
                ),
                MetricRow(
                    variable="end_year",
                    value=hansen_analysis.raw.forest_metrics.end_year,
                    unit="year",
                    source="hansen_gfc",
                    notes="forest_end_year",
                ),
            ]
        )
        if forest_loss_percent_of_aoi is not None:
            metric_rows.append(
                MetricRow(
                    variable="forest_loss_post_2020_percent_of_aoi",
                    value=forest_loss_percent_of_aoi,
                    unit="percent",
                    source="hansen_gfc",
                    notes="forest_loss_post_2020_ha / aoi_area_ha",
                )
            )
        metric_rows = sorted(metric_rows, key=lambda r: r.variable)

        hansen_methodology_block = {
            "forest_loss_post_2020": {
                "data_sources": ["hansen_global_forest_change"],
                "dataset_version": hansen_config.dataset_version,
                "forest_definition": {
                    "tree_cover_threshold_percent": hansen_config.canopy_threshold_percent
                },
                "calculation": {
                    "method": "pixel_wise_intersection",
                    "cutoff_date": f"{hansen_config.cutoff_year}-12-31",
                    "cutoff_rule": "lossyear > (cutoff_year - 2000)",
                    "area_units": "ha",
                },
                "resolution": {"pixel_size_m": 30},
                "tile_source": hansen_config.tile_source,
                "tile_source_url_template": hansen_config.url_template,
                "is_placeholder": False,
            }
        }
        if maaamet_wfs_url:
            hansen_methodology_block["forest_loss_post_2020"].setdefault(
                "calculation_run_metadata", {}
            ).update(
                {
                    "maaamet_wfs_url": maaamet_wfs_url,
                    "maaamet_wfs_layer": maaamet_wfs_layer,
                }
            )

        hansen_computed_block = {
            "forest_loss_post_2020": {
                "pixel_initial_tree_cover_ha": hansen_result.initial_tree_cover_ha,
                "pixel_forest_loss_post_2020_ha": hansen_result.forest_loss_post_2020_ha,
                "pixel_current_tree_cover_ha": hansen_result.current_tree_cover_ha,
                "mask_forest_loss_post_2020": str(
                    hansen_result.mask_forest_loss_post_2020_path.relative_to(bdir)
                ).replace("\\", "/"),
                "mask_forest_current_year": str(
                    hansen_result.mask_forest_current_path.relative_to(bdir)
                ).replace("\\", "/"),
                "mask_forest_2000": str(
                    hansen_result.mask_forest_2000_path.relative_to(bdir)
                ).replace("\\", "/"),
                "mask_forest_end_year": str(
                    hansen_result.mask_forest_end_year_path.relative_to(bdir)
                ).replace("\\", "/"),
                "tiles_manifest": str(
                    hansen_analysis.tiles_manifest_path.relative_to(bdir)
                ).replace("\\", "/"),
            }
        }

        hansen_computed_outputs_block = {
            "forest_loss_post_2020": {
                "area_ha": hansen_analysis.computed.area_ha,
                "pixel_size_m": hansen_analysis.computed.pixel_size_m,
                "mask_geojson_ref": {
                    "relpath": str(hansen_analysis.loss_mask_path.relative_to(bdir)).replace(
                        "\\", "/"
                    ),
                    "sha256": compute_sha256(hansen_analysis.loss_mask_path),
                    "content_type": "application/geo+json",
                },
                "mask_forest_2000_ref": {
                    "relpath": str(hansen_analysis.forest_2000_mask_path.relative_to(bdir)).replace(
                        "\\", "/"
                    ),
                    "sha256": compute_sha256(hansen_analysis.forest_2000_mask_path),
                    "content_type": "application/geo+json",
                },
                "mask_forest_end_year_ref": {
                    "relpath": str(
                        hansen_analysis.forest_end_year_mask_path.relative_to(bdir)
                    ).replace("\\", "/"),
                    "sha256": compute_sha256(hansen_analysis.forest_end_year_mask_path),
                    "content_type": "application/geo+json",
                },
                "tiles_manifest_ref": {
                    "relpath": str(hansen_analysis.tiles_manifest_path.relative_to(bdir)).replace(
                        "\\", "/"
                    ),
                    "sha256": compute_sha256(hansen_analysis.tiles_manifest_path),
                    "content_type": "application/json",
                },
            }
        }

        cutoff_code = max(hansen_config.cutoff_year - 2000, 0)
        first_post_cutoff_year = hansen_config.cutoff_year + 1

        hansen_acceptance_criteria_block = {
            "criteria_id": "forest_loss_post_2020_max_ha",
            "description": (
                f"Forest loss after {hansen_config.cutoff_year}-12-31 "
                f"(lossyear >= {first_post_cutoff_year}) must be <= 0 ha."
            ),
            "evidence_classes": ["forest_loss_post_2020"],
            "decision_type": "threshold",
        }
        hansen_result_block = {
            "result_id": "forest_loss_post_2020_max_ha",
            "criteria_ids": ["forest_loss_post_2020_max_ha"],
            "evidence_classes": ["forest_loss_post_2020"],
            "status": forest_loss_status,
            "observed_value": hansen_result.forest_loss_post_2020_ha,
            "threshold_value": forest_loss_threshold_ha,
            "unit": "ha",
        }

        entries = hansen_config.tile_entries or build_entries_from_provenance(
            hansen_analysis.raw.tile_provenance,
            tile_dir=hansen_config.tile_dir,
            url_template=hansen_config.url_template,
        )
        tiles_used = sorted(
            [
                {
                    "tile_id": e.tile_id,
                    "layer": e.layer,
                    "local_path": portable_hansen_local_path(e.local_path),
                    "sha256": e.sha256,
                    "size_bytes": e.size_bytes,
                    "source_url": e.source_url,
                }
                for e in entries
            ],
            key=lambda item: (item.get("tile_id", ""), item.get("layer", ""), item.get("local_path", "")),
        )

        tiles_manifest_ref = {
            "relpath": str(hansen_analysis.tiles_manifest_path.relative_to(bdir)).replace("\\", "/"),
            "sha256": compute_sha256(hansen_analysis.tiles_manifest_path),
        }

        forest_metrics = hansen_analysis.raw.forest_metrics
        forest_metrics_params = hansen_analysis.raw.forest_metrics_params
        forest_metrics_debug = hansen_analysis.raw.forest_metrics_debug
        tile_refs_treecover = [item for item in tiles_used if item.get("layer") == "treecover2000"]
        tile_refs_lossyear = [item for item in tiles_used if item.get("layer") == "lossyear"]
        forest_metrics_block = {
            "canopy_threshold_pct": forest_metrics.canopy_threshold_pct,
            "reference_forest_mask_year": forest_metrics.reference_forest_mask_year,
            "loss_year_code_basis": forest_metrics.loss_year_code_basis,
            "end_year": forest_metrics.end_year,
            "rfm_area_ha": forest_metrics.rfm_area_ha,
            "forest_end_year_area_ha": forest_metrics.forest_end_year_area_ha,
            f"forest_{forest_metrics.end_year}_ha": forest_metrics.forest_end_year_ha,
            f"loss_total_2001_{forest_metrics.end_year}_ha": forest_metrics.loss_total_2001_2024_ha,
            f"loss_2021_{forest_metrics.end_year}_ha": forest_metrics.loss_2021_2024_ha,
            f"loss_2021_{forest_metrics.end_year}_pct_of_rfm": forest_metrics.loss_2021_2024_pct_of_rfm,
            "method": {
                "area": forest_metrics_params.method_area,
                "zonal": forest_metrics_params.method_zonal,
                "notes": forest_metrics_params.method_notes,
            },
            "inputs": {
                "hansen_treecover2000": {
                    "source": "hansen_gfc_2025_v1_13",
                    "tile_refs": tile_refs_treecover,
                    "hash": tiles_manifest_ref["sha256"],
                    "tiles_manifest_ref": tiles_manifest_ref,
                },
                "hansen_lossyear": {
                    "source": "hansen_gfc_2025_v1_13",
                    "tile_refs": tile_refs_lossyear,
                    "hash": tiles_manifest_ref["sha256"],
                    "tiles_manifest_ref": tiles_manifest_ref,
                },
            },
        }
        forest_metrics_params_block = {
            "canopy_threshold_pct": forest_metrics_params.canopy_threshold_pct,
            "start_year": forest_metrics_params.start_year,
            "end_year": forest_metrics_params.end_year,
            "crs": forest_metrics_params.crs,
            "area_method": forest_metrics_params.method_area,
            "lossyear_mapping": (
                f"0=no_loss; 1..{forest_metrics_params.end_year - 2000}="
                f"2001..{forest_metrics_params.end_year} (year=lossyear+2000)"
            ),
            "method": {
                "area": forest_metrics_params.method_area,
                "zonal": forest_metrics_params.method_zonal,
                "notes": forest_metrics_params.method_notes,
            },
            "loss_year_code_basis": forest_metrics_params.loss_year_code_basis,
        }
        forest_metrics_debug_block = {
            "raster_shapes": forest_metrics_debug.raster_shapes,
            "pixel_area_m2": {
                "min": forest_metrics_debug.pixel_area_m2_min,
                "max": forest_metrics_debug.pixel_area_m2_max,
                "mean": forest_metrics_debug.pixel_area_m2_mean,
            },
            "mask_true_pixels": {
                "rfm": forest_metrics_debug.rfm_true_pixels,
                "loss_2021_end_year": forest_metrics_debug.loss_21_24_true_pixels,
                "forest_end_year": forest_metrics_debug.forest_end_year_true_pixels,
            },
            "areas_ha": {
                "rfm": forest_metrics_debug.rfm_area_ha,
                f"loss_total_2001_{forest_metrics.end_year}": forest_metrics_debug.loss_total_2001_2024_ha,
                "loss_total": forest_metrics_debug.loss_total_ha,
                f"loss_2021_{forest_metrics.end_year}": forest_metrics_debug.loss_2021_2024_ha,
                "forest_end_year": forest_metrics_debug.forest_end_year_area_ha,
                f"forest_{forest_metrics.end_year}": forest_metrics_debug.forest_end_year_ha,
                "forest_end_year_ha": forest_metrics_debug.forest_end_year_ha,
            },
        }

        hansen_external_dependencies = [
            {
                "dependency_id": "hansen_gfc_2025_v1_13",
                "dataset_version": hansen_config.dataset_version,
                "tile_source": hansen_config.tile_source,
                "aoi_geojson_sha256": geo_sha,
                "tiles_manifest": {
                    "relpath": str(hansen_analysis.tiles_manifest_path.relative_to(bdir)).replace(
                        "\\", "/"
                    ),
                    "sha256": compute_sha256(hansen_analysis.tiles_manifest_path),
                },
                "tiles_used": tiles_used,
            }
        ]

    policy_mapping_refs = policy_mapping_refs or [
        "policy-spine:eudr/article-3",
        "policy-spine:eudr/article-9",
    ]

    datasets: list[dict[str, Any]] = [
        {
            "dataset_id": "aoi_geometry_input",
            "version": "user_supplied",
            "retrieved_at_utc": generated_at_utc,
            "license": "user_supplied",
            "source_url": geo_rel.as_posix(),
        }
    ]
    if hansen_result is not None:
        datasets.append(
            {
                "dataset_id": "hansen_gfc_2025_v1_13",
                "version": hansen_config.dataset_version,
                "retrieved_at_utc": generated_at_utc,
                "license": "Hansen GFC (public)",
                "source_url": "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2025-v1.13/",
            }
        )
    if jrc_analysis is not None:
        datasets.extend(
            [
                {
                    "dataset_id": jrc_analysis.baseline_metadata.dataset_id,
                    "version": jrc_analysis.baseline_metadata.dataset_version,
                    "retrieved_at_utc": generated_at_utc,
                    "license": "see JRC catalogue/source terms",
                    "source_url": jrc_analysis.baseline_metadata.source_url,
                },
                {
                    "dataset_id": "hansen_lossyear",
                    "version": jrc_analysis.loss_metadata.dataset_version,
                    "retrieved_at_utc": generated_at_utc,
                    "license": "Hansen GFC (public)",
                    "source_url": jrc_analysis.loss_metadata.source_url,
                },
            ]
        )
    if commodity_analysis is not None:
        datasets.append(
            {
                "dataset_id": commodity_analysis.metadata.dataset_id,
                "version": commodity_analysis.metadata.dataset_version,
                "retrieved_at_utc": generated_at_utc,
                "license": "see configured provider/source terms",
                "source_url": (
                    commodity_analysis.metadata.source_url
                    or commodity_analysis.metadata.asset_identifier
                    or commodity_analysis.metadata.local_path
                    or ""
                ),
            }
        )

    parameters: dict[str, Any] = {
        "aoi_area_method": aoi_area_method or "unknown",
        "implementation": {
            "forest_loss_post_2020": "v1",
            "git_commit": _git_commit(),
        },
    }
    if hansen_result is not None:
        cutoff_code = max(hansen_config.cutoff_year - 2000, 0)
        first_post_cutoff_year = hansen_config.cutoff_year + 1
        hansen_end_year = (
            hansen_analysis.raw.forest_metrics.end_year if hansen_analysis is not None else hansen_config.dataset_end_year
        )
        hansen_end_year_code = max(hansen_end_year - 2000, 0)
        parameters["forest_loss_post_2020"] = {
            "canopy_threshold_percent": hansen_config.canopy_threshold_percent,
            "cutoff_year": hansen_config.cutoff_year,
            "acceptance_threshold_ha": forest_loss_threshold_ha,
            "pixel_area_method": (
                hansen_analysis.raw.forest_metrics_params.method_area
                if hansen_analysis is not None
                else "unknown"
            ),
            "area_method": (
                hansen_analysis.raw.forest_metrics_params.method_area
                if hansen_analysis is not None
                else "unknown"
            ),
            "lossyear_mapping": (
                "0=no_loss; 1..end_year_code=2001..end_year "
                "(year=lossyear+2000)"
            ),
            "mask_definitions": {
                "rfm_mask": "treecover2000 >= canopy_threshold_percent",
                "loss_total": "rfm_mask & (lossyear > 0)",
                "loss_post_cutoff": (
                    f"rfm_mask & (lossyear > {cutoff_code}) "
                    f"[{first_post_cutoff_year}+]"
                ),
                f"loss_2021_{hansen_end_year}": (
                    f"rfm_mask & (lossyear in 21..{hansen_end_year_code})"
                ),
                f"forest_{hansen_end_year}": "rfm_mask & (lossyear == 0)",
            },
        }
        if hansen_analysis is not None:
            parameters["forest_loss_post_2020"].update(
                {
                    "end_year": hansen_analysis.raw.forest_metrics.end_year,
                    "forest_end_year_ha": hansen_analysis.raw.forest_metrics.forest_end_year_ha,
                }
            )

    if jrc_analysis is not None:
        parameters["assessment_end_year"] = {
            "requested_end_year": jrc_analysis.metrics.requested_end_year,
            "effective_end_year": jrc_analysis.metrics.effective_end_year,
            "evidence_start_year": jrc_analysis.metrics.evidence_start_year,
            "resolution_mode": (
                "user_requested_available"
                if jrc_analysis.metrics.requested_end_year
                == jrc_analysis.metrics.effective_end_year
                else "capped_to_loss_dataset_temporal_coverage"
            ),
        }
        parameters["post_2020_loss_on_2020_forest"] = {
            "baseline_provider_id": jrc_analysis.baseline_metadata.provider_id,
            "baseline_dataset_id": jrc_analysis.baseline_metadata.dataset_id,
            "baseline_asset_identifier": jrc_analysis.baseline_metadata.asset_identifier,
            "baseline_cutoff_date": jrc_analysis.baseline_metadata.cutoff_date,
            "baseline_band": jrc_analysis.baseline_metadata.band,
            "baseline_forest_value": jrc_analysis.baseline_metadata.forest_value,
            "loss_dataset_id": jrc_analysis.loss_metadata.dataset_id,
            "loss_dataset_version": jrc_analysis.loss_metadata.dataset_version,
            "loss_dataset_latest_available_year": (
                jrc_analysis.loss_metadata.latest_available_year
            ),
            "target_crs": jrc_analysis.grid["target_crs"],
            "target_resolution_m": jrc_analysis.grid["target_resolution_m"],
            "resampling": "nearest",
            "boundary_rule": jrc_analysis.grid["boundary_rule"],
            "area_method": "equal_area_projected_pixel_count",
            "mask_definition": (
                "AOI & jrc_forest_2020 & lossyear>=21 "
                "& lossyear<=effective_end_year-2000"
            ),
        }
    if commodity_analysis is not None:
        parameters["commodity"] = {
            "id": commodity_analysis.config.id,
            "display_name": commodity_analysis.config.display_name,
            "provider": commodity_analysis.config.provider,
            "mode": commodity_analysis.config.mode,
            "dataset_title": commodity_analysis.config.dataset_title,
            "dataset_version": commodity_analysis.config.dataset_version,
            "asset_id": commodity_analysis.config.asset_id,
            "local_path": commodity_analysis.config.local_path,
            "class_values": list(commodity_analysis.config.class_values),
            "class_labels": list(commodity_analysis.config.class_labels),
            "probability_band": commodity_analysis.config.probability_band,
            "threshold": commodity_analysis.config.threshold,
            "sensitivity_thresholds": list(commodity_analysis.config.sensitivity_thresholds),
            "observation_year": commodity_analysis.config.observation_year,
            "country_scope": list(commodity_analysis.config.country_scope),
            "optional": commodity_analysis.config.optional,
        }

    results_summary: dict[str, Any] = {
        "aoi_area": {
            "area_ha": aoi_area_ha if aoi_area_ha is not None else 0.0,
            "method": aoi_area_method or "unknown",
        }
    }
    if hansen_result is not None and forest_loss_percent_of_aoi is not None:
        results_summary["deforestation_free_post_2020"] = {
            "forest_loss_post_2020_ha": hansen_result.forest_loss_post_2020_ha,
            "percent_of_aoi": forest_loss_percent_of_aoi,
            "threshold_ha": forest_loss_threshold_ha,
            "status": forest_loss_status,
            "uncertainty": {
                "pixel_area_method": (
                    hansen_analysis.raw.forest_metrics_params.method_area
                    if hansen_analysis is not None
                    else "unknown"
                ),
                "nodata": "masked_as_no_loss",
                "projection": "EPSG:4326",
                "conservative_bounds": "area estimates are lower-bound for masked/no-data pixels",
            },
        }
    if commodity_analysis is not None:
        results_summary["commodity_assessment"] = {
            "status_messages": commodity_analysis.status_messages,
            "coverage_status": commodity_analysis.coverage_status,
            "evidence_available": commodity_analysis.evidence_available,
            "post_2020_loss_and_commodity_overlap_ha": (
                commodity_analysis.metrics.post_2020_loss_and_commodity_overlap_ha
            ),
        }

    policy_mapping: list[dict[str, Any]] = [
        {
            "article_ref": "EUDR Article 9",
            "requirement": "AOI geometry is declared and traceable",
            "evidence_fields": ["aoi_geometry_ref", "inputs.sources"],
            "artifact_relpaths": [geo_rel.as_posix()],
            "status": "pass",
        }
    ]
    if hansen_result is not None and hansen_analysis is not None:
        policy_mapping.append(
            {
                "article_ref": "EUDR Article 3",
                "requirement": "Deforestation-free after 2020-01-31",
                "evidence_fields": [
                    "results_summary.deforestation_free_post_2020",
                    "computed.forest_loss_post_2020.pixel_forest_loss_post_2020_ha",
                ],
                "artifact_relpaths": [
                    str(hansen_analysis.loss_mask_path.relative_to(bdir)).replace("\\", "/"),
                    str(hansen_analysis.tiles_manifest_path.relative_to(bdir)).replace("\\", "/"),
                ],
                "status": forest_loss_status,
            }
        )

    in_scope_articles = ["article-3"] if hansen_result is not None else []
    report: dict[str, Any] = {
        "report_version": "aoi_report_v2",
        "generated_at_utc": generated_at_utc,
        "bundle_id": bundle_id,
        "report_metadata": {
            "report_type": "example",
            "regulatory_context": {
                "regulation": "EUDR",
                "in_scope_articles": in_scope_articles,
                "out_of_scope_articles": [],
            },
            "assessment_capability": "inspectable_only",
        },
        "computed": {},
        "computed_outputs": {},
        "validation": {
            "maaamet": {
                "enabled": False,
                "parcel_layer": "kataster:ky_kehtiv",
                "parcel_count": None,
                "cadastral_forest_ha_sum": None,
                "pixel_forest_ha_sum": None,
                "rel_diff_pct": None,
                "notes": "Populate when Maa-amet WFS integration is enabled in pipeline.",
            }
        },
        "methodology": {},
        "external_dependencies": [],
        "aoi_id": aoi_id,
        "aoi_geometry_ref": {
            "kind": geo_kind,
            "value": geo_rel.as_posix(),
            "sha256": geo_sha,
        },
        "inputs": {
            "sources": [
                {
                    "source_id": "aoi_geometry",
                    "sha256": geo_sha,
                    "uri": geo_rel.as_posix(),
                    "content_type": "application/geo+json" if geo_kind == "geojson" else "text/plain",
                }
            ]
        },
        "metrics": _metrics_from_rows(metric_rows),
        "evidence_artifacts": [],
        "parameters": parameters,
        "datasets": datasets,
        "policy_mapping": policy_mapping,
        "results_summary": results_summary,
        "evidence_registry": {
            "evidence_classes": [
                {
                    "class_id": "aoi_geometry",
                    "mandatory": True,
                    "status": "present",
                }
            ]
        },
        "acceptance_criteria": [
            {
                "criteria_id": "aoi_geometry_present",
                "description": "AOI geometry is present and referenced in inputs.",
                "evidence_classes": ["aoi_geometry"],
                "decision_type": "presence",
            }
        ],
        "results": [
            {
                "result_id": "result-001",
                "criteria_ids": ["aoi_geometry_present"],
                "status": "pass",
            }
        ],
        "assumptions": [],
        "regulatory_traceability": [
            {
                "regulation": "EUDR",
                "article_ref": "article-3",
                "evidence_class": "aoi_geometry",
                "acceptance_criteria": "aoi_geometry_present",
                "result_ref": "result-001",
            }
        ],
        "policy_mapping_refs": policy_mapping_refs,
        "extensions": {
            "metrics_rows_v1": [
                {
                    "variable": r.variable,
                    "value": r.value,
                    "unit": r.unit,
                    "source": r.source,
                    "notes": r.notes,
                }
                for r in metric_rows
            ]
        },
    }

    if commodity_analysis is not None:
        report["commodity"] = commodity_report_block(commodity_analysis)

    if maaamet_top10_result is not None:
        maaamet_block = report["validation"]["maaamet"]
        maaamet_block["enabled"] = True
        maaamet_block["parcel_layer"] = maaamet_wfs_layer
        maaamet_block["parcel_count"] = len(maaamet_top10_result.parcels_all)
        parcel_rows = _parcel_table_rows(maaamet_top10_result.parcels)
        maaamet_block["parcels"] = parcel_rows
        if maaamet_land_area_sum is not None:
            maaamet_block["maaamet_land_area_ha_sum"] = round(maaamet_land_area_sum, 6)
        if hansen_land_area_sum is not None:
            maaamet_block["hansen_land_area_ha_sum"] = round(hansen_land_area_sum, 6)
        if land_area_diff_ha is not None:
            maaamet_block["land_area_diff_ha"] = round(land_area_diff_ha, 6)
        if land_area_diff_pct is not None:
            maaamet_block["land_area_diff_pct"] = round(land_area_diff_pct, 6)
        maaamet_block["cadastral_forest_ha_sum"] = round(
            sum(
                p.forest_area_ha
                for p in maaamet_top10_result.parcels_all
                if p.forest_area_ha is not None
            ),
            6,
        )
        maaamet_block["pixel_forest_ha_sum"] = (
            hansen_result.current_tree_cover_ha if hansen_result is not None else None
        )
        maaamet_block["rel_diff_pct"] = None
        if maaamet_wfs_url:
            maaamet_block["notes"] = (
                f"WFS: {maaamet_wfs_url} layer={maaamet_wfs_layer}"
            )
        if maaamet_parcels_metadata_path is not None:
            maaamet_block["parcels_metadata_ref"] = {
                "relpath": str(maaamet_parcels_metadata_path.relative_to(bdir)).replace(
                    "\\", "/"
                ),
                "sha256": compute_sha256(maaamet_parcels_metadata_path),
                "content_type": "application/json",
            }

    artifact_paths: list[Path] = [geo_path]
    if maaamet_parcels_metadata_path is not None:
        artifact_paths.append(maaamet_parcels_metadata_path)
    if maaamet_top10_result is not None:
        artifact_paths.extend(
            [
                maaamet_top10_result.geojson_path,
                maaamet_top10_result.csv_path,
                maaamet_top10_result.inventory_path,
            ]
        )
    if hansen_result is not None and hansen_analysis is not None:
        if forest_metrics_block is not None:
            report["forest_metrics"] = forest_metrics_block
        if forest_metrics_params_block is not None:
            report.setdefault("extensions", {})["forest_metrics_params"] = (
                forest_metrics_params_block
            )
        if forest_metrics_debug_block is not None:
            report.setdefault("extensions", {})["forest_metrics_debug"] = (
                forest_metrics_debug_block
            )
        if hansen_methodology_block is not None:
            report["methodology"] = hansen_methodology_block
        if hansen_computed_block is not None:
            report["computed"] = hansen_computed_block
        if hansen_computed_outputs_block is not None:
            report["computed_outputs"] = hansen_computed_outputs_block
        if hansen_acceptance_criteria_block is not None:
            report["acceptance_criteria"].append(hansen_acceptance_criteria_block)
        if hansen_result_block is not None:
            report["results"].append(hansen_result_block)
            report["regulatory_traceability"].append(
                {
                    "regulation": "EUDR",
                    "article_ref": "article-3",
                    "evidence_class": "forest_loss_post_2020",
                    "acceptance_criteria": "forest_loss_post_2020_max_ha",
                    "result_ref": "forest_loss_post_2020_max_ha",
                }
            )
        if hansen_external_dependencies is not None:
            report["external_dependencies"] = hansen_external_dependencies
        if not report.get("external_dependencies"):
            fallback_entries = hansen_config.tile_entries or build_entries_from_provenance(
                hansen_analysis.raw.tile_provenance,
                tile_dir=hansen_config.tile_dir,
                url_template=hansen_config.url_template,
            )
            fallback_tiles_used = sorted(
                [
                    {
                        "tile_id": e.tile_id,
                        "layer": e.layer,
                        "local_path": portable_hansen_local_path(e.local_path),
                        "sha256": e.sha256,
                        "size_bytes": e.size_bytes,
                        "source_url": e.source_url,
                    }
                    for e in fallback_entries
                ],
                key=lambda item: (item.get("tile_id", ""), item.get("layer", ""), item.get("local_path", "")),
            )
            report["external_dependencies"] = [
                {
                    "dependency_id": "hansen_gfc_2025_v1_13",
                    "dataset_version": hansen_config.dataset_version,
                    "tile_source": hansen_config.tile_source,
                    "aoi_geojson_sha256": geo_sha,
                    "tiles_manifest": {
                        "relpath": str(hansen_analysis.tiles_manifest_path.relative_to(bdir)).replace(
                            "\\", "/"
                        ),
                        "sha256": compute_sha256(hansen_analysis.tiles_manifest_path),
                    },
                    "tiles_used": fallback_tiles_used,
                }
            ]

    if jrc_analysis is not None:
        report["methodology"]["post_2020_loss_on_2020_forest"] = {
            "data_sources": ["jrc_gfc2020", "hansen_lossyear"],
            "calculation": {
                "method": "deterministic_categorical_raster_intersection",
                "expression": (
                    "AOI AND jrc_forest_2020 AND hansen_lossyear >= 21 "
                    "AND hansen_lossyear <= effective_end_year - 2000"
                ),
                "area_units": "ha",
                "area_method": "equal_area_projected_pixel_count",
                "target_crs": jrc_analysis.grid["target_crs"],
                "target_resolution_m": jrc_analysis.grid["target_resolution_m"],
                "resampling": "nearest",
                "boundary_rule": jrc_analysis.grid["boundary_rule"],
            },
            "baseline_provider": jrc_analysis.baseline_metadata.to_dict(),
            "loss_dataset": jrc_analysis.loss_metadata.to_dict(),
            "is_placeholder": False,
        }
        report["computed"]["post_2020_loss_on_2020_forest"] = {
            key: value["value"] for key, value in jrc_analysis.metrics.to_metric_rows().items()
        }
        report["computed_outputs"]["post_2020_loss_on_2020_forest"] = {
            "summary_ref": {
                "relpath": str(jrc_analysis.summary_path.relative_to(bdir)).replace("\\", "/"),
                "sha256": compute_sha256(jrc_analysis.summary_path),
                "content_type": "application/json",
            },
            "baseline_mask_ref": {
                "relpath": str(jrc_analysis.baseline_mask_path.relative_to(bdir)).replace(
                    "\\", "/"
                ),
                "sha256": compute_sha256(jrc_analysis.baseline_mask_path),
                "content_type": "application/geo+json",
            },
            "loss_mask_ref": {
                "relpath": str(jrc_analysis.loss_mask_path.relative_to(bdir)).replace(
                    "\\", "/"
                ),
                "sha256": compute_sha256(jrc_analysis.loss_mask_path),
                "content_type": "application/geo+json",
            },
            "debug_ref": {
                "relpath": str(jrc_analysis.debug_path.relative_to(bdir)).replace("\\", "/"),
                "sha256": compute_sha256(jrc_analysis.debug_path),
                "content_type": "application/json",
            },
        }
        report["evidence_registry"]["evidence_classes"].extend(
            [
                {
                    "class_id": "jrc_forest_baseline_2020",
                    "mandatory": True,
                    "status": "present",
                },
                {
                    "class_id": "post_2020_loss_on_2020_forest",
                    "mandatory": True,
                    "status": "present",
                },
            ]
        )
        report["extensions"]["post_2020_loss_on_2020_forest"] = {
            "evidence_gaps": jrc_analysis.evidence_gaps,
            "compatibility": {
                "legacy_hansen_treecover2000_metrics_retained": hansen_result is not None,
                "legacy_baseline_metric_names": [
                    "pixel_initial_tree_cover_ha",
                    "rfm_area_ha",
                ],
                "canonical_baseline_metric": "forest_baseline_2020_ha",
            },
        }
        report["assumptions"].append(
            {
                "assumption_id": "jrc-gfc2020-baseline-evidence-layer",
                "description": (
                    "JRC Global Forest Cover 2020 is used as a forest baseline evidence "
                    "layer at the 2020-12-31 cutoff date; it is not a legal determination."
                ),
                "testable": True,
                "affects_results": [],
            }
        )

    if jrc_analysis is not None:
        artifact_paths.extend(
            [
                jrc_analysis.summary_path,
                jrc_analysis.baseline_mask_path,
                jrc_analysis.loss_mask_path,
                jrc_analysis.debug_path,
            ]
        )

    if hansen_canopy_analysis is not None:
        report["methodology"]["post_2020_loss_on_hansen10pct_canopy_baseline"] = {
            "data_sources": ["hansen_treecover2000", "hansen_lossyear"],
            "calculation": {
                "method": "deterministic_categorical_raster_intersection",
                "expression": (
                    f"AOI AND hansen_treecover2000 >= {args.hansen_canopy_threshold} "
                    "AND NOT hansen_lossyear in [1,20] AND hansen_lossyear >= 21 "
                    "AND hansen_lossyear <= effective_end_year - 2000"
                ),
                "area_units": "ha",
                "area_method": "equal_area_projected_pixel_count",
                "target_crs": hansen_canopy_analysis.grid["target_crs"],
                "target_resolution_m": hansen_canopy_analysis.grid["target_resolution_m"],
                "resampling": "nearest",
                "boundary_rule": hansen_canopy_analysis.grid["boundary_rule"],
            },
            "baseline_provider": hansen_canopy_analysis.baseline_metadata.to_dict(),
            "loss_dataset": hansen_canopy_analysis.loss_metadata.to_dict(),
            "is_placeholder": False,
            "notes": (
                "Second, parallel forest baseline (Hansen treecover2000 >= threshold, the "
                "FAO/EUDR Art.2 canopy-cover forest definition) reported alongside the "
                "JRC GFC2020 closed-canopy baseline; the two can disagree on the same AOI."
            ),
        }
        report["computed"]["post_2020_loss_on_hansen10pct_canopy_baseline"] = {
            key: value["value"]
            for key, value in hansen_canopy_analysis.metrics.to_metric_rows().items()
        }
        report["computed_outputs"]["post_2020_loss_on_hansen10pct_canopy_baseline"] = {
            "summary_ref": {
                "relpath": str(hansen_canopy_analysis.summary_path.relative_to(bdir)).replace(
                    "\\", "/"
                ),
                "sha256": compute_sha256(hansen_canopy_analysis.summary_path),
                "content_type": "application/json",
            },
            "baseline_mask_ref": {
                "relpath": str(
                    hansen_canopy_analysis.baseline_mask_path.relative_to(bdir)
                ).replace("\\", "/"),
                "sha256": compute_sha256(hansen_canopy_analysis.baseline_mask_path),
                "content_type": "application/geo+json",
            },
            "loss_mask_ref": {
                "relpath": str(hansen_canopy_analysis.loss_mask_path.relative_to(bdir)).replace(
                    "\\", "/"
                ),
                "sha256": compute_sha256(hansen_canopy_analysis.loss_mask_path),
                "content_type": "application/geo+json",
            },
            "debug_ref": {
                "relpath": str(hansen_canopy_analysis.debug_path.relative_to(bdir)).replace(
                    "\\", "/"
                ),
                "sha256": compute_sha256(hansen_canopy_analysis.debug_path),
                "content_type": "application/json",
            },
        }
        report["evidence_registry"]["evidence_classes"].extend(
            [
                {
                    "class_id": "hansen10pct_canopy_forest_baseline_2000",
                    "mandatory": False,
                    "status": "present",
                },
                {
                    "class_id": "post_2020_loss_on_hansen10pct_canopy_baseline",
                    "mandatory": False,
                    "status": "present",
                },
            ]
        )
        report["extensions"]["hansen_canopy_post2020_loss"] = {
            "evidence_gaps": hansen_canopy_analysis.evidence_gaps,
        }
        report["assumptions"].append(
            {
                "assumption_id": "hansen-treecover2000-canopy-baseline-evidence-layer",
                "description": (
                    "Hansen treecover2000 >= threshold is used as a second forest baseline "
                    "evidence layer (the FAO/EUDR Art.2 canopy-cover definition), alongside "
                    "the JRC GFC2020 closed-canopy baseline; it is not a legal determination."
                ),
                "testable": True,
                "affects_results": [],
            }
        )
        artifact_paths.extend(
            [
                hansen_canopy_analysis.summary_path,
                hansen_canopy_analysis.baseline_mask_path,
                hansen_canopy_analysis.loss_mask_path,
                hansen_canopy_analysis.debug_path,
            ]
        )

    if hansen_canopy_commodity_metrics is not None:
        report["computed"]["hansen_canopy_commodity_overlap"] = {
            key: value["value"]
            for key, value in hansen_canopy_commodity_metrics.to_metric_rows().items()
        }
        artifact_paths.append(hansen_canopy_commodity_metrics.overlap_mask_path)

    if commodity_analysis is not None:
        report["methodology"]["single_commodity_assessment"] = {
            "data_sources": [
                commodity_analysis.metadata.dataset_id,
                "jrc_gfc2020",
                "hansen_lossyear",
            ],
            "calculation": {
                "method": "deterministic_categorical_raster_intersection",
                "semantic_layers": {
                    "A": "post-2020 Hansen loss inside JRC 2020 forest",
                    "B": "current or observed configured commodity area",
                    "C": "intersection of A and B",
                    "D": "potential forest-to-commodity conversion candidate for human review",
                },
                "area_units": "ha",
                "area_method": "equal_area_projected_pixel_count",
                "target_crs": args.analysis_target_crs,
                "target_resolution_m": args.analysis_target_resolution_m,
                "resampling": "nearest",
            },
            "provider_metadata": commodity_analysis.metadata.to_dict(),
            "is_placeholder": False,
        }
        report["computed"]["commodity_assessment"] = {
            key: value["value"]
            for key, value in commodity_analysis.metrics.to_metric_rows().items()
        }
        report["computed_outputs"]["commodity_assessment"] = commodity_artifact_refs(
            commodity_analysis, bdir
        )
        report["extensions"]["commodity_assessment"] = {
            "status_messages": commodity_analysis.status_messages,
            "evidence_gaps": commodity_analysis.evidence_gaps,
            "provenance": commodity_analysis.provenance,
            "nodata": commodity_analysis.nodata,
            "grid": commodity_analysis.grid,
        }
        commodity_status = (
            "present"
            if commodity_analysis.coverage_status == "full"
            else "partial"
            if commodity_analysis.coverage_status == "partial"
            else "missing"
        )
        report["evidence_registry"]["evidence_classes"].append(
            {
                "class_id": "commodity_evidence",
                "mandatory": False,
                "status": commodity_status,
            }
        )
        commodity_artifacts = [
            path
            for path in [
                commodity_analysis.summary_path,
                commodity_analysis.commodity_mask_path,
                commodity_analysis.overlap_mask_path,
                commodity_analysis.debug_path,
            ]
            if path is not None
        ]
        artifact_paths.extend(commodity_artifacts)
        policy_mapping.append(
            {
                "article_ref": "EUDR Article 9",
                "requirement": "Commodity evidence source is declared and traceable when configured",
                "evidence_fields": [
                    "commodity",
                    "computed.commodity_assessment",
                    "extensions.commodity_assessment.evidence_gaps",
                ],
                "artifact_relpaths": [
                    str(commodity_artifacts[0].relative_to(bdir)).replace("\\", "/")
                    if commodity_artifacts
                    else geo_rel.as_posix()
                ],
                "status": "na",
            }
        )
        report["assumptions"].append(
            {
                "assumption_id": "commodity-layer-evidence-not-causation",
                "description": (
                    "The configured commodity layer is evidence of observed land cover/class, "
                    "not proof of causation, legality, or legal outcome."
                ),
                "testable": True,
                "affects_results": [],
            }
        )

    if satellite_info is not None:
        report["computed_outputs"]["satellite_imagery"] = satellite_info
        artifact_paths.extend([baseline_dst, recent_dst])
        report["assumptions"].append(
            {
                "assumption_id": "satellite-imagery-visual-context-only",
                "description": (
                    "Baseline/recent satellite imagery is supplied as visual review context "
                    "only; it does not itself establish forest loss, conversion, or a "
                    "compliance determination."
                ),
                "testable": False,
                "affects_results": [],
            }
        )

    if hansen_result is not None and hansen_analysis is not None:
        artifact_paths.append(hansen_analysis.summary_path)
        artifact_paths.extend(
            [
                hansen_analysis.loss_mask_path,
                hansen_analysis.current_mask_path,
                hansen_analysis.forest_2000_mask_path,
                hansen_analysis.forest_end_year_mask_path,
                hansen_analysis.tiles_manifest_path,
                hansen_analysis.raw.forest_mask_debug_path,
            ]
        )

        loss_present = hansen_analysis.loss_mask_path.is_file()
        current_present = hansen_analysis.current_mask_path.is_file()
        summary_present = hansen_analysis.summary_path.is_file()
        tiles_manifest_present = hansen_analysis.tiles_manifest_path.is_file()

        report["evidence_registry"]["evidence_classes"].extend(
            [
                {
                    "class_id": "forest_loss_post_2020",
                    "mandatory": True,
                    "status": "present"
                    if loss_present and current_present and summary_present
                    else "missing",
                },
                {
                    "class_id": "hansen_tiles_provenance",
                    "mandatory": True,
                    "status": "present" if tiles_manifest_present else "missing",
                },
            ]
        )

    if geo_kind == "geojson":
        from eudr_dmi_gil.analysis.maaamet_validation import run_maaamet_crosscheck

        maaamet_dir = bdir / "reports" / "aoi_report_v2" / aoi_id / "maaamet"
        computed_current_forest = (
            hansen_result.current_tree_cover_ha if hansen_result is not None else None
        )
        with _timed("maaamet_crosscheck"):
            maaamet_result = run_maaamet_crosscheck(
                aoi_geojson_path=geo_path,
                output_dir=maaamet_dir,
                computed_forest_area_ha=computed_current_forest,
                provider=maaamet_provider,
                parcels_override=maaamet_parcels_override,
                fields_used_override=maaamet_fields_used,
                top10_result=maaamet_top10_result,
            )
        crosscheck_block = {
            "source": "maaamet",
            "fields_used": maaamet_result.fields_used,
            "outcome": maaamet_result.outcome,
            "reference": {
                "source": maaamet_result.reference_source,
                "method": maaamet_result.reference_method,
                "value_ha": maaamet_result.reference_value_ha,
            },
            "computed": {"forest_area_ha": maaamet_result.computed_forest_area_ha},
            "comparison": {
                "tolerance_percent": maaamet_result.tolerance_percent,
                "diff_pct": maaamet_result.diff_pct,
            },
            "csv_ref": {
                "relpath": str(maaamet_result.csv_path.relative_to(bdir)).replace("\\", "/"),
                "sha256": compute_sha256(maaamet_result.csv_path),
                "content_type": "text/csv",
            },
            "summary_ref": {
                "relpath": str(maaamet_result.summary_path.relative_to(bdir)).replace("\\", "/"),
                "sha256": compute_sha256(maaamet_result.summary_path),
                "content_type": "application/json",
            },
        }
        if maaamet_result.reason:
            crosscheck_block["reason"] = maaamet_result.reason
        report["validation"]["forest_area_crosscheck"] = crosscheck_block
        artifact_paths.extend([maaamet_result.csv_path, maaamet_result.summary_path])

    # metrics.csv (portable, deterministic) lives alongside the report outputs.
    metrics_csv_path = bdir / "reports" / "aoi_report_v2" / aoi_id / "metrics.csv"
    with _timed("write_metrics_csv"):
        _write_metrics_csv(metrics_csv_path, metric_rows)
    artifact_paths.append(metrics_csv_path)

    if hansen_result is not None and hansen_external_dependencies is not None:
        if not report.get("external_dependencies"):
            report["external_dependencies"] = hansen_external_dependencies

    # JSON output
    report_json_path = bdir / "reports" / "aoi_report_v2" / f"{aoi_id}.json"
    # HTML output
    report_html_path = bdir / "reports" / "aoi_report_v2" / f"{aoi_id}.html"

    map_config_relpath: str | None = None
    map_config_href: str | None = None
    if geo_kind == "geojson" and hansen_analysis is not None and hansen_result is not None:
        map_dir = bdir / "reports" / "aoi_report_v2" / aoi_id / "map"
        map_config_path = map_dir / "map_config.json"
        aoi_bbox = load_aoi_bbox(geo_path)
        layers = {
            "forest_2000": _rel_href(map_config_path, hansen_analysis.forest_2000_mask_path),
            "forest_end_year": _rel_href(
                map_config_path, hansen_analysis.current_mask_path
            ),
            "forest_loss_post_2020": _rel_href(map_config_path, hansen_analysis.loss_mask_path),
            "aoi_boundary": _rel_href(map_config_path, geo_path),
            "parcels": _rel_href(map_config_path, maaamet_top10_result.geojson_path)
            if maaamet_top10_result is not None
            else None,
        }
        with _timed("write_map_config"):
            _write_map_config(
                path=map_config_path,
                aoi_bbox=aoi_bbox,
                latest_year=hansen_result.forest_metrics.end_year,
                layers=layers,
            )
        map_config_relpath = str(map_config_path.relative_to(bdir)).replace("\\", "/")
        map_config_href = _rel_href(report_html_path, map_config_path)
        report["map_assets"] = {
            "config_relpath": map_config_relpath,
            "latest_year": hansen_result.forest_metrics.end_year,
            "layers": layers,
            "aoi_bbox": {
                "min_lon": aoi_bbox[0],
                "min_lat": aoi_bbox[1],
                "max_lon": aoi_bbox[2],
                "max_lat": aoi_bbox[3],
            },
        }
        artifact_paths.append(map_config_path)

    if args.out_format in ("json", "both"):
        with _timed("write_report_json"):
            report_json_path.parent.mkdir(parents=True, exist_ok=True)
            report_json_bytes = canonical_json_bytes(report) + b"\n"
            report_json_path.write_bytes(report_json_bytes)
            artifact_paths.append(report_json_path)

    # HTML output
    if args.out_format in ("html", "both"):
        with _timed("write_report_html"):
            report_html_path.parent.mkdir(parents=True, exist_ok=True)
            # Link to whatever artifacts are already known; report JSON is included if produced.
            known_artifacts_for_html = list(artifact_paths)
            # Normalize all floats to 6 dp before rendering so that str(float) calls inside
            # _render_html_summary produce platform-stable output regardless of pyproj/GDAL noise.
            html = _render_html_summary(
                _normalise_floats(report, 6),  # type: ignore[arg-type]
                html_path=report_html_path,
                artifact_paths=known_artifacts_for_html,
                map_config_relpath=map_config_href,
                parcel_rows=_normalise_floats(parcel_rows, 6),  # type: ignore[arg-type]
            )
            report_html_path.write_text(html, encoding="utf-8")
            artifact_paths.append(report_html_path)

    def _artifact_role(relpath: str) -> str | None:
        if relpath == geo_rel.as_posix():
            return "aoi_geometry"
        if relpath.endswith(f"/{aoi_id}.json"):
            return "report_json"
        if relpath.endswith(f"/{aoi_id}.html"):
            return "report_html"
        if relpath.endswith("/metrics.csv"):
            return "metrics_csv"
        if relpath.endswith("forest_loss_post_2020_mask.geojson"):
            return "forest_loss_mask"
        if relpath.endswith("forest_current_tree_cover_mask.geojson"):
            return "forest_current_mask"
        if relpath.endswith("forest_2000_tree_cover_mask.geojson"):
            return "forest_2000_mask"
        if relpath.endswith("forest_end_year_tree_cover_mask.geojson"):
            return "forest_end_year_mask"
        if relpath.endswith("forest_loss_post_2020_tiles.json"):
            return "hansen_tiles_manifest"
        if relpath.endswith("forest_loss_post_2020_summary.json"):
            return "forest_loss_summary"
        if relpath.endswith("forest_mask_debug.json"):
            return "forest_mask_debug"
        if relpath.endswith("map/map_config.json"):
            return "report_map_config"
        if relpath.endswith("jrc_forest_2020_mask.geojson"):
            return "jrc_forest_2020_mask"
        if "jrc_gfc2020/" in relpath and relpath.endswith("_on_jrc_forest_2020_mask.geojson"):
            return "post_2020_loss_on_2020_forest_mask"
        if "jrc_gfc2020/" in relpath and relpath.endswith("_summary.json"):
            return "post_2020_loss_on_2020_forest_summary"
        if relpath.endswith("jrc_post2020_loss_debug.json"):
            return "post_2020_loss_on_2020_forest_debug"
        if "/commodity/" in relpath and relpath.endswith("_commodity_summary.json"):
            return "commodity_summary"
        if "/commodity/" in relpath and relpath.endswith("_commodity_mask.geojson"):
            return "commodity_mask"
        if "/commodity/" in relpath and relpath.endswith("_post2020_loss_overlap_mask.geojson"):
            return "commodity_post2020_loss_overlap_mask"
        if "/commodity/" in relpath and relpath.endswith("_commodity_debug.json"):
            return "commodity_debug"
        if relpath.endswith("maaamet_forest_area_crosscheck.csv"):
            return "maaamet_crosscheck_csv"
        if relpath.endswith("maaamet_forest_area_crosscheck_summary.json"):
            return "maaamet_crosscheck_summary"
        if relpath.endswith("maaamet_top10_parcels.geojson"):
            return "maaamet_top10_geojson"
        if relpath.endswith("maaamet_top10_parcels.csv"):
            return "maaamet_top10_csv"
        if relpath.endswith("maaamet_fields_inventory.json"):
            return "maaamet_fields_inventory"
        if relpath.endswith("maaamet_parcels_metadata.json"):
            return "maaamet_parcels_metadata"
        return None

    # Populate evidence_artifacts in report JSON (exclude manifest to avoid circularity).
    report["evidence_artifacts"] = []
    for p in sorted(set(artifact_paths), key=lambda p: p.as_posix()):
        relpath = str(p.relative_to(bdir)).replace("\\\\", "/")
        entry = {
            "relpath": relpath,
            "sha256": compute_sha256(p),
            "size_bytes": p.stat().st_size,
        }
        content_type = _content_type_for_path(p)
        if content_type:
            entry["content_type"] = content_type
        role = _artifact_role(relpath)
        if role:
            entry["meta"] = {"role": role}
        report["evidence_artifacts"].append(entry)

    # If we wrote report JSON, rewrite it now that evidence_artifacts is populated.
    if args.out_format in ("json", "both"):
        with _timed("rewrite_report_json"):
            report_json_path.write_bytes(canonical_json_bytes(report) + b"\n")

    # Validate contract.
    from .validate import validate_aoi_report

    with _timed("validate_report"):
        validate_aoi_report(report)

    canonical_report_root = bdir / "reports" / "aoi_report_v2" / aoi_id
    canonical_report_json_path = canonical_report_root / "report.json"
    canonical_report_html_path = canonical_report_root / "report.html"
    canonical_report_pdf_path = canonical_report_root / "report.pdf"
    canonical_metrics_csv_path = canonical_report_root / "metrics.csv"
    canonical_manifest_path = canonical_report_root / "manifest.sha256"

    with _timed("write_canonical_evidence_bundle"):
        generated_artifacts = materialize_evidence_pngs(
            report,
            bundle_root=bdir,
            report_root=canonical_report_root,
        )
        canonical_report = canonical_report_from_aoi_report_v2(
            report,
            bundle_root=bdir,
            report_root=bdir / "reports" / "aoi_report_v2",
            generated_artifacts=generated_artifacts,
        )
        write_canonical_metrics_csv(canonical_metrics_csv_path, canonical_report.metrics)
        write_canonical_report_json(canonical_report_json_path, canonical_report)
        render_canonical_html(canonical_report, canonical_report_html_path)
        render_canonical_pdf(
            canonical_report,
            canonical_report_pdf_path,
            report_root=canonical_report_root,
        )

        canonical_relpaths = [
            str(path.relative_to(bdir)).replace("\\", "/")
            for path in [
                canonical_report_json_path,
                canonical_report_html_path,
                canonical_report_pdf_path,
                canonical_metrics_csv_path,
            ]
        ]
        for artifact in generated_artifacts.values():
            if artifact.path:
                canonical_relpaths.append(
                    str((canonical_report_root / artifact.path).relative_to(bdir)).replace(
                        "\\", "/"
                    )
                )

        canonical_report = update_canonical_artifact_hashes(
            canonical_report,
            report_root=canonical_report_root,
            bundle_root=bdir,
            artifact_relpaths=canonical_relpaths,
        )
        write_canonical_report_json(canonical_report_json_path, canonical_report)
        validate_aoi_report(canonical_report.to_dict())
        write_sha256_manifest(bdir, canonical_manifest_path, canonical_relpaths)

        artifact_paths.extend([bdir / relpath for relpath in canonical_relpaths])
        artifact_paths.append(canonical_manifest_path)

    # Manifest written by bundle writer.
    # Exclude manifest itself from artifacts passed to the writer.
    with _timed("write_manifest"):
        write_manifest(bdir, sorted(set(artifact_paths), key=lambda p: p.as_posix()))

    print(str(bdir))
    return 0


@dataclass(frozen=True)
class MetricRow:
    variable: str
    value: int | float | None
    unit: str
    source: str
    notes: str


def _parse_metric_rows(raw_metrics: list[str], *, fallback_dummy: str | None) -> list[MetricRow]:
    rows: list[MetricRow] = []

    if raw_metrics:
        for raw in raw_metrics:
            rows.append(_parse_metric_row(raw))
    elif fallback_dummy is not None:
        # Backwards-compat scaffolding.
        name, value, unit = _parse_dummy_metric(fallback_dummy)
        rows.append(MetricRow(variable=name, value=value, unit=unit, source="", notes=""))

    # Stable ordering.
    return sorted(rows, key=lambda r: r.variable)


def _parse_metric_row(raw: str) -> MetricRow:
    # variable=value:unit[:source[:notes]]
    if "=" not in raw or ":" not in raw:
        raise ValueError("--metric must be variable=value:unit[:source[:notes]]")

    variable, rest = raw.split("=", 1)
    parts = rest.split(":")
    if len(parts) < 2:
        raise ValueError("--metric must be variable=value:unit[:source[:notes]]")

    value_str = parts[0]
    unit = parts[1]
    source = parts[2] if len(parts) >= 3 else ""
    notes = ":".join(parts[3:]) if len(parts) >= 4 else ""

    variable = variable.strip()
    unit = unit.strip()
    source = source.strip()
    notes = notes.strip()

    if not variable or not unit:
        raise ValueError("--metric must be variable=value:unit[:source[:notes]]")

    value: int | float
    try:
        if "." in value_str:
            value = float(value_str)
        else:
            value = int(value_str)
    except ValueError:
        raise ValueError("--metric value must be int or float") from None

    return MetricRow(variable=variable, value=value, unit=unit, source=source, notes=notes)


def _parse_dummy_metric(raw: str) -> tuple[str, int | float, str]:
    # name=value:unit
    if "=" not in raw or ":" not in raw:
        raise ValueError("--dummy-metric must be name=value:unit")

    name, rest = raw.split("=", 1)
    value_str, unit = rest.split(":", 1)
    name = name.strip()
    unit = unit.strip()

    if not name or not unit:
        raise ValueError("--dummy-metric must be name=value:unit")

    value: int | float
    try:
        if "." in value_str:
            value = float(value_str)
        else:
            value = int(value_str)
    except ValueError:
        raise ValueError("--dummy-metric value must be int or float") from None

    return name, value, unit


def _metrics_from_rows(rows: list[MetricRow]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        entry: dict[str, Any] = {"value": r.value, "unit": r.unit}
        if r.notes:
            entry["notes"] = r.notes
        out[r.variable] = entry
    return out


def _write_metrics_csv(path: Path, rows: list[MetricRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure stable row ordering.
    ordered = sorted(rows, key=lambda r: r.variable)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["variable", "value", "unit", "source", "notes"])
        for r in ordered:
            writer.writerow([r.variable, _stable_value_str(r.value), r.unit, r.source, r.notes])


def _stable_value_str(value: int | float | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    # Round to 6 dp to suppress platform-specific pyproj/GDAL floating-point
    # noise in the last 1-3 significant digits (last 10+ chars of repr).
    # 6 dp = ~0.01 mm² precision for area-in-ha values, matching pixel_* fields.
    return f"{value:.6f}"


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
