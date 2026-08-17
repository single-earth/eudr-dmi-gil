from __future__ import annotations

import csv
import html
import io
import json
import os
import struct
import zlib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping


from .bundle import compute_sha256
from .determinism import canonical_json_bytes


SCHEMA_VERSION = "eudr_evidence_report_v3"

# The Forest Baseline/Loss evidence maps (pages 5/6 of render_canonical_pdf) are drawn into a
# fixed A4 box (content width x 520pt - see the `new_page(5, ...)`/`new_page(6, ...)` blocks
# below). Evidence PNGs for those two pages are rendered at this same aspect ratio so a plain
# contain-fit exactly fills that box: no gray letterbox, and no cover-crop that could cut into
# the AOI/mask geometry `_reproject_raster_to_grid` already buffers into the frame.
_EVIDENCE_MAP_BOX_ASPECT = (595.2755905511812 - 64.0) / 520.0  # A4 width - 2*32pt margin : 520pt
EVIDENCE_MAP_PIXEL_WIDTH = 640
EVIDENCE_MAP_PIXEL_HEIGHT = round(EVIDENCE_MAP_PIXEL_WIDTH / _EVIDENCE_MAP_BOX_ASPECT)

# The cover (`cover()` in render_canonical_pdf) draws a full-bleed hero image behind the title
# text with a cover-crop (`draw_image_fit(..., fill=True)`), which scales the source image up
# until it fills the whole portrait A4 page and crops whatever is left over on one axis. The
# shared `aoi_satellite` context PNG below is generated at a landscape aspect ratio for an
# unrelated landscape use (the HTML layer switcher); cover-cropping that landscape image to a
# portrait full page crops away most of its width, which was cutting the dashed AOI outline off
# at the page edges instead of keeping it inside the visible cover. This dedicated hero PNG is
# rendered at the cover page's own portrait aspect ratio instead, so the cover-crop has nothing
# left to crop and the whole padded AOI frame - polygon included - stays inside the page.
COVER_HERO_PIXEL_WIDTH = 640
COVER_HERO_PIXEL_HEIGHT = round(COVER_HERO_PIXEL_WIDTH * 841.8897637795277 / 595.2755905511812)

# Cover (page 1) is composited from the Esri World Imagery export service. Page 4 uses a caller
# supplied regional raster when present, then falls back to the locally pinned recent Sentinel-2
# raster before attempting Esri. This keeps recurring page-4 gaps out of offline/derived evidence
# packages whose ordinary AOI satellite imagery is already available.
ESRI_WORLDIMAGERY_EXPORT_URL = (
    "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export"
)
ESRI_WORLDIMAGERY_TILE_URL_TEMPLATE = (
    "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
)
ESRI_WORLDIMAGERY_DATASET_TITLE = "Esri World Imagery"
ESRI_WORLDIMAGERY_DATASET_VERSION = "world_imagery_current"
ESRI_WORLDIMAGERY_SOURCE_URL = (
    "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer"
)
ESRI_WORLDIMAGERY_ATTRIBUTION = "Esri, Maxar, Earthstar Geographics, and the GIS User Community"
ESRI_WORLDIMAGERY_LICENSE = "Esri master license agreement (basemap use permitted for web/print display with attribution)"

# Page 7 (Satellite Evidence, see `new_page(7, ...)` below) draws the before/after comparison
# into an A4 box capped at (content width x BEFORE_AFTER_BOX_HEIGHT pt) - the same technique as
# `_EVIDENCE_MAP_BOX_ASPECT` above but sized for this page's own available space (a swatch
# legend and a dates/providers row block sit below the image here, unlike pages 5/6), and treated
# as a ceiling rather than a fixed size: each before/after panel is *requested* at the pixel size
# that would make the combined two-panel image (panel + divider + panel) match this box's aspect
# ratio, but `_reproject_pair_to_shared_grid` only ever widens a raster's real, undistorted
# coverage - for an AOI whose own bounding box is already close to square, both source rasters'
# real coverage clamps the result back toward that square-ish shape well before it reaches this
# ceiling (see the `new_page(7, ...)` comment on the resulting box sizing below). The ceiling
# still replaces the old fixed 420pt box, which left ~200pt of page height unused even for AOI
# shapes that could have filled it.
BEFORE_AFTER_BOX_HEIGHT = 600.0
_BEFORE_AFTER_DIVIDER_PX = 6
_BEFORE_AFTER_BOX_ASPECT = (595.2755905511812 - 64.0) / BEFORE_AFTER_BOX_HEIGHT
BEFORE_AFTER_PANEL_PIXEL_HEIGHT = 720
BEFORE_AFTER_PANEL_PIXEL_WIDTH = round(
    (_BEFORE_AFTER_BOX_ASPECT * BEFORE_AFTER_PANEL_PIXEL_HEIGHT - _BEFORE_AFTER_DIVIDER_PX) / 2
)

# Round 18: when a commodity (e.g. coffee) layer is configured and available for an AOI, the
# baseline/loss evidence maps (pages 5/6) layer the commodity evidence directly onto the same map
# image instead of leaving it as a separate artifact no page ever draws. The baseline page (5) gets
# a translucent commodity-mask overlay showing where the configured commodity sits relative to the
# JRC 2020 forest baseline; the loss page (6) gets a stronger-alpha overlay of the post-2020
# loss-and-commodity intersection so loss detected inside the commodity layer visually stands out
# from loss elsewhere in the AOI. Both colors match the standalone `commodity_layer`/`intersection`
# evidence artifacts already produced by `materialize_evidence_pngs`, so every view of this data
# agrees.
#
# Round 26 (elevated from a Brazil/coffee task-bundle finding): the original commodity color,
# (139, 90, 43) - a "natural-looking" brown chosen to suggest bare/cropland soil - measured almost
# indistinguishable from the real basemap color under real coffee-plantation pixels for a live-GEE
# AOI (~RGB(140, 95, 65) basemap vs (139, 90, 43) overlay), so blending at any reasonable alpha
# shifted the composited pixel by only a few RGB units: a genuinely invisible overlay, not a
# rendering failure (the mask rasterized and blended correctly; see
# `coffee_brazil_minas_gerais_eudr_compliant` bundle round 4). Real-world commodity/cropland colors
# cluster in the same brown/tan/green hue family as most satellite basemap imagery, so any
# "natural" color risks the same failure on some AOI's basemap. The overlay color is now a
# saturated blue with no natural-terrain analogue, chosen to sit far in hue from forest-green,
# loss-red, intersection-purple, and the AOI-boundary yellow already in use - and the overlay alpha
# is raised so the blend is assertive rather than barely-there.
_COMMODITY_OVERLAY_COLOR = (30, 136, 229, 230)
_COMMODITY_OVERLAY_ALPHA = 0.6
_COMMODITY_LOSS_OVERLAY_COLOR = (102, 45, 145, 230)
_COMMODITY_LOSS_OVERLAY_ALPHA = 0.85


@dataclass(frozen=True)
class ArtifactRef:
    path: str | None
    available: bool
    availability_status: str
    checksum_sha256: str | None = None
    content_type: str | None = None


@dataclass(frozen=True)
class LayerEntry:
    id: str
    title: str
    path: str | None
    dataset: str
    dataset_version: str
    purpose: str
    date: str | None
    available: bool
    availability_status: str
    checksum_sha256: str | None = None


@dataclass(frozen=True)
class CanonicalReport:
    schema_version: str
    report_id: str
    run_id: str
    generated_utc: str
    assessment: dict[str, Any]
    aoi: dict[str, Any]
    commodity: dict[str, Any]
    temporal_scope: dict[str, Any]
    metrics: dict[str, Any]
    datasets: list[dict[str, Any]]
    methods: list[dict[str, Any]]
    evidence_gaps: list[dict[str, Any]]
    layers: dict[str, Any]
    artifacts: dict[str, Any]
    audit: dict[str, Any]
    references: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def canonical_report_from_aoi_report_v2(
    report: Mapping[str, Any],
    *,
    bundle_root: Path,
    report_root: Path,
    generated_artifacts: Mapping[str, ArtifactRef],
) -> CanonicalReport:
    aoi_id = str(report.get("aoi_id", "unknown"))
    run_id = str(report.get("bundle_id", "unknown"))
    generated_utc = str(report.get("generated_at_utc", ""))
    metrics = _canonical_metrics(report.get("metrics", {}))
    metrics.update(_interactive_overlay_availability_metrics(generated_artifacts))
    temporal_scope = _temporal_scope(report)
    evidence_gaps = _collect_evidence_gaps(report) + _artifact_evidence_gaps(generated_artifacts)
    layers = _layers(report, generated_artifacts, temporal_scope)
    commodity = _commodity(report, metrics)
    assessment = _assessment(metrics=metrics, evidence_gaps=evidence_gaps)

    return CanonicalReport(
        schema_version=SCHEMA_VERSION,
        report_id=f"{run_id}::{aoi_id}",
        run_id=run_id,
        generated_utc=generated_utc,
        assessment=assessment,
        aoi=_aoi(report, metrics, bundle_root=bundle_root),
        commodity=commodity,
        temporal_scope=temporal_scope,
        metrics=metrics,
        datasets=list(report.get("datasets") or []),
        methods=_methods(report),
        evidence_gaps=evidence_gaps,
        layers={key: asdict(value) for key, value in layers.items()},
        artifacts=_artifacts(generated_artifacts),
        audit={
            "source_report_version": report.get("report_version"),
            "source_report_path": _relpath(report_root / f"{aoi_id}.json", bundle_root),
            "pdf_generator": _pdf_generator_metadata(),
            "compatibility": {
                "renamed_fields": {
                    "report_version": "schema_version",
                    "generated_at_utc": "generated_utc",
                    "aoi_id": "aoi.name",
                    "results_summary.aoi_area.area_ha": "metrics.aoi_area_ha",
                    "forest_metrics.rfm_area_ha": (
                        "legacy audit only; not reinterpreted as "
                        "metrics.forest_baseline_2020_ha"
                    ),
                },
                "old_report_adapter": "canonical_report_from_aoi_report_v2",
            },
            "source_report": report,
        },
        references=_references(layers),
    )


def _references(layers: Mapping[str, "LayerEntry"]) -> list[dict[str, Any]]:
    """Real, generic dataset/methodology citations for the Appendix page.

    Deliberately generic (not commodity- or country-specific claims, which would need per-AOI
    verification this codebase does not perform): each entry cites the actual dataset or
    regulation this report's pipeline depends on, gated on the corresponding evidence layer
    actually being present/available in this run.
    """
    refs: list[dict[str, Any]] = [
        {
            "id": "eudr_regulation",
            "citation": "Regulation (EU) 2023/1115 of the European Parliament and of the Council on deforestation-free products.",
            "url": "https://eur-lex.europa.eu/eli/reg/2023/1115/oj",
        },
        {
            "id": "jrc_gfc2020",
            "citation": (
                "Vancutsem, C. et al. (2021). Long-term (1990-2019) monitoring of forest cover "
                "changes in the humid tropics. Science Advances 7(10). Dataset: JRC Global "
                "Forest Cover 2020 (GFC2020), European Commission Joint Research Centre."
            ),
            "url": "https://forobs.jrc.ec.europa.eu/GFC",
        },
    ]
    forest_loss = layers.get("forest_loss")
    if getattr(forest_loss, "available", False):
        refs.append(
            {
                "id": "hansen_gfc",
                "citation": (
                    "Hansen, M.C. et al. (2013). High-Resolution Global Maps of 21st-Century "
                    "Forest Cover Change. Science 342(6160). Dataset: UMD/Google/USGS/NASA "
                    "Global Forest Change."
                ),
                "url": "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2023-v1.11/download.html",
            }
        )
    commodity_layer = layers.get("commodity")
    commodity_dataset = (getattr(commodity_layer, "dataset", "") or "").lower()
    if getattr(commodity_layer, "available", False) and "mapbiomas" in commodity_dataset:
        refs.append(
            {
                "id": "mapbiomas",
                "citation": "MapBiomas Brazil Project - Collection of Brazilian land cover and land use maps.",
                "url": "https://brasil.mapbiomas.org/",
            }
        )
    if getattr(commodity_layer, "available", False) and (
        "forest data partnership" in commodity_dataset or "fdp" in commodity_dataset
    ):
        refs.append(
            {
                "id": "forest_data_partnership",
                "citation": (
                    "Forest Data Partnership - commodity probability model (see the "
                    "report's commodity block for the exact asset id and dataset version)."
                ),
                "url": "https://github.com/google/forest-data-partnership",
            }
        )
    satellite_layer = layers.get("satellite")
    if getattr(satellite_layer, "available", False):
        refs.append(
            {
                "id": "sentinel2",
                "citation": "Contains modified Copernicus Sentinel data, accessed via the AWS Earth Search STAC API.",
                "url": "https://earth-search.aws.element84.com/v1",
            }
        )
    tmf_deforestation_layer = layers.get("tmf_deforestation")
    tmf_degradation_layer = layers.get("tmf_degradation")
    if getattr(tmf_deforestation_layer, "available", False) or getattr(tmf_degradation_layer, "available", False):
        refs.append(
            {
                "id": "jrc_tmf",
                "citation": (
                    "European Commission Joint Research Centre Tropical Moist Forest product "
                    "family, including DeforestationYear and DegradationYear evidence layers."
                ),
                "url": "https://forobs.jrc.ec.europa.eu/TMF",
            }
        )
    if getattr(layers.get("radd_alerts"), "available", False) or getattr(layers.get("radd_confirmed"), "available", False):
        refs.append(
            {
                "id": "radd_alerts",
                "citation": (
                    "RADD forest disturbance alerts from Wageningen University and Research, "
                    "using Sentinel-1 SAR alert confidence classes."
                ),
                "url": "https://wur-radd.users.earthengine.app/view/raddalert",
            }
        )
    cover_hero_layer = layers.get("cover_hero")
    regional_overview_layer = layers.get("regional_overview")
    if getattr(cover_hero_layer, "available", False) or getattr(regional_overview_layer, "available", False):
        refs.append(
            {
                "id": "esri_world_imagery",
                "citation": (
                    f"Basemap imagery: {ESRI_WORLDIMAGERY_ATTRIBUTION}, via the Esri World "
                    "Imagery export service (cover page and regional-overview page only; every "
                    "other satellite-context image in this report uses Sentinel-2, see the "
                    "sentinel2 reference above)."
                ),
                "url": ESRI_WORLDIMAGERY_SOURCE_URL,
            }
        )
    return refs


def materialize_evidence_pngs(
    report: Mapping[str, Any],
    *,
    bundle_root: Path,
    report_root: Path,
) -> dict[str, ArtifactRef]:
    evidence_dir = report_root / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, ArtifactRef] = {}

    effective_end_year = _temporal_scope(report)["effective_end_year"]
    computed_outputs = report.get("computed_outputs", {})
    jrc_outputs = (
        computed_outputs.get("post_2020_loss_on_2020_forest", {})
        if isinstance(computed_outputs, Mapping)
        else {}
    )
    tmf_outputs = (
        computed_outputs.get("jrc_tmf_change", {})
        if isinstance(computed_outputs, Mapping)
        else {}
    )
    radd_outputs = (
        computed_outputs.get("radd_alerts", {})
        if isinstance(computed_outputs, Mapping)
        else {}
    )
    commodity_outputs = (
        computed_outputs.get("commodity_assessment", {})
        if isinstance(computed_outputs, Mapping)
        else {}
    )
    satellite_outputs = (
        computed_outputs.get("satellite_imagery", {})
        if isinstance(computed_outputs, Mapping)
        else {}
    )
    aoi_geom_wgs84 = _load_report_aoi_geometry(report, bundle_root)

    satellite_recent_ref = _ref_relpath(satellite_outputs, "recent_raster_ref")
    satellite_recent_path = (
        bundle_root / satellite_recent_ref if satellite_recent_ref and (bundle_root / satellite_recent_ref).is_file() else None
    )
    satellite_baseline_ref = _ref_relpath(satellite_outputs, "baseline_raster_ref")
    satellite_baseline_path = (
        bundle_root / satellite_baseline_ref
        if satellite_baseline_ref and (bundle_root / satellite_baseline_ref).is_file()
        else None
    )
    artifacts["aoi_satellite"] = _write_satellite_context_png(
        bundle_root=bundle_root,
        raster_relpath=satellite_recent_ref,
        aoi_geom_wgs84=aoi_geom_wgs84,
        output_path=evidence_dir / "01_aoi_satellite.png",
    )

    # The cover hero basemap is fetched live from Esri World Imagery instead of the local
    # Sentinel-2 "recent" raster every other satellite-context image on this AOI still uses (see
    # the ESRI_WORLDIMAGERY_* constants above for why: a deliberate, per-image basemap-provider
    # substitution, not a pipeline change).
    artifacts["cover_hero"] = _write_esri_satellite_context_png(
        aoi_geom_wgs84=aoi_geom_wgs84,
        output_path=evidence_dir / "08_cover_hero.png",
        width=COVER_HERO_PIXEL_WIDTH,
        height=COVER_HERO_PIXEL_HEIGHT,
    )

    # Round 25: a plain satellite basemap sized to the same EVIDENCE_MAP_PIXEL_WIDTH/HEIGHT box
    # as jrc_forest_2020/forest_loss/commodity_layer/intersection below, so pages 5/6 can fall
    # back to it edge-to-edge (no letterboxing) whenever there is no mask to overlay - unlike
    # `aoi_satellite` above, which is fixed at 640x420 for the page-1/cover box's own aspect and
    # would only partially fill the evidence-map box's different aspect ratio.
    artifacts["satellite_evidence_map"] = _write_satellite_context_png(
        bundle_root=bundle_root,
        raster_relpath=satellite_recent_ref,
        aoi_geom_wgs84=aoi_geom_wgs84,
        output_path=evidence_dir / "01b_aoi_satellite_evidence_map.png",
        width=EVIDENCE_MAP_PIXEL_WIDTH,
        height=EVIDENCE_MAP_PIXEL_HEIGHT,
    )

    # Resolved ahead of the baseline/loss evidence maps below (round 18) so pages 5/6 can layer the
    # commodity/coffee-plantation evidence directly onto those same map images whenever a
    # commodity layer is actually configured and available for this AOI, instead of leaving it as
    # a separate artifact no report page ever draws.
    commodity_mask_ref = _ref_relpath(commodity_outputs, "commodity_mask_ref")
    commodity_mask_path = (
        bundle_root / commodity_mask_ref
        if commodity_mask_ref and (bundle_root / commodity_mask_ref).is_file()
        else None
    )
    baseline_commodity_mask_ref = _ref_relpath(commodity_outputs, "baseline_commodity_mask_ref")
    baseline_commodity_mask_path = (
        bundle_root / baseline_commodity_mask_ref
        if baseline_commodity_mask_ref and (bundle_root / baseline_commodity_mask_ref).is_file()
        else None
    )
    commodity_loss_overlap_ref = _ref_relpath(commodity_outputs, "post_2020_loss_overlap_mask_ref")
    commodity_loss_overlap_path = (
        bundle_root / commodity_loss_overlap_ref
        if commodity_loss_overlap_ref and (bundle_root / commodity_loss_overlap_ref).is_file()
        else None
    )
    new_fdp_ref = _ref_relpath(commodity_outputs, "fdp_new_commodity_since_baseline_mask_ref")
    new_fdp_path = (
        bundle_root / new_fdp_ref if new_fdp_ref and (bundle_root / new_fdp_ref).is_file() else None
    )
    new_mapbiomas_ref = _ref_relpath(
        commodity_outputs, "mapbiomas_new_commodity_since_baseline_mask_ref"
    )
    new_mapbiomas_path = (
        bundle_root / new_mapbiomas_ref
        if new_mapbiomas_ref and (bundle_root / new_mapbiomas_ref).is_file()
        else None
    )
    source_conversion_ref = _ref_relpath(
        commodity_outputs, "post_2020_loss_and_source_specific_new_commodity_mask_ref"
    )
    source_conversion_path = (
        bundle_root / source_conversion_ref
        if source_conversion_ref and (bundle_root / source_conversion_ref).is_file()
        else None
    )
    agreement_conversion_ref = _ref_relpath(
        commodity_outputs, "post_2020_loss_and_both_source_agreement_new_commodity_mask_ref"
    )
    agreement_conversion_path = (
        bundle_root / agreement_conversion_ref
        if agreement_conversion_ref and (bundle_root / agreement_conversion_ref).is_file()
        else None
    )

    baseline_ref = _ref_relpath(jrc_outputs, "baseline_mask_ref")
    baseline_mask_path = (
        bundle_root / baseline_ref if baseline_ref and (bundle_root / baseline_ref).is_file() else None
    )
    loss_ref = _ref_relpath(jrc_outputs, "loss_mask_ref")
    loss_mask_path = bundle_root / loss_ref if loss_ref and (bundle_root / loss_ref).is_file() else None
    tmf_deforestation_ref = _ref_relpath(tmf_outputs, "deforestation_mask_ref")
    tmf_deforestation_path = (
        bundle_root / tmf_deforestation_ref
        if tmf_deforestation_ref and (bundle_root / tmf_deforestation_ref).is_file()
        else None
    )
    tmf_degradation_ref = _ref_relpath(tmf_outputs, "degradation_mask_ref")
    tmf_degradation_path = (
        bundle_root / tmf_degradation_ref
        if tmf_degradation_ref and (bundle_root / tmf_degradation_ref).is_file()
        else None
    )
    radd_confirmed_ref = _ref_relpath(radd_outputs, "confirmed_mask_ref")
    radd_confirmed_path = (
        bundle_root / radd_confirmed_ref
        if radd_confirmed_ref and (bundle_root / radd_confirmed_ref).is_file()
        else None
    )
    radd_low_confidence_ref = _ref_relpath(radd_outputs, "low_confidence_mask_ref")
    radd_low_confidence_path = (
        bundle_root / radd_low_confidence_ref
        if radd_low_confidence_ref and (bundle_root / radd_low_confidence_ref).is_file()
        else None
    )

    # Round 26: this composite ("Forest Baseline 2020", page 5) is now drawn over the actual 2020
    # satellite raster instead of the 2025 "recent" one every mask-over-basemap composite
    # previously used regardless of the page's own subject year - every other evidence-map page's
    # basemap year now matches its own subject year (page 6/current-state below is 2025-over-2025;
    # this page is 2020-over-2020). Falls back to the recent raster only if no 2020 raster was
    # actually fetched for this AOI (never fabricates a 2020-labeled image from later imagery when
    # a real 2020 one is simply absent - it draws nothing rather than a mislabeled substitute).
    artifacts["jrc_forest_2020"] = _layered_png_from_geojson_refs(
        layers=[
            (baseline_mask_path, (33, 122, 72), 0.62),
            (
                baseline_commodity_mask_path,
                _COMMODITY_OVERLAY_COLOR[:3],
                _COMMODITY_OVERLAY_ALPHA,
            ),
        ],
        output_path=evidence_dir / "02_jrc_forest_2020.png",
        background_raster_path=satellite_baseline_path or satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
        unavailable_reason="jrc_forest_2020_mask_not_available",
    )

    # Round 26: this composite ("Forest Loss After 2020", page 6) used to draw only the raw
    # loss-mask polygon (plus, when present, the loss-and-commodity intersection); its legend
    # already carried a "Forest (JRC 2020 baseline)" row next to that, but no forest-green pixel
    # was ever actually rasterized onto this image - only onto the separate page-5 composite. That
    # is the same class of defect as the commodity-overlay-color problem above (a legend row
    # advertising a layer the image doesn't actually draw), just never previously noticed because
    # it required checking the rendered pixels against the legend rather than a color swatch
    # against a background color. It also meant any AOI with zero measured loss fell back to a
    # plain, no-overlay satellite basemap, because the sole mask it ever tried to rasterize (the
    # loss mask) is empty by definition whenever loss is zero.
    #
    # Both are fixed together by making this composite's primary layer "current forest" - the JRC
    # 2020 baseline forest polygon minus whatever the Hansen loss mask actually removed from it
    # (identical to the baseline polygon when loss is zero, a proper geometric subset of it
    # otherwise) - stacked with the same commodity overlay page 5 uses, then the raw loss mask on
    # top (present only when loss is nonzero), then the loss-and-commodity intersection on top of
    # that (present only where loss actually fell inside the commodity layer). Every layer in the
    # stack is real, rasterized geometry, never a placeholder: `render_canonical_pdf` gates each
    # legend row on whether its corresponding layer here actually contributed nonzero area (see the
    # `new_page(6, ...)` block). A zero-loss AOI now renders forest + commodity context on page 6
    # instead of a bare basemap; a nonzero-loss AOI now actually shows the surviving-forest pixels
    # its legend already claimed.
    current_forest_geom = _load_layer_geometry(baseline_mask_path)
    loss_geom_for_diff = _load_layer_geometry(loss_mask_path)
    if current_forest_geom is not None and loss_geom_for_diff is not None:
        try:
            current_forest_geom = current_forest_geom.difference(loss_geom_for_diff)
        except Exception:
            pass  # keep the undifferenced baseline geometry rather than fail the whole composite

    artifacts["forest_loss"] = _layered_png_from_geojson_refs(
        layers=[
            (current_forest_geom, (33, 122, 72), 0.62),
            (commodity_mask_path, _COMMODITY_OVERLAY_COLOR[:3], _COMMODITY_OVERLAY_ALPHA),
            (loss_mask_path, (198, 40, 40), 0.75),
            (commodity_loss_overlap_path, _COMMODITY_LOSS_OVERLAY_COLOR[:3], _COMMODITY_LOSS_OVERLAY_ALPHA),
        ],
        output_path=evidence_dir / f"03_forest_loss_2021_{effective_end_year}.png",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
        unavailable_reason="post_2020_loss_mask_not_available",
    )

    artifacts["tmf_deforestation"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=tmf_deforestation_ref,
        output_path=evidence_dir / f"13_tmf_deforestation_2021_{effective_end_year}.png",
        color=(211, 47, 47, 220),
        unavailable_reason="tmf_deforestation_mask_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )
    artifacts["tmf_degradation"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=tmf_degradation_ref,
        output_path=evidence_dir / f"14_tmf_degradation_2021_{effective_end_year}.png",
        color=(245, 124, 0, 220),
        unavailable_reason="tmf_degradation_mask_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )
    artifacts["radd_confirmed"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=radd_confirmed_ref,
        output_path=evidence_dir / "15_radd_confirmed_alerts.png",
        color=(142, 36, 170, 225),
        unavailable_reason="radd_confirmed_alert_mask_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )
    artifacts["radd_low_confidence"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=radd_low_confidence_ref,
        output_path=evidence_dir / "16_radd_low_confidence_alerts.png",
        color=(251, 192, 45, 225),
        unavailable_reason="radd_low_confidence_alert_mask_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )
    artifacts["radd_alerts"] = _layered_png_from_geojson_refs(
        layers=[
            (radd_low_confidence_path, (251, 192, 45), 0.72),
            (radd_confirmed_path, (142, 36, 170), 0.78),
        ],
        output_path=evidence_dir / "17_radd_alerts_confirmed_low_confidence.png",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
        unavailable_reason="radd_alert_masks_not_available",
    )

    artifacts["commodity_layer"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=commodity_mask_ref,
        output_path=evidence_dir / "04_commodity_layer.png",
        color=_COMMODITY_OVERLAY_COLOR,
        unavailable_reason="usable_commodity_layer_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )
    artifacts["baseline_commodity_layer"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=baseline_commodity_mask_ref,
        output_path=evidence_dir / "04b_baseline_commodity_layer.png",
        color=_COMMODITY_OVERLAY_COLOR,
        unavailable_reason="usable_baseline_commodity_layer_not_available",
        background_raster_path=satellite_baseline_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )

    overlap_ref = commodity_loss_overlap_ref or loss_ref
    artifacts["intersection"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=overlap_ref,
        output_path=evidence_dir / "05_intersection.png",
        color=(102, 45, 145, 230),
        unavailable_reason="intersection_mask_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )

    artifacts["fdp_new_commodity"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=new_fdp_ref,
        output_path=evidence_dir / "09_fdp_new_commodity_since_baseline.png",
        color=(0, 132, 124, 220),
        unavailable_reason="fdp_new_commodity_layer_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )
    artifacts["mapbiomas_new_commodity"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=new_mapbiomas_ref,
        output_path=evidence_dir / "10_mapbiomas_new_commodity_since_baseline.png",
        color=(30, 136, 229, 220),
        unavailable_reason="mapbiomas_new_commodity_layer_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )
    artifacts["source_specific_conversion"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=source_conversion_ref,
        output_path=evidence_dir / "11_source_specific_conversion.png",
        color=(245, 124, 0, 230),
        unavailable_reason="source_specific_conversion_layer_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )
    artifacts["both_source_agreement_conversion"] = _png_from_geojson_ref(
        bundle_root=bundle_root,
        relpath=agreement_conversion_ref,
        output_path=evidence_dir / "12_both_source_agreement_conversion.png",
        color=(106, 27, 154, 230),
        unavailable_reason="both_source_agreement_conversion_layer_not_available",
        background_raster_path=satellite_recent_path,
        aoi_geom_wgs84=aoi_geom_wgs84,
    )

    # A standalone interactive Leaflet map: this AOI's own boundary over an Esri World Imagery
    # basemap, with the same JRC 2020 baseline / forest loss / commodity / intersection masks the
    # static evidence PNGs above draw, exposed here as independently toggle-able overlay layers
    # instead of one fixed composite - report.html's "Image Downloads" list links to this single
    # file in place of separate per-layer PNG downloads (see render_canonical_html). Moved below
    # the mask-path resolution above (round 7) so it can be given real overlay geometry instead of
    # just the AOI outline; colors match the static composites' own layer colors for consistency.
    artifacts["aoi_satellite_map"] = _write_esri_leaflet_aoi_map_html(
        aoi_geom_wgs84=aoi_geom_wgs84,
        aoi_name=str(report.get("aoi_id", "unknown")),
        output_path=evidence_dir / "01c_aoi_satellite_map.html",
        overlay_layers=[
            ("JRC Global Forest Cover 2020", baseline_mask_path, (33, 122, 72)),
            (f"Forest loss 2021-{effective_end_year}", loss_mask_path, (198, 40, 40)),
            (f"TMF deforestation 2021-{effective_end_year}", tmf_deforestation_path, (211, 47, 47)),
            (f"TMF degradation 2021-{effective_end_year}", tmf_degradation_path, (245, 124, 0)),
            ("RADD confirmed/high-confidence alerts", radd_confirmed_path, (142, 36, 170)),
            ("RADD low-confidence alerts", radd_low_confidence_path, (251, 192, 45)),
            ("New FDP coffee after baseline", new_fdp_path, (0, 132, 124)),
            ("New MapBiomas coffee after baseline", new_mapbiomas_path, (30, 136, 229)),
            ("Source-specific conversion", source_conversion_path, (245, 124, 0)),
            ("Both-source agreement conversion", agreement_conversion_path, (106, 27, 154)),
            ("Commodity layer", commodity_mask_path, _COMMODITY_OVERLAY_COLOR[:3]),
            ("Loss and commodity intersection", commodity_loss_overlap_path, _COMMODITY_LOSS_OVERLAY_COLOR[:3]),
        ],
    )

    artifacts["before_after"] = _write_before_after_png(
        bundle_root=bundle_root,
        baseline_relpath=satellite_baseline_ref,
        recent_relpath=satellite_recent_ref,
        aoi_geom_wgs84=aoi_geom_wgs84,
        output_path=evidence_dir / "06_before_after.png",
    )

    admin_boundaries_ref = _ref_relpath(satellite_outputs, "regional_admin_boundaries_ref")
    regional_raster_ref = _ref_relpath(satellite_outputs, "regional_raster_ref")
    if regional_raster_ref:
        artifacts["regional_overview"] = _write_regional_overview_png(
            bundle_root=bundle_root,
            raster_relpath=regional_raster_ref,
            aoi_geom_wgs84=aoi_geom_wgs84,
            output_path=evidence_dir / "07_regional_overview.png",
            pad_factor=3.0,
            admin_boundaries_relpath=admin_boundaries_ref,
        )
    elif satellite_recent_ref:
        # Offline fallback: use the locally pinned recent satellite raster when no dedicated
        # regional raster was supplied. The frame is clamped to real raster coverage so this does
        # not invent a wider regional image, but it does keep page 4 populated with real AOI
        # context instead of a gap panel.
        artifacts["regional_overview"] = _write_regional_overview_png(
            bundle_root=bundle_root,
            raster_relpath=satellite_recent_ref,
            aoi_geom_wgs84=aoi_geom_wgs84,
            output_path=evidence_dir / "07_regional_overview.png",
            pad_factor=3.0,
            admin_boundaries_relpath=admin_boundaries_ref,
            allow_variable_width=True,
        )
    else:
        # Last resort for reports with AOI geometry but no local satellite raster. Unlike the
        # Sentinel fallback above, this live service is not pinned and may be unavailable.
        artifacts["regional_overview"] = _write_regional_overview_png_esri(
            bundle_root=bundle_root,
            aoi_geom_wgs84=aoi_geom_wgs84,
            output_path=evidence_dir / "07_regional_overview.png",
            pad_factor=3.0,
            admin_boundaries_relpath=admin_boundaries_ref,
        )

    legend_path = evidence_dir / "legend.png"
    _write_legend_png(legend_path)
    artifacts["legend"] = _available(legend_path, report_root)
    return artifacts


def write_canonical_report_json(path: Path, report: CanonicalReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(report.to_dict()) + b"\n")


def write_canonical_metrics_csv(path: Path, metrics: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["variable", "value", "unit", "source", "notes"])
        for name in sorted(metrics):
            item = metrics[name]
            if not isinstance(item, Mapping):
                continue
            writer.writerow(
                [
                    name,
                    _stable_value_str(item.get("value")),
                    item.get("unit", ""),
                    item.get("provenance", ""),
                    item.get("description", ""),
                ]
            )


def render_canonical_html(report: CanonicalReport, output_path: Path) -> None:
    payload = report.to_dict()
    metrics = payload["metrics"]
    aoi = payload["aoi"]
    commodity = payload["commodity"]
    assessment = payload["assessment"]
    temporal = payload["temporal_scope"]
    layers = payload["layers"]

    commodity_name = _display_value(commodity.get("display_name") or commodity.get("id"))
    country = _display_value(aoi.get("country"))
    evidence_period = f"{temporal['evidence_start_year']}-{temporal['effective_end_year']}"
    has_review = bool(assessment.get("human_review_required"))
    loss_value = _metric_value(metrics, "forest_loss_post_2020_on_baseline_ha")
    has_loss = isinstance(loss_value, (int, float)) and loss_value > 0
    status_title = (
        "Post-2020 forest-loss evidence detected"
        if has_loss
        else "No post-2020 baseline forest loss detected"
    )
    status_detail = (
        "Satellite-derived and geospatial evidence indicates post-2020 loss within the JRC 2020 forest baseline. Human review is required before any legal conclusion."
        if has_review
        else "The configured evidence layers did not detect post-2020 loss within the JRC 2020 forest baseline."
    )
    status_class = "is-warning" if has_review else "is-ok"
    review_label = "Needs review" if has_review else "No review trigger from configured evidence"

    ordered_layer_ids = [
        "satellite",
        "jrc_forest_2020",
        "forest_loss",
        "tmf_deforestation",
        "tmf_degradation",
        "radd_confirmed",
        "radd_low_confidence",
        "radd_alerts",
        "fdp_new_commodity",
        "mapbiomas_new_commodity",
        "source_specific_conversion",
        "both_source_agreement_conversion",
        "commodity",
        "intersection",
    ]
    switcher_layers = [
        layers[key]
        for key in ordered_layer_ids
        if key in layers and _show_layer_in_switcher(key, layers[key])
    ]
    initial_layer = _first_available_layer(
        {str(layer.get("id")): layer for layer in switcher_layers},
        ["forest_loss", "jrc_forest_2020", "commodity", "intersection", "satellite"],
    ) or (switcher_layers[0] if switcher_layers else None)
    # Computed from the un-overridden `layers` (below) so the hero's CSS background - which can
    # only ever be a real image - keeps using the static 01_aoi_satellite.png even after the
    # "satellite" switcher tab is repointed at the interactive map just below.
    hero_layer = _first_available_layer(layers, ["satellite", "intersection", "forest_loss", "jrc_forest_2020"])
    hero_style = (
        f' style="--hero-image:url(\'{_esc_attr(hero_layer["path"])}\')"'
        if hero_layer and hero_layer.get("path")
        else ""
    )

    # Round 7 (geospatial-evidence-framework, coffee_brazil_minas_gerais_eudr_compliant bundle):
    # the "satellite" layer/tab used to be swapped for the interactive Leaflet map here (an
    # .html artifact, see `_write_esri_leaflet_aoi_map_html`) so the "Area Of Interest" viewer
    # opened it directly. That made the panel's static-image use case (a plain, always-loads
    # snapshot) redundant with the interactive map, which remains one click away via the "Image
    # Downloads" list below. Reverted: the "satellite" tab/viewer is the real static PNG again,
    # exactly like every other switcher tab.

    layer_buttons = _render_layer_buttons(switcher_layers, initial_layer, output_path)
    main_viewer = _render_main_viewer(initial_layer, output_path)
    layer_info = _render_layer_info(initial_layer, output_path)
    downloads = _render_layer_downloads(layers, output_path)
    layer_legend = _render_report_layer_legend(
        str(initial_layer.get("id")) if isinstance(initial_layer, Mapping) else None,
        layers,
        output_path,
    )
    state = _display_value(aoi.get("state"), "")
    municipality = _display_value(aoi.get("municipality"), "")

    data_json_raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    data_json = html.escape(data_json_raw)
    metrics_rows = _render_metrics_rows(metrics)
    dataset_rows = _render_dataset_rows(payload.get("datasets") or [])
    method_rows = _render_method_rows(payload.get("methods") or [])
    provenance_rows = _render_provenance_rows(payload)
    gap_rows = _render_gap_rows(payload.get("evidence_gaps") or [])
    artifact_rows = _render_artifact_rows(payload.get("artifacts") or {})
    reference_rows = _render_reference_rows(payload.get("references") or [])
    two_situations_section = _render_two_situations_section(
        metrics,
        commodity,
        temporal,
        evidence_period,
    )
    sentinel_diagnostics_section = _render_sentinel_diagnostics_section(metrics)

    json_href = _artifact_href(payload, "report.json") or "report.json"
    pdf_href = _artifact_href(payload, "report.pdf") or "report.pdf"
    metrics_href = _artifact_href(payload, "metrics.csv") or "metrics.csv"

    doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>EUDR Evidence Package - {html.escape(str(aoi['name']))}</title>
  <style>
    :root {{
      --bg: #ffffff;
      --ink: #090b0e;
      --muted: #68717a;
      --line: #e3e8e2;
      --soft: #f6f8f5;
      --accent: #c7ff16;
      --forest: #17462f;
      --warning: #ff9f0a;
      --danger: #c62828;
      --ok: #16834a;
      --panel: #ffffff;
      --dark: #080a09;
      --radius: 8px;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }}
    a {{ color: inherit; }}
    a:focus-visible, button:focus-visible, summary:focus-visible {{
      outline: 3px solid var(--accent);
      outline-offset: 3px;
    }}
    .topbar {{
      position: sticky;
      top: 0;
      z-index: 20;
      display: grid;
      grid-template-columns: auto minmax(0, 1fr) auto;
      align-items: center;
      gap: 18px;
      min-height: 64px;
      padding: 10px clamp(16px, 4vw, 56px);
      color: #fff;
      background: rgba(7, 8, 10, .96);
    }}
    .brand {{ display: flex; align-items: center; gap: 10px; font-weight: 760; font-size: 20px; }}
    .brand-mark {{ width: 18px; height: 18px; border-radius: 50%; background: var(--accent); box-shadow: 0 0 20px rgba(199,255,22,.35); }}
    .tagline {{ color: #cfd5d0; font-size: 13px; overflow-wrap: anywhere; }}
    .actions, .footer-actions {{ display: flex; flex-wrap: wrap; gap: 10px; justify-content: flex-end; }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 40px;
      border: 1px solid rgba(255,255,255,.2);
      border-radius: 8px;
      padding: 9px 13px;
      background: rgba(255,255,255,.08);
      color: #fff;
      text-decoration: none;
      font: inherit;
      cursor: pointer;
      white-space: nowrap;
    }}
    .btn.primary {{ background: var(--accent); border-color: var(--accent); color: #0b0d0c; font-weight: 760; }}
    .hero {{
      min-height: 520px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(300px, .58fr);
      gap: clamp(28px, 6vw, 88px);
      align-items: center;
      padding: clamp(50px, 7vw, 88px) clamp(18px, 6vw, 82px);
      color: #fff;
      background:
        linear-gradient(90deg, rgba(3,8,6,.98), rgba(3,8,6,.82) 46%, rgba(3,8,6,.3)),
        var(--hero-image, linear-gradient(135deg, #102b1d, #151b16));
      background-size: cover;
      background-position: center;
    }}
    .kicker {{ margin: 0 0 16px; color: var(--accent); font-size: 12px; font-weight: 850; letter-spacing: .08em; text-transform: uppercase; }}
    h1 {{
      margin: 0;
      max-width: 920px;
      font-size: clamp(48px, 7vw, 92px);
      line-height: .95;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }}
    .subtitle {{ margin: 18px 0 0; max-width: 720px; color: #dfe5df; font-size: 18px; }}
    .meta {{ display: grid; gap: 12px; margin-top: 34px; font-size: 13px; color: #d9ded9; }}
    .meta-row {{ display: grid; grid-template-columns: 120px minmax(0, 1fr); gap: 16px; }}
    .meta-row strong {{ color: #fff; overflow-wrap: anywhere; }}
    .status-panel {{
      border: 1px solid rgba(255,255,255,.18);
      border-radius: var(--radius);
      padding: clamp(22px, 3vw, 30px);
      background: rgba(9,12,10,.86);
      box-shadow: 0 24px 72px rgba(0,0,0,.28);
    }}
    .status-label {{ color: var(--warning); font-size: 12px; letter-spacing: .08em; text-transform: uppercase; font-weight: 850; }}
    .status-panel.is-ok .status-label, .status-panel.is-ok h2, .status-panel.is-ok .status-icon {{ color: var(--ok); }}
    .status-panel h2 {{ margin: 12px 0 0; color: var(--warning); font-size: clamp(28px, 3.2vw, 44px); line-height: 1.05; letter-spacing: 0; }}
    .status-head {{ display: grid; grid-template-columns: 1fr 54px; gap: 18px; align-items: start; }}
    .status-icon {{ display: grid; place-items: center; width: 54px; height: 54px; border: 2px solid currentColor; border-radius: 50%; color: var(--warning); font-weight: 850; }}
    .status-panel p {{ margin: 18px 0 0; color: #edf1ed; }}
    main {{ max-width: 1480px; margin: 0 auto; padding: 46px clamp(16px, 4vw, 58px) 78px; }}
    .section {{ margin-top: 58px; }}
    .section-title {{ margin: 0 0 18px; font-size: 12px; text-transform: uppercase; letter-spacing: .08em; font-weight: 850; }}
    .metrics {{ display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 12px; }}
    .card, .panel {{ border: 1px solid var(--line); border-radius: var(--radius); background: var(--panel); }}
    .card {{ min-height: 124px; padding: 20px; }}
    .card-label {{ color: var(--muted); font-size: 12px; }}
    .card-value {{ margin-top: 12px; font-size: clamp(21px, 1.8vw, 30px); font-weight: 770; line-height: 1.1; letter-spacing: 0; overflow-wrap: anywhere; }}
    .card-sub {{ margin-top: 7px; color: var(--muted); font-size: 12px; }}
    .warning .card-value, .warning-value {{ color: #a85f00; }}
    .notice {{ margin-top: 16px; padding: 16px 18px; border-radius: var(--radius); background: #fff8dc; color: #55430a; font-size: 13px; }}
    .two-col {{ display: grid; grid-template-columns: minmax(0, 1.4fr) minmax(300px, .8fr); gap: 28px; align-items: start; }}
    .three-col {{ display: grid; grid-template-columns: 1.05fr .82fr .9fr; gap: 28px; align-items: start; }}
    .viewer {{ overflow: hidden; height: 490px; min-height: 490px; background: #121916; }}
    .viewer img {{ width: 100%; min-height: 490px; height: 100%; object-fit: cover; display: block; background: #121916; }}
    .empty-viewer {{ min-height: 490px; display: grid; place-items: center; padding: 28px; color: #fff; text-align: center; }}
    .legend {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 14px; margin-top: 12px; color: var(--muted); font-size: 12px; }}
    .legend span {{ display: inline-flex; align-items: center; gap: 8px; }}
    .swatch {{ width: 18px; height: 8px; border-radius: 2px; display: inline-block; }}
    .forest {{ background: #217a48; }}
    .loss {{ background: #c62828; }}
    .commodity-swatch {{ background: #1e88e5; }}
    .intersection-swatch {{ background: #662d91; }}
    .tmf-deforestation-swatch {{ background: #d32f2f; }}
    .tmf-degradation-swatch {{ background: #f57c00; }}
    .radd-confirmed-swatch {{ background: #8e24aa; }}
    .radd-low-swatch {{ background: #fbc02d; }}
    .fdp-new-swatch {{ background: #00847c; }}
    .mapbiomas-new-swatch {{ background: #1e88e5; }}
    .source-conversion-swatch {{ background: #f57c00; }}
    .agreement-conversion-swatch {{ background: #6a1b9a; }}
    .detail-list {{ border-top: 1px solid var(--line); }}
    .detail {{ display: grid; grid-template-columns: 142px minmax(0, 1fr); gap: 16px; padding: 14px 0; border-bottom: 1px solid var(--line); font-size: 13px; }}
    .detail-key {{ color: var(--muted); }}
    .detail-value {{ font-weight: 560; overflow-wrap: anywhere; }}
    .tabs {{ display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 14px; }}
    .tab {{
      border: 1px solid #9aaa92;
      border-radius: 8px;
      background: #fff;
      color: #182018;
      padding: 9px 12px;
      font: inherit;
      cursor: pointer;
    }}
    .tab[aria-selected="true"] {{ background: var(--forest); border-color: var(--forest); color: #fff; }}
    .tab[disabled] {{ color: #858d85; background: #f4f6f2; cursor: not-allowed; }}
    .layer-meta {{ padding: 18px; }}
    .layer-meta h3 {{ margin: 0 0 8px; font-size: 22px; letter-spacing: 0; }}
    .layer-meta p {{ margin: 0; color: var(--muted); }}
    .download-list {{ display: grid; gap: 8px; margin-top: 14px; }}
    .download-list a {{ color: #17462f; overflow-wrap: anywhere; }}
    .evidence-row {{ display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 16px; padding: 17px 0; border-bottom: 1px solid var(--line); }}
    .evidence-row:last-child {{ border-bottom: 0; }}
    .evidence-name {{ font-weight: 700; }}
    .evidence-desc {{ margin-top: 4px; color: var(--muted); font-size: 12px; }}
    .evidence-value {{ font-size: 25px; font-weight: 780; letter-spacing: 0; white-space: nowrap; }}
    .timeline {{ position: relative; padding-left: 28px; }}
    .timeline:before {{ content: ""; position: absolute; left: 8px; top: 8px; bottom: 8px; width: 2px; background: #dce9d7; }}
    .time-item {{ position: relative; padding: 0 0 26px 18px; }}
    .time-item:last-child {{ padding-bottom: 0; }}
    .time-dot {{ position: absolute; left: -27px; top: 2px; width: 18px; height: 18px; border: 4px solid #fff; border-radius: 50%; background: var(--accent); box-shadow: 0 0 0 1px #d8e0d6; }}
    .time-item.warning .time-dot {{ background: var(--warning); }}
    .time-date {{ color: var(--muted); font-size: 12px; }}
    .time-name {{ margin-top: 5px; font-weight: 700; }}
    .time-copy {{ margin: 5px 0 0; color: var(--muted); font-size: 12px; }}
    .method-step {{ display: grid; grid-template-columns: 42px minmax(0, 1fr); gap: 14px; align-items: center; padding: 13px 0; border-bottom: 1px solid var(--line); }}
    .method-step:last-child {{ border-bottom: 0; }}
    .method-index {{ display: grid; place-items: center; width: 42px; height: 42px; border-radius: 8px; background: #eef7e9; color: #397b38; font-weight: 850; }}
    .method-step strong {{ display: block; }}
    .method-step span {{ color: var(--muted); font-size: 12px; }}
    .comparison {{ display: grid; grid-template-columns: minmax(0, 1fr); gap: 16px; }}
    .comparison figure {{ margin: 0; }}
    .comparison img {{ width: 100%; height: auto; border: 1px solid var(--line); border-radius: var(--radius); background: #121916; display: block; }}
    .comparison figcaption {{ margin-top: 8px; color: var(--muted); font-size: 12px; }}
    .quality {{ border-top: 1px solid var(--line); }}
    .quality-row {{ display: grid; grid-template-columns: 170px minmax(0, 1fr); gap: 18px; padding: 13px 0; border-bottom: 1px solid var(--line); font-size: 13px; }}
    .quality-row span:first-child {{ color: var(--muted); }}
    .situation-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }}
    .situation-list {{ display: grid; gap: 12px; }}
    .situation-row {{ display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 16px; padding: 12px 0; border-bottom: 1px solid var(--line); }}
    .situation-row:last-child {{ border-bottom: 0; }}
    .situation-name {{ font-weight: 680; }}
    .situation-note {{ margin-top: 3px; color: var(--muted); font-size: 12px; }}
    .situation-value {{ font-weight: 780; white-space: nowrap; }}
    .appendix {{ margin-top: 70px; border-top: 1px solid var(--line); padding-top: 28px; }}
    details {{ border-bottom: 1px solid var(--line); }}
    summary {{ cursor: pointer; padding: 16px 0; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: #3c463d; background: #f7f9f5; }}
    pre {{ overflow: auto; padding: 14px; background: #f7f9f5; border: 1px solid var(--line); border-radius: var(--radius); white-space: pre-wrap; word-break: break-word; }}
    .gap {{ padding: 18px; color: var(--muted); background: #f7f9f5; }}
    footer {{ background: var(--dark); color: #fff; padding: 44px clamp(18px, 6vw, 82px); }}
    .footer-grid {{ max-width: 1480px; margin: 0 auto; display: grid; grid-template-columns: 1fr 1fr .8fr; gap: 42px; }}
    .footer-title {{ margin: 0 0 13px; color: #b8beb8; font-size: 11px; letter-spacing: .08em; text-transform: uppercase; font-weight: 820; }}
    .footer-copy {{ margin: 0; color: #d5dad5; font-size: 12px; }}
    .artifact-links {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 18px; font-size: 12px; color: #dce2dc; }}
    .artifact-links a {{ overflow-wrap: anywhere; }}
    @media (max-width: 1100px) {{
      .hero, .two-col, .three-col, .footer-grid {{ grid-template-columns: 1fr; }}
      .situation-grid {{ grid-template-columns: 1fr; }}
      .metrics {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
    }}
    @media (max-width: 760px) {{
      .topbar {{ grid-template-columns: 1fr; align-items: start; position: static; }}
      .actions {{ justify-content: start; }}
      .hero {{ min-height: 0; padding: 44px 16px; }}
      h1 {{ font-size: 46px; }}
      main {{ padding: 34px 15px 56px; }}
      .metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .detail, .quality-row {{ grid-template-columns: 1fr; gap: 4px; }}
      .viewer, .viewer img, .empty-viewer {{ height: 340px; min-height: 340px; }}
      .comparison, .artifact-links {{ grid-template-columns: 1fr; }}
      .evidence-row {{ grid-template-columns: 1fr; }}
      .evidence-value {{ white-space: normal; }}
    }}
    @media (max-width: 420px) {{
      .metrics {{ grid-template-columns: 1fr; }}
      .meta-row {{ grid-template-columns: 1fr; gap: 3px; }}
    }}
    @media print {{
      .topbar, .tabs, .download-list, .footer-actions {{ display: none !important; }}
      .hero {{ min-height: 0; }}
      .section {{ break-inside: avoid; }}
      footer {{ break-before: page; }}
    }}
  </style>
</head>
<body>
  <script type="application/json" id="report-data">{data_json}</script>
  <header class="topbar">
    <div class="brand"><span class="brand-mark" aria-hidden="true"></span><span>Single.Earth</span></div>
    <div class="tagline">Evidence package for satellite-derived and geospatial review</div>
    <nav class="actions" aria-label="Report downloads">
      <a class="btn" href="{_esc_attr(json_href)}" download>JSON download</a>
      <a class="btn primary" href="{_esc_attr(pdf_href)}" download>PDF download</a>
    </nav>
  </header>

  <section class="hero"{hero_style}>
    <div>
      <p class="kicker">EUDR Evidence Package</p>
      <h1>{html.escape(commodity_name)} - {html.escape(country)}</h1>
      <p class="subtitle">Customer-facing evidence package generated from the canonical report model.</p>
      <div class="meta">
        <div class="meta-row"><span>Report ID</span><strong>{html.escape(str(payload['report_id']))}</strong></div>
        <div class="meta-row"><span>Run ID</span><strong>{html.escape(str(payload['run_id']))}</strong></div>
        <div class="meta-row"><span>Generated UTC</span><strong>{html.escape(str(payload['generated_utc']))}</strong></div>
      </div>
    </div>
    <article class="status-panel {status_class}">
      <div class="status-label">Assessment status</div>
      <div class="status-head"><h2>{html.escape(status_title)}</h2><div class="status-icon" aria-hidden="true">{'!' if has_review else 'OK'}</div></div>
      <p>{html.escape(status_detail)}</p>
    </article>
  </section>

  <main>
    <section>
      <h2 class="section-title">Executive Summary</h2>
      <div class="metrics">
        {_metric_card("Commodity", commodity_name)}
        {_metric_card("Country", country)}
        {_metric_card("AOI area", _fmt_metric(metrics, "aoi_area_ha"), "Area of interest")}
        {_metric_card("JRC 2020 forest baseline", _fmt_metric(metrics, "forest_baseline_2020_ha"), _fmt_metric(metrics, "forest_baseline_2020_percent_of_aoi", suffix=" of AOI"))}
        {_metric_card("Post-2020 forest loss within baseline", _fmt_metric(metrics, "forest_loss_post_2020_on_baseline_ha"), _fmt_metric(metrics, "forest_loss_post_2020_percent_of_baseline", suffix=" of baseline"), warning=has_loss)}
        {_metric_card("Overall review status", review_label)}
      </div>
      <div class="notice">This report contains satellite-derived and geospatial evidence. It does not itself constitute a legal compliance determination.</div>
    </section>

    <section class="section two-col">
      <div>
        <h2 class="section-title">Area Of Interest</h2>
        <div id="layer-panel" class="viewer" role="tabpanel" aria-live="polite">
          {main_viewer}
        </div>
        <div id="layer-legend" class="legend" aria-label="Map legend">
          {layer_legend}
        </div>
      </div>
      <aside>
        <h2 class="section-title">Plot And Data Overview</h2>
        <div class="detail-list">
          {_detail("AOI", aoi.get("name"))}
          {_detail("AOI outline", aoi.get("geometry_ref"))}
          {_detail("CRS", aoi.get("crs"))}
          {_detail("Commodity", commodity_name)}
          {_detail("Country", country)}
          {_detail("State", state)}
          {_detail("Municipality", municipality)}
          {_detail("Evidence period", evidence_period)}
        </div>
      </aside>
    </section>

    <section class="section two-col">
      <div>
        <h2 class="section-title">Layer Switcher</h2>
        <div class="tabs" role="tablist" aria-label="Evidence layers">
          {layer_buttons}
        </div>
        <div id="layer-info" class="panel layer-meta">{layer_info}</div>
      </div>
      <aside>
        <h2 class="section-title">Image Downloads</h2>
        <div id="layer-downloads" class="panel layer-meta">
          {downloads}
        </div>
      </aside>
    </section>

    <section class="section three-col">
      <article>
        <h2 class="section-title">Key Evidence</h2>
        <div class="panel layer-meta">
          {_evidence_row("Baseline forest area", "JRC 2020 forest baseline inside AOI", _fmt_metric(metrics, "forest_baseline_2020_ha"))}
          {_evidence_row("Post-2020 loss area", f"Detected during {evidence_period}", _fmt_metric(metrics, "forest_loss_post_2020_on_baseline_ha"), warning=has_loss)}
          {_evidence_row("Baseline forest without detected loss", "Baseline forest minus detected post-2020 loss", _fmt_ha(_baseline_without_loss(metrics)))}
          {_evidence_row("Commodity intersection", "Overlap between commodity evidence and post-2020 baseline loss", _fmt_metric(metrics, "post_2020_loss_and_commodity_overlap_ha"))}
        </div>
      </article>
      <article>
        <h2 class="section-title">Assessment Timeline</h2>
        <div class="panel layer-meta timeline">
          {_timeline_item(str(temporal["cutoff_date"]), "EUDR cutoff date", "Baseline date used for forest status.")}
          {_timeline_item(evidence_period, "Evidence period", "Post-2020 forest-loss screening.", warning=has_loss)}
          {_timeline_item(_display_value(commodity.get("observation_year")), "Commodity observation year", "Available when a configured commodity layer supplies it.")}
          {_timeline_item(str(payload["generated_utc"]), "Assessment generation date", "Evidence package assembled from canonical inputs.")}
        </div>
      </article>
      <article>
        <h2 class="section-title">Methodology</h2>
        <div class="panel layer-meta">
          {_method_step(1, "AOI", "Structured area of interest geometry and area calculation.")}
          {_method_step(2, "JRC forest baseline", "JRC Global Forest Cover 2020 baseline within the AOI.")}
          {_method_step(3, "Hansen loss", "Post-2020 loss-year evidence filtered to the assessment period.")}
          {_method_step(4, "Optional commodity layer", "Configured commodity evidence when available for this AOI.")}
          {_method_step(5, "Spatial intersection", "Derived overlaps between AOI, baseline forest, loss, and commodity evidence.")}
          {_method_step(6, "Evidence-package generation", "Canonical JSON, HTML, PDF, metrics, and checksums generated for audit.")}
        </div>
      </article>
    </section>

    {two_situations_section}
    {sentinel_diagnostics_section}

    <section class="section two-col">
      <div>
        <h2 class="section-title">Satellite Evidence</h2>
        {_render_satellite_evidence(layers, output_path)}
      </div>
      <aside>
        <h2 class="section-title">Data And Quality</h2>
        <div class="quality">
          {_quality_row("Datasets and versions", _dataset_summary(payload.get("datasets") or []))}
          {_quality_row("Temporal coverage", f"{evidence_period}; cutoff {temporal['cutoff_date']}")}
          {_quality_row("Spatial resolution", _spatial_resolution(payload))}
          {_quality_row("Processing method", "Automated geospatial raster and vector intersection")}
          {_quality_row("Coverage status", _display_value(commodity.get("coverage_status")))}
          {_quality_row("Confidence basis", "Deterministic source artifacts, metrics, and checksum manifest where available")}
          {_quality_row("Evidence gaps", _gap_summary(payload.get("evidence_gaps") or []))}
          {_quality_row("Human-review requirement", "Required" if has_review else "No review trigger from configured evidence")}
        </div>
      </aside>
    </section>

    <section class="appendix">
      <h2 class="section-title">Technical Appendix</h2>
      <details open><summary>Metrics table</summary><table><thead><tr><th>Metric</th><th>Value</th><th>Unit</th><th>Provenance</th><th>Description</th></tr></thead><tbody>{metrics_rows}</tbody></table></details>
      <details><summary>Datasets</summary><table><thead><tr><th>Dataset</th><th>Version</th><th>Source</th><th>Retrieved UTC</th><th>License</th></tr></thead><tbody>{dataset_rows}</tbody></table></details>
      <details><summary>Methods</summary><table><thead><tr><th>Method</th><th>Details</th></tr></thead><tbody>{method_rows}</tbody></table></details>
      <details><summary>Provenance and audit metadata</summary><table><thead><tr><th>Field</th><th>Value</th></tr></thead><tbody>{provenance_rows}</tbody></table></details>
      <details><summary>Checksums and generated artifacts</summary><table><thead><tr><th>Artifact</th><th>Path</th><th>Status</th><th>Checksum</th></tr></thead><tbody>{artifact_rows}</tbody></table></details>
      <details><summary>References</summary><table><thead><tr><th>Reference</th><th>Details</th></tr></thead><tbody>{reference_rows}</tbody></table></details>
      <details><summary>Limitations and evidence gaps</summary><table><thead><tr><th>Gap</th><th>Status</th><th>Path</th></tr></thead><tbody>{gap_rows}</tbody></table></details>
      <details><summary>Canonical JSON</summary><pre>{data_json}</pre></details>
    </section>
  </main>

  <footer>
    <div class="footer-grid">
      <section>
        <h2 class="footer-title">Generated Artifacts</h2>
        <div class="artifact-links">
          <a href="{_esc_attr(json_href)}">report.json</a>
          <a href="{_esc_attr(pdf_href)}">report.pdf</a>
          <a href="{_esc_attr(metrics_href)}">metrics.csv</a>
          <span>manifest.sha256</span>
        </div>
      </section>
      <section>
        <h2 class="footer-title">Non-decision disclaimer</h2>
        <p class="footer-copy">This evidence package supports review. It is not sufficient for a legal compliance determination without human assessment and supply-chain documentation.</p>
      </section>
      <section>
        <h2 class="footer-title">Downloads</h2>
        <div class="footer-actions">
          <a class="btn primary" href="{_esc_attr(pdf_href)}" download>PDF download</a>
          <a class="btn" href="{_esc_attr(json_href)}" download>JSON download</a>
        </div>
      </section>
    </div>
  </footer>
  <script>
    (() => {{
      const report = JSON.parse(document.getElementById("report-data").textContent);
      const buttons = Array.from(document.querySelectorAll("[data-layer]"));
      const panel = document.getElementById("layer-panel");
      const legend = document.getElementById("layer-legend");
      const info = document.getElementById("layer-info");
      const downloads = document.getElementById("layer-downloads");
      const exists = new Map(buttons.map((button) => [button.dataset.layer, button.dataset.pathStatus]));
      const escapeHTML = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"}}[char]));
      const isHtmlPath = (path) => String(path || "").toLowerCase().endsWith(".html");
      const swatches = {{
        jrc_forest_2020: [["forest", "JRC forest baseline"], ["commodity-swatch", "Commodity layer"]],
        forest_loss: [["loss", "Forest loss"], ["forest", "JRC forest baseline"], ["commodity-swatch", "Commodity layer"], ["intersection-swatch", "Loss and commodity intersection"]],
        tmf_deforestation: [["tmf-deforestation-swatch", "TMF deforestation"]],
        tmf_degradation: [["tmf-degradation-swatch", "TMF degradation"]],
        radd_confirmed: [["radd-confirmed-swatch", "RADD confirmed/high-confidence alerts"]],
        radd_low_confidence: [["radd-low-swatch", "RADD low-confidence alerts"]],
        radd_alerts: [["radd-confirmed-swatch", "RADD confirmed/high-confidence alerts"], ["radd-low-swatch", "RADD low-confidence alerts"]],
        fdp_new_commodity: [["fdp-new-swatch", "New FDP coffee"]],
        mapbiomas_new_commodity: [["mapbiomas-new-swatch", "New MapBiomas coffee"]],
        source_specific_conversion: [["source-conversion-swatch", "Source-specific conversion"]],
        both_source_agreement_conversion: [["agreement-conversion-swatch", "Both-source agreement conversion"]],
        commodity: [["commodity-swatch", "Commodity layer"]],
        intersection: [["intersection-swatch", "Loss and commodity intersection"]],
      }};
      function layerLegendRows(key) {{
        const rows = (swatches[key] || [])
          .filter((entry) => {{
            const layerKey = entry[0] === "forest" ? "jrc_forest_2020"
              : entry[0] === "loss" ? "forest_loss"
              : entry[0] === "commodity-swatch" ? "commodity"
              : entry[0] === "intersection-swatch" ? "intersection"
              : entry[0] === "radd-confirmed-swatch" ? "radd_confirmed"
              : entry[0] === "radd-low-swatch" ? "radd_low_confidence"
              : key;
            const layer = report.layers[layerKey];
            return layer && layer.available && layer.path && exists.get(layerKey) !== "missing";
          }})
          .map(([cssClass, label]) => `<span><i class="swatch ${{cssClass}}"></i>${{escapeHTML(label)}}</span>`);
        return rows.length ? rows.join("") : "<span>No rendered overlay layers</span>";
      }}
      function layerDownloadRows() {{
        const rows = buttons
          .filter((button) => !button.disabled && button.dataset.path)
          .map((button) => {{
            const path = button.dataset.path;
            const label = escapeHTML(button.textContent.trim());
            return isHtmlPath(path)
              ? `<a href="${{escapeHTML(path)}}" target="_blank" rel="noopener">${{label}} (interactive map)</a>`
              : `<a href="${{escapeHTML(path)}}" download>${{label}}</a>`;
          }});
        return rows.length ? `<div class="download-list">${{rows.join("")}}</div>` : `<div class="gap">No downloadable layer images are available.</div>`;
      }}
      function selectLayer(key) {{
        const layer = report.layers[key];
        if (!layer) return;
        buttons.forEach((button) => button.setAttribute("aria-selected", String(button.dataset.layer === key)));
        const pathStatus = exists.get(key);
        if (layer.available && layer.path && pathStatus !== "missing") {{
          panel.innerHTML = isHtmlPath(layer.path)
            ? `<iframe src="${{escapeHTML(layer.path)}}" title="${{escapeHTML(layer.title)}} interactive map" loading="lazy" style="width:100%;height:100%;border:0;"></iframe>`
            : `<img src="${{escapeHTML(layer.path)}}" alt="${{escapeHTML(layer.title)}} evidence layer">`;
        }} else {{
          const reason = pathStatus === "missing" ? "Declared artifact path was not found in the generated bundle." : (layer.availability_status || "Layer unavailable.");
          panel.innerHTML = `<div class="empty-viewer">${{escapeHTML(reason)}}</div>`;
        }}
        info.innerHTML = `<h3>${{escapeHTML(layer.title)}}</h3><p>${{escapeHTML(layer.purpose || "")}}</p><div class="detail-list"><div class="detail"><div class="detail-key">Dataset</div><div class="detail-value">${{escapeHTML(layer.dataset || "")}}</div></div><div class="detail"><div class="detail-key">Date/version</div><div class="detail-value">${{escapeHTML(layer.date || layer.dataset_version || "")}}</div></div><div class="detail"><div class="detail-key">Availability</div><div class="detail-value">${{escapeHTML(layer.availability_status || "")}}</div></div></div>`;
        legend.innerHTML = layerLegendRows(key);
        downloads.innerHTML = layerDownloadRows();
      }}
      buttons.forEach((button) => button.addEventListener("click", () => selectLayer(button.dataset.layer)));
      const selected = buttons.find((button) => button.getAttribute("aria-selected") === "true") || buttons.find((button) => !button.disabled);
      if (selected) selectLayer(selected.dataset.layer);
    }})();
  </script>
</body>
</html>
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(doc, encoding="utf-8")


def _metric_value(metrics: Mapping[str, Any], name: str) -> Any:
    item = metrics.get(name)
    return item.get("value") if isinstance(item, Mapping) else None


def _display_value(value: Any, fallback: str = "Unspecified") -> str:
    if value is None or value == "":
        return fallback
    return str(value)


def _fmt_metric(metrics: Mapping[str, Any], name: str, *, suffix: str = "") -> str:
    item = metrics.get(name)
    if not isinstance(item, Mapping):
        return "Unavailable"
    value = item.get("value")
    unit = str(item.get("unit") or "")
    if value is None:
        return "Unavailable"
    if unit == "ha":
        return _fmt_ha(value)
    if unit == "percent":
        return f"{_fmt_number(value)}%{suffix}"
    if unit:
        return f"{_fmt_number(value)} {unit}{suffix}"
    return f"{_fmt_number(value)}{suffix}"


def _fmt_ha(value: Any) -> str:
    if value is None:
        return "Unavailable"
    return f"{_fmt_number(value)} ha"


def _fmt_number(value: Any) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if abs(value) >= 100:
            return f"{value:,.1f}"
        return f"{value:,.3f}".rstrip("0").rstrip(".")
    return str(value)


def _metric_card(label: str, value: str, sub: str | None = None, *, warning: bool = False) -> str:
    cls = "card warning" if warning else "card"
    sub_html = f'<div class="card-sub">{html.escape(sub)}</div>' if sub else ""
    return (
        f'<article class="{cls}"><div class="card-label">{html.escape(label)}</div>'
        f'<div class="card-value">{html.escape(value)}</div>{sub_html}</article>'
    )


def _detail(label: str, value: Any) -> str:
    return (
        f'<div class="detail"><div class="detail-key">{html.escape(label)}</div>'
        f'<div class="detail-value">{html.escape(_display_value(value))}</div></div>'
    )


def _evidence_row(label: str, description: str, value: str, *, warning: bool = False) -> str:
    cls = "evidence-value warning-value" if warning else "evidence-value"
    return (
        '<div class="evidence-row">'
        f'<div><div class="evidence-name">{html.escape(label)}</div>'
        f'<div class="evidence-desc">{html.escape(description)}</div></div>'
        f'<div class="{cls}">{html.escape(value)}</div></div>'
    )


def _timeline_item(date: str, name: str, copy: str, *, warning: bool = False) -> str:
    cls = "time-item warning" if warning else "time-item"
    return (
        f'<div class="{cls}"><span class="time-dot"></span>'
        f'<div class="time-date">{html.escape(date)}</div>'
        f'<div class="time-name">{html.escape(name)}</div>'
        f'<p class="time-copy">{html.escape(copy)}</p></div>'
    )


def _method_step(index: int, title: str, detail: str) -> str:
    return (
        '<div class="method-step">'
        f'<div class="method-index">{index}</div>'
        f'<div><strong>{html.escape(title)}</strong><span>{html.escape(detail)}</span></div>'
        '</div>'
    )


def _quality_row(label: str, value: str) -> str:
    return (
        f'<div class="quality-row"><span>{html.escape(label)}</span>'
        f'<strong>{html.escape(value)}</strong></div>'
    )


def _show_layer_in_switcher(key: str, layer: Mapping[str, Any]) -> bool:
    if key == "commodity" and not layer.get("available"):
        return False
    if key == "before_after" or key == "legend":
        return False
    return True


def _first_available_layer(layers: Mapping[str, Any], keys: list[str]) -> Mapping[str, Any] | None:
    for key in keys:
        layer = layers.get(key)
        if isinstance(layer, Mapping) and layer.get("available") and layer.get("path"):
            return layer
    return None


def _layer_path_status(layer: Mapping[str, Any], output_path: Path) -> str:
    relpath = layer.get("path")
    if not relpath:
        return "none"
    if (output_path.parent / str(relpath)).is_file():
        return "present"
    return "missing"


def _render_layer_buttons(
    layers: list[Mapping[str, Any]],
    initial_layer: Mapping[str, Any] | None,
    output_path: Path,
) -> str:
    rows = []
    selected_id = initial_layer.get("id") if isinstance(initial_layer, Mapping) else None
    for layer in layers:
        available = bool(layer.get("available") and layer.get("path"))
        disabled = "" if available else " disabled"
        selected = "true" if layer.get("id") == selected_id else "false"
        path = _esc_attr(layer.get("path") or "")
        path_status = _layer_path_status(layer, output_path)
        rows.append(
            f'<button class="tab" type="button" role="tab" data-layer="{_esc_attr(layer.get("id"))}" '
            f'data-path="{path}" data-path-status="{path_status}" aria-selected="{selected}"{disabled}>'
            f'{html.escape(str(layer.get("title") or layer.get("id")))}</button>'
        )
    return "".join(rows) or '<p class="gap">No report layers were declared.</p>'


def _is_html_layer_path(path: Any) -> bool:
    return str(path or "").lower().endswith(".html")


def _render_main_viewer(layer: Mapping[str, Any] | None, output_path: Path) -> str:
    if not isinstance(layer, Mapping):
        return '<div class="empty-viewer">No evidence layer is available.</div>'
    status = _layer_path_status(layer, output_path)
    if layer.get("available") and layer.get("path") and status != "missing":
        if _is_html_layer_path(layer["path"]):
            return (
                f'<iframe src="{_esc_attr(layer["path"])}" '
                f'title="{_esc_attr(layer.get("title") or "Evidence layer")} interactive map" '
                'loading="lazy" style="width:100%;height:100%;border:0;"></iframe>'
            )
        return (
            f'<img src="{_esc_attr(layer["path"])}" '
            f'alt="{_esc_attr(layer.get("title") or "Evidence layer")} evidence layer">'
        )
    reason = (
        "Declared artifact path was not found in the generated bundle."
        if status == "missing"
        else str(layer.get("availability_status") or "Layer unavailable.")
    )
    return f'<div class="empty-viewer">{html.escape(reason)}</div>'


def _render_layer_info(layer: Mapping[str, Any] | None, output_path: Path) -> str:
    if not isinstance(layer, Mapping):
        return '<div class="gap">No layer selected.</div>'
    status = _layer_path_status(layer, output_path)
    availability = (
        "declared_path_missing"
        if status == "missing"
        else str(layer.get("availability_status") or "")
    )
    return (
        f'<h3>{html.escape(str(layer.get("title") or ""))}</h3>'
        f'<p>{html.escape(str(layer.get("purpose") or ""))}</p>'
        '<div class="detail-list">'
        f'{_detail("Dataset", layer.get("dataset"))}'
        f'{_detail("Date/version", layer.get("date") or layer.get("dataset_version"))}'
        f'{_detail("Availability", availability)}'
        '</div>'
    )


def _render_layer_downloads(layers: Mapping[str, Any], output_path: Path) -> str:
    downloadable_ids = [
        "satellite_interactive_map",
        "jrc_forest_2020",
        "forest_loss",
        "tmf_deforestation",
        "tmf_degradation",
        "radd_confirmed",
        "radd_low_confidence",
        "radd_alerts",
        "commodity",
        "intersection",
        "before_after",
    ]
    rows = []
    for key in downloadable_ids:
        layer = layers.get(key) if isinstance(layers, Mapping) else None
        if not (
            isinstance(layer, Mapping)
            and layer.get("available")
            and layer.get("path")
            and _layer_path_status(layer, output_path) != "missing"
        ):
            continue
        title = html.escape(str(layer.get("title") or key))
        path = _esc_attr(layer["path"])
        if _is_html_layer_path(layer["path"]):
            rows.append(
                f'<a href="{path}" target="_blank" rel="noopener">{title} (interactive map)</a>'
            )
        else:
            rows.append(f'<a href="{path}" download>{title}</a>')
    if rows:
        return f'<div class="download-list">{"".join(rows)}</div>'

    # Backward-compatible fallback for older reports where only the interactive map was emitted.
    interactive = layers.get("satellite_interactive_map") if isinstance(layers, Mapping) else None
    if (
        isinstance(interactive, Mapping)
        and interactive.get("available")
        and interactive.get("path")
        and _layer_path_status(interactive, output_path) != "missing"
    ):
        title = html.escape(str(interactive.get("title") or "Evidence layers"))
        link = f'<a href="{_esc_attr(interactive["path"])}" target="_blank" rel="noopener">{title}</a>'
        return f'<div class="download-list">{link}</div>'
    return '<div class="gap">No downloadable layer images are available.</div>'


def _render_satellite_evidence(layers: Mapping[str, Any], output_path: Path) -> str:
    before_after = layers.get("before_after")
    if not (
        isinstance(before_after, Mapping)
        and before_after.get("available")
        and before_after.get("path")
        and _layer_path_status(before_after, output_path) != "missing"
    ):
        return '<div class="panel gap">Before/after imagery is not available in this bundle. No comparison has been fabricated.</div>'
    return (
        '<div class="comparison"><figure>'
        f'<img src="{_esc_attr(before_after["path"])}" alt="{_esc_attr(before_after.get("title") or "Before/after evidence")}">'
        f'<figcaption>{html.escape(str(before_after.get("title") or "Before/after evidence"))}</figcaption>'
        '</figure></div>'
    )


def _layer_is_rendered(layers: Mapping[str, Any], key: str, output_path: Path) -> bool:
    layer = layers.get(key)
    return bool(
        isinstance(layer, Mapping)
        and layer.get("available")
        and layer.get("path")
        and _layer_path_status(layer, output_path) != "missing"
    )


def _render_report_layer_legend(
    selected_layer_id: str | None,
    layers: Mapping[str, Any],
    output_path: Path,
) -> str:
    by_layer = {
        "jrc_forest_2020": [
            ("jrc_forest_2020", "forest", "JRC forest baseline"),
            ("baseline_commodity", "commodity-swatch", "Baseline commodity layer"),
        ],
        "forest_loss": [
            ("forest_loss", "loss", "Forest loss"),
            ("jrc_forest_2020", "forest", "JRC forest baseline"),
            ("commodity", "commodity-swatch", "Commodity layer"),
            ("intersection", "intersection-swatch", "Loss and commodity intersection"),
        ],
        "tmf_deforestation": [
            ("tmf_deforestation", "tmf-deforestation-swatch", "TMF deforestation")
        ],
        "tmf_degradation": [
            ("tmf_degradation", "tmf-degradation-swatch", "TMF degradation")
        ],
        "radd_confirmed": [
            ("radd_confirmed", "radd-confirmed-swatch", "RADD confirmed/high-confidence alerts")
        ],
        "radd_low_confidence": [
            ("radd_low_confidence", "radd-low-swatch", "RADD low-confidence alerts")
        ],
        "radd_alerts": [
            ("radd_confirmed", "radd-confirmed-swatch", "RADD confirmed/high-confidence alerts"),
            ("radd_low_confidence", "radd-low-swatch", "RADD low-confidence alerts"),
        ],
        "fdp_new_commodity": [("fdp_new_commodity", "fdp-new-swatch", "New FDP coffee")],
        "mapbiomas_new_commodity": [
            ("mapbiomas_new_commodity", "mapbiomas-new-swatch", "New MapBiomas coffee")
        ],
        "source_specific_conversion": [
            ("source_specific_conversion", "source-conversion-swatch", "Source-specific conversion")
        ],
        "both_source_agreement_conversion": [
            (
                "both_source_agreement_conversion",
                "agreement-conversion-swatch",
                "Both-source agreement conversion",
            )
        ],
        "commodity": [("commodity", "commodity-swatch", "Commodity layer")],
        "intersection": [("intersection", "intersection-swatch", "Loss and commodity intersection")],
    }
    specs = by_layer.get(str(selected_layer_id or ""), [])
    rows = [
        f'<span><i class="swatch {css_class}"></i>{html.escape(label)}</span>'
        for key, css_class, label in specs
        if _layer_is_rendered(layers, key, output_path)
    ]
    return "".join(rows) or "<span>No rendered overlay layers</span>"


def _render_two_situations_section(
    metrics: Mapping[str, Any],
    commodity: Mapping[str, Any],
    temporal: Mapping[str, Any],
    evidence_period: str,
) -> str:
    baseline_year = _metric_value(metrics, "commodity_baseline_observation_year") or 2020
    latest_year = commodity.get("observation_year") or _metric_value(metrics, "commodity_observation_year")
    commodity_name = _display_value(commodity.get("display_name") or commodity.get("id"))

    baseline_rows = [
        _situation_row(
            "JRC forest baseline",
            f"Forest evidence at the {temporal['cutoff_date']} cutoff",
            _fmt_metric(metrics, "forest_baseline_2020_ha"),
        ),
        _situation_row(
            f"{commodity_name} baseline by primary source",
            f"Configured commodity evidence for {baseline_year}",
            _fmt_metric(metrics, "baseline_forest_and_commodity_overlap_ha"),
        ),
        _situation_row(
            "FDP new-coffee baseline denominator",
            "Only rendered when both FDP years are configured",
            _fmt_metric(metrics, "fdp_new_commodity_since_baseline_ha"),
        ),
        _situation_row(
            "MapBiomas new-coffee baseline denominator",
            "Only rendered when both MapBiomas years are configured",
            _fmt_metric(metrics, "mapbiomas_new_commodity_since_baseline_ha"),
        ),
    ]
    change_rows = [
        _situation_row(
            "JRC/Hansen forest loss",
            f"Loss inside JRC baseline during {evidence_period}",
            _fmt_metric(metrics, "forest_loss_post_2020_on_baseline_ha"),
        ),
        _situation_row(
            f"New {commodity_name.lower()} after baseline",
            f"Latest commodity evidence ({_display_value(latest_year)}) minus {baseline_year}",
            _fmt_metric(metrics, "new_commodity_since_baseline_ha"),
        ),
        _situation_row(
            "Loss and new commodity",
            "Strict conversion-screening signal",
            _fmt_metric(metrics, "post_2020_loss_and_new_commodity_overlap_ha"),
        ),
        _situation_row(
            "Both-source agreement conversion",
            "Loss intersecting new commodity where FDP and MapBiomas agree",
            _fmt_metric(metrics, "post_2020_loss_and_both_source_agreement_new_commodity_overlap_ha"),
        ),
    ]
    return f"""
    <section class="section">
      <h2 class="section-title">Two Situations</h2>
      <div class="situation-grid">
        <article class="panel layer-meta">
          <h3>Baseline {html.escape(str(baseline_year))}</h3>
          <div class="situation-list">{''.join(baseline_rows)}</div>
        </article>
        <article class="panel layer-meta">
          <h3>Change {html.escape(evidence_period)}</h3>
          <div class="situation-list">{''.join(change_rows)}</div>
        </article>
      </div>
    </section>
"""


def _situation_row(name: str, note: str, value: str) -> str:
    return (
        '<div class="situation-row">'
        f'<div><div class="situation-name">{html.escape(name)}</div>'
        f'<div class="situation-note">{html.escape(note)}</div></div>'
        f'<div class="situation-value">{html.escape(value)}</div>'
        '</div>'
    )


def _render_sentinel_diagnostics_section(metrics: Mapping[str, Any]) -> str:
    rows = []
    years = sorted(
        {
            key.split("_")[1]
            for key in metrics
            if key.startswith("s2_") and key.endswith("_scene_count")
        }
    )
    for year in years:
        rows.append(
            "<tr>"
            f"<td>{html.escape(year)}</td>"
            f"<td>{html.escape(_fmt_metric(metrics, f's2_{year}_scene_count'))}</td>"
            f"<td>{html.escape(_display_value(_metric_value(metrics, f's2_{year}_least_cloudy_scene_date')))}</td>"
            f"<td>{html.escape(_fmt_metric(metrics, f's2_{year}_least_cloudy_scene_cloud_pct'))}</td>"
            f"<td>{html.escape(_fmt_metric(metrics, f's2_{year}_mean_valid_obs_per_pixel'))}</td>"
            f"<td>{html.escape(_fmt_metric(metrics, f's2_{year}_min_valid_obs_per_pixel'))}</td>"
            "</tr>"
        )
    if not rows:
        return ""
    return (
        '<section class="section">'
        '<h2 class="section-title">Sentinel-2 Scene Depth</h2>'
        '<div class="panel layer-meta">'
        '<table><thead><tr><th>Year</th><th>Scene count</th><th>Least-cloudy date</th>'
        '<th>Cloud %</th><th>Mean valid obs</th><th>Min valid obs</th></tr></thead>'
        f"<tbody>{''.join(rows)}</tbody></table>"
        '</div></section>'
    )


def _baseline_without_loss(metrics: Mapping[str, Any]) -> float | None:
    baseline = _metric_value(metrics, "forest_baseline_2020_ha")
    loss = _metric_value(metrics, "forest_loss_post_2020_on_baseline_ha")
    if isinstance(baseline, (int, float)) and isinstance(loss, (int, float)):
        return max(float(baseline) - float(loss), 0.0)
    return None


def _dataset_summary(datasets: list[Any]) -> str:
    if not datasets:
        return "No datasets declared"
    names = []
    for dataset in datasets[:3]:
        if isinstance(dataset, Mapping):
            label = dataset.get("dataset_id") or dataset.get("id") or dataset.get("title")
            version = dataset.get("version")
            names.append(f"{label} ({version})" if version else str(label))
    suffix = " and more" if len(datasets) > 3 else ""
    return ", ".join(name for name in names if name) + suffix


def _spatial_resolution(payload: Mapping[str, Any]) -> str:
    """Extract the real analysis-grid resolution(s) declared by the report's own methodology
    entries, instead of always falling back to a vague pointer. Each `methods[]` entry may carry
    either a flat `resolution.pixel_size_m` (legacy Hansen methodology block) or a
    `calculation.target_resolution_m` + `calculation.target_crs` pair (JRC/Hansen and commodity
    intersection methodology blocks); both are real, declared values, never invented here."""
    labels: list[str] = []
    for method in payload.get("methods") or []:
        if not isinstance(method, Mapping):
            continue
        resolution = method.get("resolution")
        if isinstance(resolution, Mapping) and resolution.get("pixel_size_m") is not None:
            label = f"{resolution['pixel_size_m']} m"
            if label not in labels:
                labels.append(label)
        calculation = method.get("calculation")
        if isinstance(calculation, Mapping) and calculation.get("target_resolution_m") is not None:
            target_crs = calculation.get("target_crs")
            label = (
                f"{calculation['target_resolution_m']} m ({target_crs})"
                if target_crs
                else f"{calculation['target_resolution_m']} m"
            )
            if label not in labels:
                labels.append(label)
    if labels:
        return ", ".join(labels) + " analysis grid"
    return "See methods and source dataset metadata"


def _processing_summary(payload: Mapping[str, Any]) -> str:
    """Describe the actual processing that ran, from the report's own declared methodology.

    Deliberately does not assert a processing engine (e.g. Google Earth Engine) that this
    counterpart does not use: JRC/Hansen intersection here runs as local deterministic raster
    processing (rasterio/NumPy), not a remote GEE computation.
    """
    methods_list = [m for m in (payload.get("methods") or []) if isinstance(m, Mapping)]
    labels: list[str] = []
    for method in methods_list:
        calculation = method.get("calculation")
        if isinstance(calculation, Mapping):
            label = calculation.get("method")
            if label and label not in labels:
                labels.append(str(label))
    if labels:
        return "Local raster processing (rasterio/NumPy): " + ", ".join(labels)
    return "Local raster processing (rasterio/NumPy)"


def _gap_summary(gaps: list[Any]) -> str:
    if not gaps:
        return "No evidence gaps declared"
    return f"{len(gaps)} declared evidence gap{'s' if len(gaps) != 1 else ''}"


def _artifact_href(payload: Mapping[str, Any], relpath: str) -> str | None:
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, Mapping):
        return None
    item = artifacts.get(relpath)
    if isinstance(item, Mapping) and item.get("path"):
        return str(item["path"])
    return relpath


def _render_metrics_rows(metrics: Mapping[str, Any]) -> str:
    rows = []
    for name, item in sorted(metrics.items()):
        if not isinstance(item, Mapping):
            continue
        rows.append(
            "<tr>"
            f"<th>{html.escape(str(name))}</th>"
            f"<td>{html.escape(_stable_value_str(item.get('value')))}</td>"
            f"<td>{html.escape(str(item.get('unit', '')))}</td>"
            f"<td>{html.escape(str(item.get('provenance', '')))}</td>"
            f"<td>{html.escape(str(item.get('description', '')))}</td>"
            "</tr>"
        )
    return "".join(rows) or '<tr><td colspan="5">No metrics declared</td></tr>'


def _render_dataset_rows(datasets: list[Any]) -> str:
    rows = []
    for dataset in datasets:
        if not isinstance(dataset, Mapping):
            continue
        rows.append(
            "<tr>"
            f"<th>{html.escape(str(dataset.get('dataset_id') or dataset.get('id') or 'dataset'))}</th>"
            f"<td>{html.escape(str(dataset.get('version') or ''))}</td>"
            f"<td>{html.escape(str(dataset.get('source_url') or dataset.get('source') or ''))}</td>"
            f"<td>{html.escape(str(dataset.get('retrieved_at_utc') or ''))}</td>"
            f"<td>{html.escape(str(dataset.get('license') or ''))}</td>"
            "</tr>"
        )
    return "".join(rows) or '<tr><td colspan="5">No datasets declared</td></tr>'


def _render_method_rows(methods: list[Any]) -> str:
    rows = []
    for method in methods:
        if not isinstance(method, Mapping):
            continue
        method_id = method.get("id") or "method"
        rows.append(
            f"<tr><th>{html.escape(str(method_id))}</th>"
            f"<td><pre>{html.escape(json.dumps(method, sort_keys=True, indent=2, default=str))}</pre></td></tr>"
        )
    return "".join(rows) or '<tr><td colspan="2">No methods declared</td></tr>'


def _render_provenance_rows(payload: Mapping[str, Any]) -> str:
    rows = [
        ("Schema version", payload.get("schema_version")),
        ("Report ID", payload.get("report_id")),
        ("Run ID", payload.get("run_id")),
        ("Generated UTC", payload.get("generated_utc")),
        ("Audit metadata", json.dumps(payload.get("audit") or {}, sort_keys=True, indent=2, default=str)),
    ]
    return "".join(
        f"<tr><th>{html.escape(label)}</th><td><pre>{html.escape(str(value))}</pre></td></tr>"
        for label, value in rows
    )


def _render_gap_rows(gaps: list[Any]) -> str:
    rows = []
    for gap in gaps:
        if not isinstance(gap, Mapping):
            continue
        label = gap.get("artifact_id") or gap.get("gap_id") or gap.get("code") or "gap"
        detail = gap.get("status") or gap.get("reason") or gap.get("description") or gap.get("message") or ""
        rows.append(
            "<tr>"
            f"<th>{html.escape(str(label))}</th>"
            f"<td>{html.escape(str(detail))}</td>"
            f"<td>{html.escape(str(gap.get('path') or ''))}</td>"
            "</tr>"
        )
    return "".join(rows) or '<tr><td colspan="3">No evidence gaps declared</td></tr>'


def _render_artifact_rows(artifacts: Mapping[str, Any]) -> str:
    rows = []
    for key, artifact in sorted(artifacts.items(), key=lambda kv: str(kv[0])):
        if not isinstance(artifact, Mapping):
            continue
        rows.append(
            "<tr>"
            f"<th>{html.escape(str(key))}</th>"
            f"<td>{html.escape(str(artifact.get('path') or ''))}</td>"
            f"<td>{html.escape(str(artifact.get('availability_status') or artifact.get('available') or ''))}</td>"
            f"<td>{html.escape(str(artifact.get('checksum_sha256') or ''))}</td>"
            "</tr>"
        )
    return "".join(rows) or '<tr><td colspan="4">No artifacts declared</td></tr>'


def _render_reference_rows(references: list[Any]) -> str:
    rows = []
    for ref in references:
        if isinstance(ref, Mapping):
            label = ref.get("id") or ref.get("title") or ref.get("url") or "reference"
            value = json.dumps(ref, sort_keys=True, indent=2, default=str)
        else:
            label = "reference"
            value = str(ref)
        rows.append(
            f"<tr><th>{html.escape(str(label))}</th><td><pre>{html.escape(value)}</pre></td></tr>"
        )
    return "".join(rows) or '<tr><td colspan="2">No references declared</td></tr>'


def _esc_attr(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _fa_solid_font_path() -> Path | None:
    """Locate the bundled Font Awesome Free solid-style webfont.

    ``fontawesomefree`` ships the real Font Awesome Free static assets (TTF webfonts plus a
    name-to-unicode ``metadata/icons.json``) as installable package data, so evidence-package
    icons are drawn from real glyphs in a real font rather than approximated with shapes.
    """
    try:
        import fontawesomefree
    except ImportError:
        return None
    path = (
        Path(fontawesomefree.__file__).parent
        / "static"
        / "fontawesomefree"
        / "webfonts"
        / "fa-solid-900.ttf"
    )
    return path if path.is_file() else None


# Font Awesome Free 6 solid glyphs (codepoints from fontawesomefree's own
# metadata/icons.json), used for evidence-package icons throughout the PDF.
ICON_GLYPH = {
    "aoi_polygon": chr(0xF5EE),  # draw-polygon
    "forest": chr(0xF1BB),  # tree
    "fire": chr(0xF06D),  # fire
    "leaf": chr(0xF06C),  # leaf
    "layers": chr(0xF5FD),  # layer-group
    "file": chr(0xF15C),  # file-lines
    "location": chr(0xF3C5),  # location-dot
    "crosshairs": chr(0xF05B),  # crosshairs
    "vector_square": chr(0xF5CB),  # vector-square
    "search": chr(0xF002),  # magnifying-glass
    "ban": chr(0xF05E),  # ban
    "clipboard_check": chr(0xF46C),  # clipboard-check
    "scale": chr(0xF24E),  # scale-balanced
    "satellite": chr(0xF7BF),  # satellite
    "shield": chr(0xF3ED),  # shield-halved
    "warning_triangle": chr(0xF071),  # triangle-exclamation
}


def render_canonical_pdf(report: CanonicalReport, output_path: Path, *, report_root: Path) -> None:
    import reportlab
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.utils import ImageReader
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.pdfmetrics import stringWidth
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.pdfgen import canvas

    payload = report.to_dict()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(output_path), pagesize=A4, pageCompression=0, invariant=1)

    icon_font = "FAIcons"
    _fa_path = _fa_solid_font_path()
    if _fa_path is not None:
        try:
            pdfmetrics.registerFont(TTFont(icon_font, str(_fa_path)))
        except Exception:
            icon_font = None
    else:
        icon_font = None
    c.setTitle(f"EUDR Evidence Package - {payload['report_id']}")
    c.setAuthor("Single.Earth")
    c.setSubject("Deterministic EUDR evidence package")
    c.setCreator(f"eudr-dmi-gil ReportLab {reportlab.Version}")
    c.setKeywords("EUDR evidence package, deterministic report, non-decision")
    width, height = A4

    ink = colors.HexColor("#0b0d0c")
    muted = colors.HexColor("#5f665f")
    line = colors.HexColor("#dfe5dc")
    soft = colors.HexColor("#f7f9f5")
    accent = colors.HexColor("#c7ff16")
    warning = colors.HexColor("#ff8a00")
    forest = colors.HexColor("#17462f")
    orange_soft = colors.HexColor("#fff6df")
    aoi_boundary = colors.HexColor("#ffcc00")

    margin = 32.0
    content_top = height - 72.0
    content_w = width - (2 * margin)

    metrics = payload["metrics"]
    aoi = payload["aoi"]
    commodity = payload["commodity"]
    temporal = payload["temporal_scope"]
    layers = payload["layers"]
    assessment = payload["assessment"]
    artifacts = payload.get("artifacts") or {}
    evidence_period = f"{temporal['evidence_start_year']}-{temporal['effective_end_year']}"
    commodity_name = _display_value(commodity.get("display_name") or commodity.get("id"))
    country = _display_value(aoi.get("country"))
    state = _display_value(aoi.get("state"), "")
    municipality = _display_value(aoi.get("municipality"), "")
    locality = ", ".join(part for part in [municipality, state] if part)
    context = f"{commodity_name.upper()}  ·  {country.upper()}"
    location_context = f"{context}  ·  {locality.upper()}" if locality else context
    needs_review = bool(assessment.get("human_review_required"))
    review_state = "Needs review" if needs_review else "No review trigger"
    loss_positive = needs_review

    def set_alpha(value: float) -> None:
        try:
            c.setFillAlpha(value)
        except Exception:
            pass

    def reset_alpha() -> None:
        set_alpha(1)

    def draw_logo(x: float, y: float, *, light: bool = False, scale: float = 1.0) -> None:
        c.saveState()
        c.setStrokeColor(accent)
        c.setLineWidth(1.8 * scale)
        c.circle(x + (7 * scale), y - (7 * scale), 6 * scale, stroke=1, fill=0)
        c.setFillColor(accent)
        c.circle(x + (14 * scale), y - (2 * scale), 2.2 * scale, stroke=0, fill=1)
        c.setFont("Helvetica-Bold", 9 * scale)
        c.setFillColor(colors.white if light else ink)
        c.drawString(x + (20 * scale), y - (10 * scale), "Single.Earth")
        c.restoreState()

    def draw_icon(x: float, y: float, glyph: str, *, size: float = 11, color: Any = ink) -> bool:
        """Draw a Font Awesome glyph centred at (x, y). Returns False (no-op) if the icon font
        could not be registered, so callers can fall back to a plain layout instead of leaving a
        blank gap."""
        if not icon_font or not glyph:
            return False
        c.setFillColor(color)
        c.setFont(icon_font, size)
        c.drawCentredString(x, y, glyph)
        return True

    def draw_icon_badge(
        cx: float, cy: float, radius: float, glyph: str, *, fill: Any = forest, icon_color: Any = colors.white
    ) -> None:
        c.setFillColor(fill)
        c.circle(cx, cy, radius, stroke=0, fill=1)
        if not draw_icon(cx, cy - (radius * 0.34), glyph, size=radius * 1.15, color=icon_color):
            pass

    def draw_swatch_legend(
        x: float, y: float, items: list[tuple[Any, str]], *, swatch: float = 10, gap: float = 16, size: float = 7.6
    ) -> float:
        """Draw a horizontal row of colour-swatch + label legend entries starting at (x, y-top)."""
        cx = x
        for color, label in items:
            c.setFillColor(color)
            c.setStrokeColor(line)
            c.roundRect(cx, y - swatch, swatch, swatch, 2, stroke=1, fill=1)
            c.setFillColor(ink)
            c.setFont("Helvetica", size)
            label_w = stringWidth(label, "Helvetica", size)
            c.drawString(cx + swatch + 5, y - swatch + 1.5, label)
            cx += swatch + 5 + label_w + gap
        return cx

    def draw_legend_card(x: float, y: float, w: float, items: list[tuple[Any, str]]) -> float:
        """Small floating white card of stacked colour-swatch legend rows (map-overlay style)."""
        row_h = 22.0
        h = 14 + row_h * len(items)
        c.setFillColor(colors.white)
        c.setStrokeColor(line)
        c.roundRect(x, y - h, w, h, 6, stroke=1, fill=1)
        ry = y - 16
        for color, label in items:
            c.setFillColor(color)
            c.setStrokeColor(line)
            c.roundRect(x + 12, ry - 9, 12, 12, 2, stroke=1, fill=1)
            draw_wrapped(label, x + 30, ry - 8, w - 42, size=8, leading=10, max_lines=1)
            ry -= row_h
        return y - h

    def evidence_map_bottom(image_y: float) -> float:
        """The bottom y-coordinate of the fixed 520pt-tall page 5/6 evidence-map image box,
        shared by every floating card anchored over that image (round 19: round 18 only
        bottom-anchored the swatch legend, leaving the metric card floating at a fixed offset
        from the top instead of the same baseline)."""
        return image_y - 18 - 520

    def evidence_map_card_y(image_y: float, card_h: float, *, inset: float = 16.0) -> float:
        """Top y-coordinate for a ``card_h``-tall floating card so it sits ``inset`` px above
        the evidence-map image's own bottom edge, exactly like ``draw_evidence_map_legend``
        anchors the swatch-legend card - used so the metric card on pages 5/6 sits on the same
        bottom baseline as the legend instead of floating mid-image."""
        return evidence_map_bottom(image_y) + inset + card_h

    def draw_evidence_map_legend(image_y: float, items: list[tuple[Any, str]]) -> None:
        """Anchor a page 5/6 evidence-map legend card to the bottom-right corner of the fixed
        520pt-tall image box, like a real map legend inset, instead of floating partway up the
        image (round 18: user-flagged that the legend needed to sit at the bottom of the image,
        not mid-image). Derives its own height from ``items`` so it still works whichever page
        adds a commodity-layer legend row and which does not. Round 20: page 6's
        coffee-plantation-loss headline number used to float here as its own card above this
        legend; it is now fused into the bottom-left metric card instead (see
        ``draw_dual_metric_card``), so this only ever draws the swatch-legend card."""
        img_bottom = evidence_map_bottom(image_y)
        row_h = 22.0
        h = 14 + row_h * len(items)
        legend_x = width - margin - 190
        draw_legend_card(legend_x, img_bottom + 16 + h, 174, items)

    def draw_header(page_no: int, title: str) -> None:
        c.setFillColor(ink)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(margin, height - 33, f"{page_no:02d}")
        c.drawString(margin + 36, height - 33, title.upper())
        draw_logo(width - 106, height - 24, scale=0.72)
        c.setStrokeColor(line)
        c.setLineWidth(0.8)
        c.line(margin, height - 52, width - margin, height - 52)

    def draw_footer(page_no: int) -> None:
        c.setStrokeColor(line)
        c.setLineWidth(0.6)
        c.line(margin, 38, width - margin, 38)
        c.setFillColor(muted)
        c.setFont("Helvetica", 6.4)
        c.drawString(margin, 24, f"EUDR EVIDENCE PACKAGE  ·  {location_context}")
        c.setFillColor(ink)
        c.setFont("Helvetica-Bold", 7)
        c.drawRightString(width - margin, 24, str(page_no))

    def new_page(page_no: int, title: str) -> None:
        if page_no > 1:
            c.showPage()
        draw_header(page_no, title)

    def finish_page(page_no: int) -> None:
        draw_footer(page_no)

    def wrap(text: Any, max_w: float, font: str = "Helvetica", size: float = 9) -> list[str]:
        words = str(_display_value(text, "")).replace("\n", " ").split()
        rows: list[str] = []
        current = ""
        for word in words:
            candidate = f"{current} {word}".strip()
            if stringWidth(candidate, font, size) <= max_w:
                current = candidate
                continue
            if current:
                rows.append(current)
            current = word
            while stringWidth(current, font, size) > max_w and len(current) > 1:
                piece = current
                while stringWidth(piece, font, size) > max_w and len(piece) > 1:
                    piece = piece[:-1]
                rows.append(piece)
                current = current[len(piece) :]
        if current:
            rows.append(current)
        return rows or [""]

    def draw_wrapped(
        text: Any,
        x: float,
        y: float,
        max_w: float,
        *,
        font: str = "Helvetica",
        size: float = 9,
        leading: float = 12,
        color: Any = ink,
        max_lines: int | None = None,
    ) -> float:
        c.setFillColor(color)
        c.setFont(font, size)
        rows = wrap(text, max_w, font, size)
        if max_lines is not None and len(rows) > max_lines:
            rows = rows[:max_lines]
            if rows:
                rows[-1] = rows[-1].rstrip(".") + "..."
        for row in rows:
            c.drawString(x, y, row)
            y -= leading
        return y

    def draw_label(x: float, y: float, label: str, value: Any, *, max_w: float) -> float:
        c.setFillColor(muted)
        c.setFont("Helvetica-Bold", 6.8)
        c.drawString(x, y, label.upper())
        return draw_wrapped(value, x, y - 11, max_w, font="Helvetica-Bold", size=9, leading=12)

    def draw_icon_label(
        x: float, y: float, icon_key: str, label: str, value: Any, *, max_w: float
    ) -> float:
        icon_w = 15.0
        drawn = draw_icon(x + 5, y - 3, ICON_GLYPH.get(icon_key, ""), size=9.5, color=forest)
        offset = icon_w if drawn else 0.0
        return draw_label(x + offset, y, label, value, max_w=max_w - offset)

    def draw_metric_card(
        x: float,
        y: float,
        w: float,
        h: float,
        label: str,
        value: str,
        sub: str = "",
        *,
        alert: bool = False,
    ) -> None:
        c.setFillColor(colors.white)
        c.setStrokeColor(line)
        c.roundRect(x, y - h, w, h, 4, stroke=1, fill=1)
        c.setFillColor(warning if alert else forest)
        c.circle(x + 14, y - 22, 5, stroke=0, fill=1)
        draw_wrapped(label, x + 28, y - 18, w - 42, font="Helvetica-Bold", size=6.8, leading=8, max_lines=2)
        c.setFillColor(ink)
        c.setFont("Helvetica-Bold", 17)
        draw_wrapped(value, x + 28, y - 43, w - 38, font="Helvetica-Bold", size=17, leading=18, max_lines=2)
        if sub:
            draw_wrapped(sub, x + 28, y - h + 16, w - 38, size=7, leading=8, color=muted, max_lines=2)

    def draw_dual_metric_card(
        x: float,
        y: float,
        w: float,
        h: float,
        primary: tuple[str, str, str, Any],
        secondary: tuple[str, str, str, Any],
    ) -> None:
        """One card holding two stacked metric blocks, each with its own dot colour, split by a
        thin rule. Round 20: page 6 used to float the coffee-plantation-loss headline as its own
        card at the top-right of the evidence map; fused here into the primary loss-after-2020
        card instead (per-block fonts shrunk from ``draw_metric_card``'s so both blocks' label/
        value/sub still fit their half of the card)."""
        c.setFillColor(colors.white)
        c.setStrokeColor(line)
        c.roundRect(x, y - h, w, h, 4, stroke=1, fill=1)
        row_h = h / 2
        for index, (label, value, sub, dot_color) in enumerate((primary, secondary)):
            top = y - (index * row_h)
            c.setFillColor(dot_color)
            c.circle(x + 13, top - 15, 4, stroke=0, fill=1)
            draw_wrapped(label, x + 24, top - 12, w - 36, font="Helvetica-Bold", size=6.2, leading=7, max_lines=2)
            draw_wrapped(value, x + 24, top - 33, w - 32, font="Helvetica-Bold", size=13, leading=14, max_lines=1)
            if sub:
                draw_wrapped(sub, x + 24, top - 50, w - 32, size=6.2, leading=7, color=muted, max_lines=2)
            if index == 0:
                c.setStrokeColor(line)
                c.setLineWidth(0.6)
                c.line(x + 12, top - row_h, x + w - 12, top - row_h)

    def draw_notice(x: float, y: float, w: float, h: float, text: str) -> None:
        c.setFillColor(orange_soft)
        c.setStrokeColor(colors.HexColor("#f1dfb5"))
        c.roundRect(x, y - h, w, h, 5, stroke=1, fill=1)
        c.setFillColor(warning)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(x + 14, y - 24, "!")
        draw_wrapped(text, x + 34, y - 18, w - 48, size=8.2, leading=11, color=ink, max_lines=4)

    def draw_photo_chip(x: float, y: float, text: str, *, align: str = "left") -> None:
        pad = 8.0
        chip_w = stringWidth(text, "Helvetica-Bold", 8.5) + (2 * pad)
        chip_x = x if align == "left" else x - chip_w
        c.setFillColor(colors.black)
        set_alpha(0.55)
        c.roundRect(chip_x, y - 17, chip_w, 17, 8.5, stroke=0, fill=1)
        reset_alpha()
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 8.5)
        c.drawCentredString(chip_x + (chip_w / 2), y - 12, text)

    def image_path(layer_id: str) -> Path | None:
        layer_obj = layers.get(layer_id)
        if not isinstance(layer_obj, Mapping):
            return None
        rel = layer_obj.get("path")
        if not rel:
            return None
        path = report_root / str(rel)
        return path if path.is_file() else None

    def draw_image_fit(
        path: Path | None,
        x: float,
        y: float,
        w: float,
        h: float,
        *,
        fill: bool = False,
        gap: str = "Evidence image unavailable in this bundle.",
    ) -> None:
        c.setFillColor(colors.HexColor("#f3f5f1"))
        c.setStrokeColor(line)
        c.roundRect(x, y - h, w, h, 5, stroke=1, fill=1)
        if path is None:
            draw_wrapped(gap, x + 18, y - 28, w - 36, size=9, leading=12, color=muted)
            return
        try:
            img = ImageReader(str(path))
            img_w, img_h = img.getSize()
        except Exception:
            draw_wrapped(f"Image could not be embedded: {path.name}", x + 18, y - 28, w - 36, size=9, leading=12, color=muted)
            return
        scale = max(w / img_w, h / img_h) if fill else min(w / img_w, h / img_h)
        draw_w = img_w * scale
        draw_h = img_h * scale
        if fill:
            c.saveState()
            clip = c.beginPath()
            clip.roundRect(x, y - h, w, h, 5)
            c.clipPath(clip, stroke=0, fill=0)
        c.drawImage(
            str(path),
            x + ((w - draw_w) / 2),
            y - h + ((h - draw_h) / 2),
            width=draw_w,
            height=draw_h,
            preserveAspectRatio=True,
            mask="auto",
        )
        if fill:
            c.restoreState()

    def metric(name: str) -> str:
        return _fmt_metric(metrics, name)

    def metric_value(name: str) -> Any:
        return _metric_value(metrics, name)

    def artifact_checksum(name: str) -> str:
        item = artifacts.get(name)
        if isinstance(item, Mapping) and item.get("checksum_sha256"):
            return str(item["checksum_sha256"])
        return "See manifest.sha256"

    def dataset_label(dataset: Mapping[str, Any]) -> str:
        label = dataset.get("dataset_id") or dataset.get("id") or dataset.get("title") or "dataset"
        version = dataset.get("version") or dataset.get("dataset_version")
        return f"{label} ({version})" if version else str(label)

    def draw_rows(x: float, y: float, w: float, rows: list[tuple[str, Any]], *, row_h: float = 32) -> float:
        label_w = min(150, w * 0.38)
        for label, value in rows:
            c.setFillColor(colors.white)
            c.setStrokeColor(line)
            c.rect(x, y - row_h, w, row_h, stroke=1, fill=1)
            c.setFillColor(soft)
            c.rect(x, y - row_h, label_w, row_h, stroke=0, fill=1)
            draw_wrapped(label, x + 8, y - 13, label_w - 16, font="Helvetica-Bold", size=7.4, leading=9, max_lines=2)
            draw_wrapped(value, x + label_w + 9, y - 12, w - label_w - 18, size=7.4, leading=9, max_lines=2)
            y -= row_h
        return y

    def draw_gap_panel(x: float, y: float, w: float, h: float, title: str, text: str) -> None:
        c.setFillColor(colors.HexColor("#fafafa"))
        c.setStrokeColor(line)
        c.roundRect(x, y - h, w, h, 5, stroke=1, fill=1)
        c.setFillColor(warning)
        c.setFont("Helvetica-Bold", 15)
        c.drawString(x + 16, y - 24, "!")
        draw_wrapped(title, x + 38, y - 17, w - 54, font="Helvetica-Bold", size=10, leading=12)
        draw_wrapped(text, x + 38, y - 42, w - 54, size=8.1, leading=11, color=muted, max_lines=5)

    def draw_forest_texture() -> None:
        c.setFillColor(colors.HexColor("#07110a"))
        c.rect(0, 0, width, height, stroke=0, fill=1)
        palette = ["#102f1d", "#164229", "#0b2517", "#1c5634"]
        for i in range(120):
            x = ((i * 47) % 650) - 35
            y = ((i * 83) % 900) - 25
            r = 18 + ((i * 11) % 34)
            c.setFillColor(colors.HexColor(palette[i % len(palette)]))
            set_alpha(0.42)
            c.circle(x, y, r, stroke=0, fill=1)
        reset_alpha()
        c.setStrokeColor(colors.HexColor("#31513c"))
        c.setLineWidth(16)
        c.bezier(360, height + 20, 330, 610, 410, 430, 355, 210)
        c.setLineWidth(7)
        c.setStrokeColor(colors.HexColor("#725e43"))
        c.bezier(360, height + 20, 330, 610, 410, 430, 355, 210)

    def cover() -> None:
        hero = image_path("cover_hero") or image_path("satellite")
        if hero is None:
            draw_forest_texture()
        else:
            draw_image_fit(hero, 0, height, width, height, fill=True)
        c.setFillColor(colors.black)
        set_alpha(0.58)
        c.rect(0, 0, width, height, stroke=0, fill=1)
        reset_alpha()
        draw_logo(38, height - 42, light=True, scale=1.05)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 40)
        c.drawString(38, height - 145, "EUDR")
        c.drawString(38, height - 190, "Evidence")
        c.drawString(38, height - 235, "Package")
        c.setFillColor(accent)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(38, height - 275, context)
        if locality:
            c.setFillColor(colors.white)
            c.setFont("Helvetica-Bold", 10)
            c.drawString(38, height - 294, locality.upper())
        box_h = 52
        c.setStrokeColor(accent if not loss_positive else warning)
        c.setFillColor(colors.black)
        set_alpha(0.36)
        c.roundRect(38, height - 355, 270, box_h, 5, stroke=1, fill=1)
        reset_alpha()
        c.setFillColor(warning if loss_positive else accent)
        c.setFont("Helvetica-Bold", 17)
        c.drawString(54, height - 333, "!")
        draw_wrapped(assessment.get("summary"), 82, height - 324, 205, font="Helvetica-Bold", size=7.8, leading=10, color=colors.white, max_lines=3)
        c.setFont("Helvetica-Bold", 6.8)
        c.setFillColor(colors.HexColor("#cbd6ce"))
        c.drawString(38, 105, "ASSESSMENT COMPLETE")
        c.drawString(width - 215, 105, "REPORT ID")
        draw_wrapped(payload["generated_utc"][:10], 38, 92, 180, font="Helvetica-Bold", size=8, leading=10, color=colors.white, max_lines=2)
        draw_wrapped(payload["report_id"], width - 215, 92, 175, font="Helvetica-Bold", size=8, leading=10, color=colors.white, max_lines=3)

    cover()

    new_page(2, "Executive Summary")
    y = content_top
    y = draw_wrapped(
        "This assessment evaluates the supplied area of interest for review against configured satellite-derived datasets. It is an evidence package, not a compliance determination.",
        margin,
        y,
        content_w,
        size=9,
        leading=12,
    )
    y -= 18
    card_w = (content_w - 10) / 2
    for idx, (label, value, sub, alert) in enumerate(
        [
            ("Commodity", commodity_name, "", False),
            ("Country", country, "", False),
            ("AOI area", metric("aoi_area_ha"), "AOI total", False),
            ("JRC 2020 baseline area", metric("forest_baseline_2020_ha"), metric("forest_baseline_2020_percent_of_aoi") + " of AOI", False),
            ("Post-2020 loss area", metric("forest_loss_post_2020_on_baseline_ha"), metric("forest_loss_post_2020_percent_of_aoi") + " of AOI", loss_positive),
            ("Review status", review_state, "", loss_positive),
        ]
    ):
        x = margin + (idx % 2) * (card_w + 10)
        row_y = y - (idx // 2) * 92
        draw_metric_card(x, row_y, card_w, 78, label, value, sub, alert=alert)
    y -= 294
    notice_text = (
        f"{assessment.get('summary')} This report supports review and does not certify EUDR "
        "compliance or non-compliance."
    )
    draw_notice(margin, y, content_w, 64, notice_text)
    finish_page(2)

    new_page(3, "Assessment Workflow")
    y = content_top
    y = draw_wrapped("The analysis follows a transparent deterministic workflow from canonical inputs to generated artifacts.", margin, y, content_w, size=9, leading=12)
    y -= 20
    steps = [
        ("AOI Polygon", "Input area of interest geometry."),
        ("JRC 2020 Baseline", "Forest baseline evidence at the 31 Dec 2020 cutoff."),
        ("Hansen Post-2020 Loss", f"Tree-cover loss evidence for {evidence_period}."),
        ("Optional Commodity Layer", "Configured commodity reference layer when available."),
        ("Intersection", "Spatial overlay used for review prioritization."),
        ("Evidence Package", "Report JSON, HTML, PDF, metrics, evidence images, and checksums."),
    ]
    step_icons = ["aoi_polygon", "forest", "fire", "leaf", "layers", "file"]
    step_h = 64
    for idx, (title, detail) in enumerate(steps, start=1):
        x = margin + 42
        c.setFillColor(colors.white)
        c.setStrokeColor(line)
        c.roundRect(x, y - step_h, content_w - 84, step_h, 7, stroke=1, fill=1)
        c.setFillColor(ink)
        c.circle(x + 24, y - 28, 10, stroke=0, fill=1)
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(x + 24, y - 31, str(idx))
        icon_glyph = ICON_GLYPH.get(step_icons[idx - 1], "")
        draw_icon_badge(x + 54, y - 28, 12, icon_glyph, fill=forest, icon_color=accent)
        text_x = x + 86
        draw_wrapped(title, text_x, y - 22, content_w - 196, font="Helvetica-Bold", size=10, leading=12)
        draw_wrapped(detail, text_x, y - 39, content_w - 196, size=8, leading=10, color=muted, max_lines=2)
        if idx < len(steps):
            c.setStrokeColor(ink)
            c.line(width / 2, y - step_h - 5, width / 2, y - step_h - 18)
        y -= step_h + 18
    finish_page(3)

    new_page(4, "Regional Overview")
    regional = image_path("regional_overview")
    map_w = content_w * 0.68
    map_h = 500
    map_y = content_top - 28
    draw_image_fit(
        regional,
        margin,
        map_y,
        map_w,
        map_h,
        fill=True,
        gap="Regional or country map is not available in the local evidence bundle.",
    )
    if regional is not None:
        draw_swatch_legend(
            margin,
            map_y - map_h - 8,
            [
                (colors.HexColor("#3c423c"), "Regional/state boundary"),
                (aoi_boundary, "AOI boundary"),
            ],
        )
    side_x = margin + map_w + 18
    side_w = width - margin - side_x
    y = content_top - 20
    y = draw_icon_label(side_x, y, "location", "Location", country, max_w=side_w)
    y -= 18
    y = draw_icon_label(side_x, y, "location", "State", state or None, max_w=side_w)
    y -= 18
    y = draw_icon_label(side_x, y, "location", "Municipality", municipality or None, max_w=side_w)
    y -= 18
    centroid = aoi.get("centroid")
    centroid_text = (
        f"{centroid['lat']:.3f}, {centroid['lon']:.3f}" if isinstance(centroid, Mapping) else None
    )
    y = draw_icon_label(side_x, y, "crosshairs", "Centroid", centroid_text, max_w=side_w)
    y -= 18
    y = draw_icon_label(side_x, y, "vector_square", "AOI Area", metric("aoi_area_ha"), max_w=side_w)
    y -= 18
    draw_gap_panel(
        side_x,
        y,
        side_w,
        138,
        "Commodity-region rationale",
        "No cited commodity-region rationale is configured in the canonical references, so no rationale has been invented.",
    )
    finish_page(4)

    basemap_composited = image_path("satellite") is not None
    non_forest_label = "satellite basemap" if basemap_composited else "light background"

    # Round 18: whether this AOI has a configured, evidence-available commodity (e.g. coffee)
    # layer - gates the coffee-plantation overlay/legend rows/metric card added to pages 5/6 below,
    # so AOIs without a commodity layer (e.g. the mandatory zero-config regression fixture) render
    # exactly as before.
    commodity_layer_entry = layers.get("commodity")
    baseline_commodity_layer_entry = layers.get("baseline_commodity")
    commodity_overlay_available = bool(
        commodity.get("evidence_available")
        and isinstance(commodity_layer_entry, Mapping)
        and commodity_layer_entry.get("available")
    )
    baseline_commodity_overlay_available = bool(
        commodity.get("evidence_available")
        and isinstance(baseline_commodity_layer_entry, Mapping)
        and baseline_commodity_layer_entry.get("available")
    )
    commodity_loss_metric_available = (
        commodity_overlay_available
        and metric_value("post_2020_loss_and_commodity_overlap_ha") is not None
    )
    commodity_swatch = colors.HexColor("#1e88e5")
    commodity_loss_swatch = colors.HexColor("#662d91")
    commodity_label = f"{commodity_name} plantations ({_display_value(commodity.get('observation_year'))})"
    baseline_commodity_label = (
        f"{commodity_name} plantations "
        f"({_display_value(commodity.get('baseline_observation_year'))})"
    )

    new_page(5, "Forest Baseline 2020")
    y = content_top
    y = draw_wrapped("Forest extent mapped by JRC Global Forest Cover 2020 inside the AOI.", margin, y, content_w, size=9, leading=12)
    # fill=False (contain): the evidence PNG is now rendered at this exact box aspect ratio
    # (EVIDENCE_MAP_PIXEL_WIDTH/HEIGHT, see _write_mask_over_basemap_png), so contain-fit already
    # covers the box edge-to-edge with no gray band - unlike fill=True's cover-crop, it can never
    # cut into the AOI/mask polygons _reproject_raster_to_grid buffers into frame. When a commodity
    # layer is available, this image also carries a translucent commodity-mask overlay (see
    # materialize_evidence_pngs) showing where it sits relative to the 2020 forest baseline.
    draw_image_fit(image_path("jrc_forest_2020"), margin, y - 18, content_w, 520, gap="JRC 2020 baseline image is unavailable.")
    draw_metric_card(margin + 16, evidence_map_card_y(y, 92), 185, 92, "Forest baseline 2020", metric("forest_baseline_2020_ha"), metric("forest_baseline_2020_percent_of_aoi") + " of AOI")
    page5_legend_items = [
        (colors.HexColor("#217a48"), "Forest (2020)"),
        (soft, f"Non-forest (2020) / {non_forest_label}"),
    ]
    if baseline_commodity_overlay_available:
        page5_legend_items.append((commodity_swatch, baseline_commodity_label))
    draw_evidence_map_legend(y, page5_legend_items)
    finish_page(5)

    new_page(6, "Forest Loss After 2020")
    y = content_top
    loss_image = image_path("forest_loss")
    satellite_image = image_path("satellite_evidence_map")
    has_cross_observer_layers = any(
        image_path(layer_id) is not None
        for layer_id in ("tmf_deforestation", "tmf_degradation", "radd_alerts", "radd_confirmed", "radd_low_confidence")
    )
    forest_loss_layer = layers.get("forest_loss")
    forest_loss_status = (
        forest_loss_layer.get("availability_status") if isinstance(forest_loss_layer, Mapping) else None
    )
    forest_context_shown = bool(
        loss_image is not None
        and isinstance(layers.get("jrc_forest_2020"), Mapping)
        and layers["jrc_forest_2020"].get("available")
    )
    commodity_suffix = f" and {commodity_name.lower()} plantation" if commodity_overlay_available else ""

    if has_cross_observer_layers:
        intro_text = (
            "Post-2020 change evidence is shown as a cross-observer comparison. Hansen/JRC, "
            "TMF deforestation, TMF degradation and RADD alerts have different source "
            "definitions, sensors, spatial supports and temporal semantics; differing areas are "
            "reported as method differences, not as automatic source error."
        )
        y = draw_wrapped(intro_text, margin, y, content_w, size=8.6, leading=11)
        panel_gap = 12
        panel_w = (content_w - panel_gap) / 2
        panel_h = 214
        top = y - 15
        panel_specs = [
            (
                "Hansen/JRC loss",
                loss_image or satellite_image,
                metric("forest_loss_post_2020_on_baseline_ha"),
                "loss",
                "Loss on JRC 2020 forest",
            ),
            (
                "TMF deforestation",
                image_path("tmf_deforestation"),
                metric(f"tmf_deforestation_{temporal['evidence_start_year']}_{temporal['effective_end_year']}_ha"),
                "tmf-deforestation",
                "JRC TMF DeforestationYear",
            ),
            (
                "TMF degradation",
                image_path("tmf_degradation"),
                metric(f"tmf_degradation_{temporal['evidence_start_year']}_{temporal['effective_end_year']}_ha"),
                "tmf-degradation",
                "JRC TMF DegradationYear",
            ),
            (
                "RADD alerts",
                image_path("radd_alerts") or image_path("radd_confirmed") or image_path("radd_low_confidence"),
                f"Confirmed {metric('radd_confirmed_alert_area_ha')} / low {metric('radd_low_confidence_alert_area_ha')}",
                "radd",
                "Sentinel-1 alert confidence classes",
            ),
        ]
        swatch_by_kind = {
            "loss": warning,
            "tmf-deforestation": colors.HexColor("#d32f2f"),
            "tmf-degradation": colors.HexColor("#f57c00"),
            "radd": colors.HexColor("#8e24aa"),
        }
        for index, (title, path, value, kind, subtitle) in enumerate(panel_specs):
            col = index % 2
            row = index // 2
            x = margin + col * (panel_w + panel_gap)
            p_top = top - row * (panel_h + 56)
            draw_image_fit(path, x, p_top, panel_w, panel_h, gap=f"{title} map is unavailable.")
            draw_photo_chip(x + 9, p_top - 8, title.upper(), align="left")
            c.setFillColor(swatch_by_kind[kind])
            c.roundRect(x, p_top - panel_h - 20, 12, 12, 2, stroke=0, fill=1)
            draw_wrapped(str(value), x + 18, p_top - panel_h - 10, panel_w - 20, font="Helvetica-Bold", size=8.2, leading=9, max_lines=1)
            draw_wrapped(subtitle, x + 18, p_top - panel_h - 24, panel_w - 20, size=6.8, leading=8, color=muted, max_lines=1)
        draw_swatch_legend(
            margin,
            54,
            [
                (colors.HexColor("#c62828"), "Hansen loss"),
                (colors.HexColor("#d32f2f"), "TMF deforestation"),
                (colors.HexColor("#f57c00"), "TMF degradation"),
                (colors.HexColor("#8e24aa"), "RADD confirmed"),
                (colors.HexColor("#fbc02d"), "RADD low-confidence"),
                (aoi_boundary, "AOI boundary"),
            ],
        )
    else:
        page6_image = loss_image or satellite_image
        using_satellite_fallback = loss_image is None and satellite_image is not None
        if loss_image is not None and loss_positive:
            intro_text = (
                f"Tree-cover loss evidence detected by Hansen Global Forest Change during "
                f"{evidence_period}, shown together with current forest{commodity_suffix} extent for "
                f"context."
            )
        elif loss_image is not None:
            intro_text = (
                f"No post-2020 tree-cover loss was detected by Hansen Global Forest Change during "
                f"{evidence_period}; current forest{commodity_suffix} extent shown for AOI context."
            )
        elif using_satellite_fallback and forest_loss_status == "source_mask_contains_no_renderable_features":
            intro_text = (
                f"No post-2020 tree-cover loss was detected by Hansen Global Forest Change during "
                f"{evidence_period}; satellite basemap shown for AOI context."
            )
        elif using_satellite_fallback:
            intro_text = (
                f"Post-2020 tree-cover loss evidence from Hansen Global Forest Change is unavailable "
                f"for this AOI during {evidence_period}; satellite basemap shown for AOI context."
            )
        else:
            intro_text = f"Tree-cover loss evidence detected by Hansen Global Forest Change during {evidence_period}."
        y = draw_wrapped(intro_text, margin, y, content_w, size=9, leading=12)
        img_top = y - 18
        draw_image_fit(page6_image, margin, img_top, content_w, 520, gap="Post-2020 forest-loss image is unavailable.")
        if using_satellite_fallback:
            draw_photo_chip(margin + 10, img_top - 10, "SATELLITE BASEMAP - NO LOSS OVERLAY", align="left")
        elif loss_image is not None and not loss_positive:
            draw_photo_chip(margin + 10, img_top - 10, "NO LOSS DETECTED - FOREST/COMMODITY CONTEXT SHOWN", align="left")
        if using_satellite_fallback:
            page6_legend_items = [(aoi_boundary, "AOI boundary")]
        else:
            page6_legend_items = []
            if loss_positive:
                page6_legend_items.append((colors.HexColor("#c62828"), f"Loss ({evidence_period})"))
            if forest_context_shown:
                page6_legend_items.append(
                    (colors.HexColor("#217a48"), "Forest (JRC 2020 baseline)")
                )
            if commodity_overlay_available:
                page6_legend_items.append((commodity_swatch, commodity_label))
        primary_metric = (
            "Forest loss after 2020",
            metric("forest_loss_post_2020_on_baseline_ha"),
            f"{metric('forest_loss_post_2020_percent_of_aoi')} of AOI; {metric('forest_loss_post_2020_percent_of_baseline')} of baseline",
            warning if loss_positive else forest,
        )
        if commodity_loss_metric_available:
            if loss_positive:
                page6_legend_items.append(
                    (commodity_loss_swatch, f"Loss within {commodity_name.lower()} plantations")
                )
            secondary_metric = (
                f"Forest loss at {commodity_name.lower()} plantations ({evidence_period})",
                metric("post_2020_loss_and_commodity_overlap_ha"),
                f"{metric('post_2020_loss_and_commodity_percent_of_loss')} of total loss; {metric('post_2020_loss_and_commodity_percent_of_aoi')} of AOI",
                commodity_loss_swatch,
            )
            draw_dual_metric_card(margin + 16, evidence_map_card_y(y, 150), 185, 150, primary_metric, secondary_metric)
        else:
            draw_metric_card(margin + 16, evidence_map_card_y(y, 92), 185, 92, *primary_metric[:3], alert=loss_positive)
        draw_evidence_map_legend(y, page6_legend_items)
    finish_page(6)

    new_page(7, "Satellite Evidence")
    y = content_top
    y = draw_wrapped("Before/after imagery is used only when configured source imagery is available in the evidence bundle.", margin, y, content_w, size=9, leading=12)
    y -= 20
    before_after = image_path("before_after")
    before_year = str(temporal["cutoff_date"])[:4]
    after_year = str(temporal["effective_end_year"])
    if before_after is None:
        draw_gap_panel(
            margin,
            y,
            content_w,
            220,
            "Before/after evidence unavailable",
            "No suitable before/after imagery is available in this bundle. No visual comparison has been fabricated.",
        )
        y2 = 90
    else:
        # `before_after` is rendered by `_write_before_after_png` at the pixel size that targets
        # this exact box's aspect ratio (see BEFORE_AFTER_BOX_HEIGHT). That target is only
        # reachable when the AOI's own bounding-box shape happens to be as tall as the box; for
        # AOIs closer to square (like this bundle's), both source rasters' real coverage clamps
        # the shared crop back toward the AOI's own natural, undistorted aspect (never a
        # distorted or fabricated one, per round 16's rule), which can be well short of the
        # BEFORE_AFTER_BOX_HEIGHT ceiling. The box here is always sized to the image's own actual
        # aspect ratio rather than the ceiling, so content_w is filled edge-to-edge with no gray
        # letterbox and the ceiling is only ever a cap, never a stretch target.
        hero_img = ImageReader(str(before_after))
        hero_w, hero_h = hero_img.getSize()
        box_scale = min(content_w / hero_w, BEFORE_AFTER_BOX_HEIGHT / hero_h)
        box_w, box_h = hero_w * box_scale, hero_h * box_scale
        box_x = margin + (content_w - box_w) / 2
        draw_image_fit(before_after, box_x, y, box_w, box_h)
        draw_photo_chip(box_x + 10, y - 10, f"BEFORE - {before_year}", align="left")
        draw_photo_chip(box_x + box_w - 10, y - 10, f"AFTER - {after_year}", align="right")
        legend_y = y - box_h - 10
        draw_swatch_legend(margin, legend_y, [(aoi_boundary, "AOI boundary")])
        # A short (near-square AOI) image leaves real leftover vertical space below the legend
        # that no amount of image sizing can honestly reclaim (see above) - rather than stranding
        # the dates/providers rows at their old fixed distance from the footer and leaving that
        # leftover space as a disconnected gap in the middle of the page, pull the rows up to sit
        # just below the legend whenever there is slack, so the page's real content reads as one
        # contiguous block and any unavoidable leftover space collects as a single trailing margin
        # above the footer instead. Never pushed lower than the original 90pt floor.
        y2 = max(90, legend_y - 24)
    before_after_layer = layers.get("before_after")
    before_after_dates = (
        before_after_layer.get("date") if isinstance(before_after_layer, Mapping) else None
    ) or evidence_period
    draw_rows(margin, y2, content_w, [("Dates", before_after_dates), ("Providers", layers.get("before_after", {}).get("dataset", "source_imagery") if isinstance(layers.get("before_after"), Mapping) else "source_imagery")], row_h=28)
    finish_page(7)

    new_page(8, "Interpretation")
    y = content_top
    tmf_deforestation_metric = f"tmf_deforestation_{temporal['evidence_start_year']}_{temporal['effective_end_year']}_ha"
    tmf_degradation_metric = f"tmf_degradation_{temporal['evidence_start_year']}_{temporal['effective_end_year']}_ha"
    radd_alert_text = (
        f"Confirmed/high-confidence: {metric('radd_confirmed_alert_area_ha')}; "
        f"low-confidence: {metric('radd_low_confidence_alert_area_ha')}."
    )
    interp = (
        "Evidence suggests potential post-2020 forest disturbance within the AOI."
        if loss_positive
        else "Configured evidence did not detect post-2020 forest loss on the JRC 2020 baseline within the AOI."
    )
    draw_wrapped(interp, margin, y, content_w * 0.52, font="Helvetica", size=18, leading=23)
    y -= 110
    bullets = [
        ("Hansen/JRC disturbance", metric("forest_loss_post_2020_on_baseline_ha"), "search"),
        ("TMF deforestation", metric(tmf_deforestation_metric), "fire"),
        ("TMF degradation", metric(tmf_degradation_metric), "layers"),
        ("RADD alerts", radd_alert_text, "satellite"),
        (
            "Source-linkage gap",
            "The AOI is concession context; harvesting block, tree/log origin, shipment linkage and chain-of-custody evidence remain separate gaps.",
            "ban",
        ),
        ("Legal boundary", "No compliance determination is made by this report.", "scale"),
    ]
    for label, text, icon_key in bullets:
        badge_fill = warning if label in {"Detected evidence", "Review state"} and loss_positive else accent
        draw_icon_badge(margin + 8, y - 6, 7, ICON_GLYPH.get(icon_key, ""), fill=badge_fill, icon_color=ink)
        draw_wrapped(label, margin + 26, y, content_w * 0.52, font="Helvetica-Bold", size=9, leading=11)
        y = draw_wrapped(text, margin + 26, y - 13, content_w * 0.52, size=8.2, leading=11, color=muted, max_lines=3)
        y -= 12
    side_x = margin + content_w * 0.62
    side_w = width - margin - side_x
    c.setFillColor(forest)
    c.roundRect(side_x, content_top - 20 - 360, side_w, 360, 5, stroke=0, fill=1)
    draw_icon(side_x + side_w / 2, content_top - 110, ICON_GLYPH["shield"], size=34, color=accent)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 15)
    c.drawCentredString(side_x + side_w / 2, content_top - 155, "EUDR Compliance Status")
    c.setFillColor(warning if loss_positive else accent)
    c.roundRect(side_x + 44, content_top - 203, side_w - 88, 30, 15, stroke=0, fill=1)
    c.setFillColor(ink)
    c.setFont("Helvetica-Bold", 8)
    c.drawCentredString(side_x + side_w / 2, content_top - 193, review_state.upper())
    draw_wrapped("Not sufficient information to confirm compliance.", side_x + 28, content_top - 250, side_w - 56, size=9, leading=12, color=colors.white)
    finish_page(8)

    new_page(9, "Data And Methods")
    y = content_top
    y = draw_wrapped("Analysis conducted using configured datasets and reproducible processing from the canonical report model.", margin, y, content_w, size=9, leading=12)
    y -= 18
    dataset_rows = [
        ("Forest Baseline", layers.get("jrc_forest_2020", {}).get("dataset", "Unavailable") if isinstance(layers.get("jrc_forest_2020"), Mapping) else "Unavailable"),
        ("Forest Loss", layers.get("forest_loss", {}).get("dataset", "Unavailable") if isinstance(layers.get("forest_loss"), Mapping) else "Unavailable"),
        ("Commodity Layer", layers.get("commodity", {}).get("dataset", "Unavailable") if isinstance(layers.get("commodity"), Mapping) else "Unavailable"),
        ("Imagery", layers.get("satellite", {}).get("dataset", "Unavailable") if isinstance(layers.get("satellite"), Mapping) else "Unavailable"),
        ("Processing", _processing_summary(payload)),
        ("Spatial Resolution", _spatial_resolution(payload)),
        ("Temporal Scope", f"Cutoff {temporal['cutoff_date']}; evidence {evidence_period}"),
        ("Uncertainty And Coverage", _gap_summary(payload.get("evidence_gaps") or [])),
    ]
    draw_rows(margin, y, content_w, dataset_rows, row_h=42)
    if payload.get("datasets"):
        y2 = 170
        draw_wrapped("Configured dataset records", margin, y2, content_w, font="Helvetica-Bold", size=9, leading=12)
        rows = [(f"Dataset {idx}", dataset_label(ds)) for idx, ds in enumerate(payload["datasets"][:4], start=1) if isinstance(ds, Mapping)]
        if rows:
            draw_rows(margin, y2 - 18, content_w, rows, row_h=27)
    finish_page(9)

    new_page(10, "Audit Trail")
    y = content_top
    rows = [
        ("Report ID", payload["report_id"]),
        ("Run ID", payload["run_id"]),
        ("Generated UTC", payload["generated_utc"]),
        ("Cutoff Date", temporal["cutoff_date"]),
        ("Effective End Year", temporal["effective_end_year"]),
        ("Coordinate System", aoi.get("crs")),
        ("AOI Area", metric("aoi_area_ha")),
        ("Evidence Hash", artifact_checksum("manifest.sha256")),
        ("Manifest", "manifest.sha256"),
        ("Software Version Or Commit", payload.get("audit", {}).get("software_version") or "eudr-dmi-gil 0.0.0"),
        ("PDF Generator", f"ReportLab {reportlab.Version}"),
    ]
    draw_rows(margin, y, content_w, rows, row_h=35)
    draw_notice(margin, 126, content_w, 58, "Deterministic-processing statement: inputs, parameters, outputs, and generated artifacts are versioned and cryptographically hashable for integrity checks.")
    finish_page(10)

    new_page(11, "Deterministic Artifacts")
    y = content_top
    y = draw_wrapped("All generated evidence-package artifacts are deterministic outputs or declared evidence gaps.", margin, y, content_w, size=9, leading=12)
    y -= 18
    core = [
        ("report.json", "Machine-readable canonical report"),
        ("report.html", "Interactive human-readable report"),
        ("report.pdf", "A4 report for review"),
        ("metrics.csv", "Canonical metrics table"),
        ("manifest.sha256", "Integrity manifest"),
    ]
    artifact_short_descriptions = {
        "aoi_satellite": "Satellite basemap evidence (raster)",
        "jrc_forest_2020": "Forest baseline evidence map (raster)",
        "forest_loss": "Forest loss evidence map (raster)",
        "commodity_layer": "Commodity evidence map (raster)",
        "baseline_commodity_layer": "Baseline commodity evidence map (raster)",
        "intersection": "Overlap evidence map (raster)",
        "before_after": "Before/after satellite comparison (raster)",
        "legend": "Map legend (raster)",
    }
    for key in [
        "aoi_satellite",
        "jrc_forest_2020",
        "forest_loss",
        "commodity_layer",
        "baseline_commodity_layer",
        "intersection",
        "before_after",
        "legend",
    ]:
        item = artifacts.get(key)
        if isinstance(item, Mapping):
            description = (
                artifact_short_descriptions.get(key, "Evidence artifact (raster)")
                if item.get("available")
                else str(item.get("availability_status") or "")
            )
            core.append((str(item.get("path") or key), description))
    draw_rows(margin, y, content_w, core[:14], row_h=35)
    finish_page(11)

    new_page(12, "Appendix")
    y = content_top
    draw_wrapped("Reference Sources", margin, y, content_w, font="Helvetica-Bold", size=12, leading=15)
    y -= 20
    refs = payload.get("references") or []
    if refs:
        for idx, ref in enumerate(refs[:6], start=1):
            if isinstance(ref, Mapping):
                text = str(ref.get("citation") or ref.get("id") or ref)
                url = ref.get("url")
                if url:
                    text = f"{text} ({url})"
            else:
                text = str(ref)
            y = draw_wrapped(f"{idx}. {text}", margin, y, content_w, size=8.2, leading=11, max_lines=3)
            y -= 5
    else:
        y = draw_wrapped("No configured reference sources were supplied in the canonical report.", margin, y, content_w, size=8.5, leading=11, color=muted)
    y -= 18
    draw_wrapped("Limitations And Evidence Gaps", margin, y, content_w, font="Helvetica-Bold", size=12, leading=15)
    y -= 20
    gaps = payload.get("evidence_gaps") or []
    if gaps:
        for gap in gaps[:7]:
            if isinstance(gap, Mapping):
                label = gap.get("gap_id") or gap.get("artifact_id") or gap.get("code") or "gap"
                status = gap.get("status") or gap.get("reason") or gap.get("description") or gap.get("message") or ""
            else:
                label = "gap"
                status = str(gap)
            y = draw_wrapped(f"{label}: {status}", margin, y, content_w, size=8.2, leading=11, max_lines=2)
            y -= 4
    else:
        y = draw_wrapped("No evidence gaps are declared.", margin, y, content_w, size=8.5, leading=11, color=muted)
    y -= 14
    rows = [
        ("Sample/test-AOI notice", "Review source report audit metadata to identify sample or test AOIs where relevant."),
        ("Schema Version", payload["schema_version"]),
        ("Methodology Version", SCHEMA_VERSION),
    ]
    draw_rows(margin, y, content_w, rows, row_h=36)
    finish_page(12)
    c.save()


def _pdf_generator_metadata() -> dict[str, str]:
    try:
        import reportlab

        version = str(reportlab.Version)
    except Exception:
        version = "unavailable"
    return {
        "engine": "ReportLab",
        "version": version,
        "renderer": "eudr_dmi_gil.reports.report_model.render_canonical_pdf",
        "page_layout": "a4_portrait_12_page_v1",
    }


def write_sha256_manifest(bundle_dir: Path, manifest_path: Path, relpaths: list[str]) -> None:
    unique_relpaths = sorted(set(relpaths))
    rows: list[str] = []
    for relpath in unique_relpaths:
        if relpath == manifest_path.relative_to(bundle_dir).as_posix():
            continue
        path = bundle_dir / relpath
        if not path.is_file():
            raise FileNotFoundError(f"Declared artifact missing: {relpath}")
        rows.append(f"{compute_sha256(path)}  {relpath}")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def update_canonical_artifact_hashes(
    report: CanonicalReport,
    *,
    report_root: Path,
    bundle_root: Path,
    artifact_relpaths: list[str],
) -> CanonicalReport:
    payload = report.to_dict()
    artifacts = dict(payload["artifacts"])
    for relpath in sorted(set(artifact_relpaths)):
        path = bundle_root / relpath
        if path.is_file():
            local_rel = _relpath(path, report_root)
            artifacts[local_rel] = {
                "path": local_rel,
                "available": True,
                "availability_status": "available",
                "checksum_sha256": compute_sha256(path),
                "content_type": _content_type(path),
            }
    payload["artifacts"] = artifacts
    return CanonicalReport(**payload)


def _canonical_metrics(raw: Any) -> dict[str, Any]:
    raw_metrics = raw if isinstance(raw, Mapping) else {}
    out: dict[str, Any] = {}
    for name, item in sorted(raw_metrics.items(), key=lambda kv: str(kv[0])):
        if not isinstance(item, Mapping):
            continue
        out[str(name)] = {
            "value": item.get("value"),
            "unit": item.get("unit", ""),
            "description": str(item.get("notes") or ""),
            # The real per-metric dataset/process attribution computed by
            # reports.cli.MetricRow.source (e.g. "jrc_tmf", "radd",
            # "hansen_treecover2000+hansen_lossyear"). Only metrics whose analysis
            # stage never set a source (the legacy generic fallback below) still
            # need the placeholder.
            "provenance": item.get("source") or "source_report.metrics",
        }
    descriptions = {
        "aoi_area_ha": "AOI area.",
        "forest_baseline_2020_ha": "JRC 2020 forest baseline area inside AOI.",
        "forest_baseline_2020_percent_of_aoi": "JRC baseline forest share of AOI.",
        "forest_loss_post_2020_on_baseline_ha": "Post-2020 loss intersected with JRC 2020 forest baseline.",
        "forest_loss_post_2020_percent_of_aoi": "Post-2020 forest loss share of AOI.",
        "forest_loss_post_2020_percent_of_baseline": "Post-2020 forest loss share of JRC baseline forest.",
        "commodity_area_ha": "Configured commodity class area inside AOI.",
        "post_2020_loss_and_commodity_overlap_ha": "Overlap between post-2020 baseline forest loss and commodity evidence.",
    }
    for name, fallback_unit in [
        ("aoi_area_ha", "ha"),
        ("forest_baseline_2020_ha", "ha"),
        ("forest_baseline_2020_percent_of_aoi", "percent"),
        ("forest_loss_post_2020_on_baseline_ha", "ha"),
        ("forest_loss_post_2020_percent_of_aoi", "percent"),
        ("forest_loss_post_2020_percent_of_baseline", "percent"),
        ("commodity_area_ha", "ha"),
        ("post_2020_loss_and_commodity_overlap_ha", "ha"),
    ]:
        item = raw_metrics.get(name)
        if isinstance(item, Mapping):
            value = item.get("value")
            unit = item.get("unit") or fallback_unit
            notes = item.get("notes") or descriptions[name]
        else:
            value = None
            unit = fallback_unit
            notes = descriptions[name]
        out[name] = {
            "value": value,
            "unit": unit,
            "description": notes,
            "provenance": (
                "jrc_gfc2020+hansen_lossyear"
                if name not in {"commodity_area_ha", "post_2020_loss_and_commodity_overlap_ha"}
                else "commodity_provider+jrc_gfc2020+hansen_lossyear"
            ),
        }
    return out


def _interactive_overlay_availability_metrics(
    artifacts: Mapping[str, ArtifactRef],
) -> dict[str, dict[str, Any]]:
    overlay_artifacts = [
        "jrc_forest_2020",
        "forest_loss",
        "fdp_new_commodity",
        "mapbiomas_new_commodity",
        "source_specific_conversion",
        "both_source_agreement_conversion",
    ]
    rows: dict[str, dict[str, Any]] = {}
    for artifact_id in overlay_artifacts:
        artifact = artifacts.get(artifact_id)
        available = bool(artifact and artifact.available and artifact.path)
        rows[f"interactive_overlay_{artifact_id}_available"] = {
            "value": available,
            "unit": "boolean",
            "description": (
                "Whether this optional overlay was rendered into the interactive AOI map; "
                "false records a per-AOI empty/unavailable-layer omission."
            ),
            "provenance": "generated_artifacts",
            "availability_status": artifact.availability_status if artifact else "not_materialized",
        }
    return rows


def _temporal_scope(report: Mapping[str, Any]) -> dict[str, Any]:
    params = report.get("parameters", {})
    assessment = params.get("assessment_end_year", {}) if isinstance(params, Mapping) else {}
    if not isinstance(assessment, Mapping):
        assessment = {}
    return {
        "cutoff_date": "2020-12-31",
        "evidence_start_year": int(assessment.get("evidence_start_year") or 2021),
        "requested_end_year": int(assessment.get("requested_end_year") or 2025),
        "effective_end_year": int(assessment.get("effective_end_year") or assessment.get("requested_end_year") or 2025),
    }


def _aoi(
    report: Mapping[str, Any],
    metrics: Mapping[str, Any],
    *,
    bundle_root: Path,
) -> dict[str, Any]:
    geom = report.get("aoi_geometry_ref", {})
    area = metrics.get("aoi_area_ha", {})
    admin = _admin_from_report(report, geom, bundle_root=bundle_root)
    return {
        "name": str(report.get("aoi_id", "unknown")),
        "country": admin.get("country"),
        "state": admin.get("state"),
        "municipality": admin.get("municipality"),
        "centroid": _centroid_from_report_geometry(report, bundle_root=bundle_root),
        "area_ha": area.get("value") if isinstance(area, Mapping) else None,
        "polygon_count": None,
        "geometry_ref": geom.get("value") if isinstance(geom, Mapping) else None,
        "crs": "EPSG:4326" if isinstance(geom, Mapping) and geom.get("kind") == "geojson" else None,
    }


def _centroid_from_report_geometry(
    report: Mapping[str, Any], *, bundle_root: Path
) -> dict[str, float] | None:
    """Real AOI centroid (WGS84 lat/lon) computed from the actual AOI polygon, not fabricated."""
    geom = _load_report_aoi_geometry(report, bundle_root)
    if geom is None or geom.is_empty:
        return None
    centroid = geom.centroid
    return {"lat": round(centroid.y, 6), "lon": round(centroid.x, 6)}


def _admin_from_report(
    report: Mapping[str, Any],
    geom: Any,
    *,
    bundle_root: Path,
) -> dict[str, str | None]:
    values = _admin_from_aoi_geometry_ref(geom, bundle_root=bundle_root)
    explicit = report.get("aoi_admin")
    if isinstance(explicit, Mapping):
        for key in ("country", "state", "municipality"):
            value = explicit.get(key)
            if isinstance(value, str) and value.strip():
                values[key] = value.strip()
    return values


def _admin_from_aoi_geometry_ref(geom: Any, *, bundle_root: Path) -> dict[str, str | None]:
    values: dict[str, str | None] = {"country": None, "state": None, "municipality": None}
    if not isinstance(geom, Mapping) or geom.get("kind") != "geojson":
        return values
    relpath = geom.get("value")
    if not isinstance(relpath, str) or not relpath:
        return values
    path = bundle_root / relpath
    if not path.is_file():
        return values
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return values
    features = payload.get("features") if isinstance(payload, Mapping) else None
    if not isinstance(features, list):
        return values
    keys = {
        "country": [
            "country",
            "country_name",
            "country_of_production",
            "producer_country",
            "admin_country",
            "ADM0_NAME",
        ],
        "state": ["state", "region", "province", "admin_state", "ADM1_NAME", "adm1_name"],
        "municipality": [
            "municipality",
            "admin_municipality",
            "ADM2_NAME",
            "adm2_name",
            "county",
            "district",
        ],
    }
    for feature in features:
        if not isinstance(feature, Mapping):
            continue
        props = feature.get("properties")
        if not isinstance(props, Mapping):
            continue
        for admin_field, aliases in keys.items():
            if values[admin_field]:
                continue
            for key in aliases:
                value = props.get(key)
                if isinstance(value, str) and value.strip():
                    values[admin_field] = value.strip()
                    break
    return values


def _country_from_aoi_geometry_ref(geom: Any, *, bundle_root: Path) -> str | None:
    return _admin_from_aoi_geometry_ref(geom, bundle_root=bundle_root).get("country")


def _commodity(report: Mapping[str, Any], metrics: Mapping[str, Any]) -> dict[str, Any]:
    commodity = report.get("commodity", {})
    params = report.get("parameters", {})
    commodity_params = params.get("commodity", {}) if isinstance(params, Mapping) else {}
    if not isinstance(commodity, Mapping):
        commodity = {}
    if not isinstance(commodity_params, Mapping):
        commodity_params = {}
    mode = commodity.get("mode") or commodity_params.get("mode") or "discrete_classes"
    result = {
        "id": commodity.get("id") or commodity_params.get("id"),
        "display_name": commodity.get("display_name") or commodity_params.get("display_name"),
        "evidence_available": bool(commodity.get("evidence_available", False)),
        "provider": commodity.get("provider") or commodity_params.get("provider"),
        "mode": mode,
        "dataset": (
            commodity.get("dataset")
            or commodity.get("dataset_title")
            or commodity_params.get("dataset_title")
        ),
        "version": (
            commodity.get("version")
            or commodity.get("dataset_version")
            or commodity_params.get("dataset_version")
        ),
        "observation_year": commodity.get("observation_year") or commodity_params.get("observation_year"),
        "baseline_observation_year": _metric_value(
            metrics, "commodity_baseline_observation_year"
        )
        or commodity_params.get("baseline_observation_year"),
        "class_values": commodity.get("class_values") or commodity_params.get("class_values") or [],
        "coverage_status": commodity.get("coverage_status") or "missing",
        "evidence_gaps": commodity.get("evidence_gaps") or [],
        "metrics": {
            "commodity_area_ha": metrics["commodity_area_ha"]["value"],
            "post_2020_loss_and_commodity_overlap_ha": metrics[
                "post_2020_loss_and_commodity_overlap_ha"
            ]["value"],
        },
    }
    if mode == "probability_threshold":
        extensions = report.get("extensions", {})
        commodity_extension = (
            extensions.get("commodity_assessment", {}) if isinstance(extensions, Mapping) else {}
        )
        provenance = (
            commodity_extension.get("provenance", {})
            if isinstance(commodity_extension, Mapping)
            else {}
        )
        result["probability_band"] = commodity_params.get("probability_band")
        result["threshold"] = commodity_params.get("threshold")
        result["sensitivity_thresholds"] = commodity_params.get("sensitivity_thresholds") or []
        result["probability_profile"] = (
            provenance.get("probability_profile") if isinstance(provenance, Mapping) else None
        )
    return result


def _assessment(*, metrics: Mapping[str, Any], evidence_gaps: list[dict[str, Any]]) -> dict[str, Any]:
    loss = metrics["forest_loss_post_2020_on_baseline_ha"]["value"]
    overlap = metrics["post_2020_loss_and_commodity_overlap_ha"]["value"]
    human_review = bool((isinstance(loss, (int, float)) and loss > 0) or overlap not in (None, 0, 0.0))
    status = "human_review_required" if human_review else "no_post_2020_baseline_loss_detected"
    summary = (
        "Potential post-2020 forest disturbance detected; human review required."
        if human_review
        else "No post-2020 forest loss on the JRC 2020 baseline was detected by the configured evidence."
    )
    hansen_overlap_entry = metrics.get(
        "forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_ha"
    )
    hansen_overlap = hansen_overlap_entry["value"] if isinstance(hansen_overlap_entry, Mapping) else None
    if isinstance(hansen_overlap, (int, float)) and hansen_overlap > 0:
        human_review = True
        status = "human_review_required"
        summary += (
            f" Under a broader tree-canopy (>=10%) forest baseline (the FAO/EUDR Art.2 forest "
            f"definition), {hansen_overlap:,.2f} ha of post-2020 forest loss overlaps the "
            "configured commodity layer, even though the stricter JRC closed-canopy baseline "
            "shows no such overlap; both baselines are reported, not reconciled into one figure."
        )
    return {
        "status": status,
        "human_review_required": human_review,
        "summary": summary,
        "limitations": evidence_gaps,
    }


def _methods(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    methodology = report.get("methodology", {})
    if not isinstance(methodology, Mapping):
        return []
    return [
        {"id": str(key), **(dict(value) if isinstance(value, Mapping) else {"description": str(value)})}
        for key, value in sorted(methodology.items(), key=lambda kv: str(kv[0]))
    ]


def _collect_evidence_gaps(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    extensions = report.get("extensions", {})
    if isinstance(extensions, Mapping):
        for key in [
            "post_2020_loss_on_2020_forest",
            "commodity_assessment",
            "hansen_canopy_post2020_loss",
            "jrc_tmf_change",
            "radd_alerts",
        ]:
            block = extensions.get(key)
            if isinstance(block, Mapping):
                for gap in block.get("evidence_gaps") or []:
                    if isinstance(gap, Mapping):
                        gaps.append(dict(gap))
        wood_evidence_state = extensions.get("wood_evidence_state")
        if isinstance(wood_evidence_state, Mapping):
            for gap in wood_evidence_state.get("evidence_gaps") or []:
                if isinstance(gap, Mapping):
                    gaps.append(dict(gap))
    return gaps


def _artifact_evidence_gaps(artifacts: Mapping[str, ArtifactRef]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    for artifact_id, artifact in sorted(artifacts.items()):
        if artifact.available:
            continue
        gaps.append(
            {
                "gap_id": f"missing_{artifact_id}",
                "artifact_id": artifact_id,
                "status": artifact.availability_status,
                "path": None,
            }
        )
    return gaps


def _layers(
    report: Mapping[str, Any],
    artifacts: Mapping[str, ArtifactRef],
    temporal_scope: Mapping[str, Any],
) -> dict[str, LayerEntry]:
    baseline_dataset = "JRC/GFC2020/V3"
    baseline_version = "V3"
    params = report.get("parameters", {})
    post_params = params.get("post_2020_loss_on_2020_forest", {}) if isinstance(params, Mapping) else {}
    if isinstance(post_params, Mapping):
        baseline_dataset = str(post_params.get("baseline_dataset_id") or baseline_dataset)
        baseline_version = str(post_params.get("baseline_dataset_id") or baseline_version)
    commodity = _commodity(report, _canonical_metrics(report.get("metrics", {})))
    computed_outputs = report.get("computed_outputs", {})
    methodology = report.get("methodology", {})
    tmf_method = (
        methodology.get("jrc_tmf_change", {})
        if isinstance(methodology, Mapping)
        else {}
    )
    radd_method = (
        methodology.get("radd_alerts", {})
        if isinstance(methodology, Mapping)
        else {}
    )
    tmf_defo_dataset = (
        tmf_method.get("deforestation_dataset", {})
        if isinstance(tmf_method, Mapping)
        else {}
    )
    tmf_deg_dataset = (
        tmf_method.get("degradation_dataset", {})
        if isinstance(tmf_method, Mapping)
        else {}
    )
    radd_dataset = radd_method.get("dataset", {}) if isinstance(radd_method, Mapping) else {}
    satellite_outputs = (
        computed_outputs.get("satellite_imagery", {})
        if isinstance(computed_outputs, Mapping)
        else {}
    )
    satellite_dataset = str(satellite_outputs.get("dataset_title") or "satellite_context")
    satellite_version = str(satellite_outputs.get("dataset_version") or "unavailable")
    satellite_date = str(satellite_outputs.get("recent_date") or "") or None
    regional_raster_ref = _ref_relpath(satellite_outputs, "regional_raster_ref")
    recent_raster_ref = _ref_relpath(satellite_outputs, "recent_raster_ref")
    regional_uses_local_satellite = bool(regional_raster_ref or recent_raster_ref)
    regional_dataset = satellite_dataset if regional_uses_local_satellite else ESRI_WORLDIMAGERY_DATASET_TITLE
    regional_version = satellite_version if regional_uses_local_satellite else ESRI_WORLDIMAGERY_DATASET_VERSION
    regional_date = None
    if regional_raster_ref:
        regional_date = str(satellite_outputs.get("regional_date") or "") or None
    elif recent_raster_ref:
        regional_date = str(satellite_outputs.get("recent_date") or "") or None
    before_after_date = None
    baseline_date = satellite_outputs.get("baseline_date")
    recent_date = satellite_outputs.get("recent_date")
    if baseline_date or recent_date:
        before_after_date = f"{baseline_date or '?'} vs {recent_date or '?'}"
    return {
        "satellite": _layer(
            "satellite",
            "AOI satellite context",
            artifacts["aoi_satellite"],
            satellite_dataset,
            satellite_version,
            "Visual AOI context from source imagery",
            satellite_date,
        ),
        "satellite_evidence_map": _layer(
            "satellite_evidence_map",
            "AOI satellite context (evidence-map aspect)",
            artifacts.get("satellite_evidence_map") or _missing("satellite_evidence_map_not_materialized"),
            satellite_dataset,
            satellite_version,
            "Plain satellite basemap, sized to the evidence-map page box, used on pages 5/6 when no "
            "forest/loss mask is renderable",
            satellite_date,
        ),
        "cover_hero": _layer(
            "cover_hero",
            "Cover hero context",
            artifacts.get("cover_hero") or _missing("cover_hero_not_materialized"),
            ESRI_WORLDIMAGERY_DATASET_TITLE,
            ESRI_WORLDIMAGERY_DATASET_VERSION,
            "Full-bleed AOI satellite context for the report cover page",
            None,
        ),
        "regional_overview": _layer(
            "regional_overview",
            "Regional overview",
            artifacts.get("regional_overview") or _missing("regional_overview_not_materialized"),
            regional_dataset,
            regional_version,
            "Wider-context satellite view showing the AOI within its surrounding region",
            regional_date,
        ),
        "satellite_interactive_map": _layer(
            "satellite_interactive_map",
            "Evidence layers (interactive map)",
            artifacts.get("aoi_satellite_map") or _missing("aoi_satellite_map_not_materialized"),
            ESRI_WORLDIMAGERY_DATASET_TITLE,
            ESRI_WORLDIMAGERY_DATASET_VERSION,
            "Interactive Leaflet map of this AOI's boundary over an Esri World Imagery satellite "
            "mosaic, with the JRC 2020 baseline, forest loss, commodity, and intersection masks "
            "as independently toggle-able overlay layers; report.html's Image Downloads list "
            "links to this single file in place of separate per-layer PNG downloads",
            None,
        ),
        "jrc_forest_2020": _layer(
            "jrc_forest_2020",
            "JRC Global Forest Cover 2020",
            artifacts["jrc_forest_2020"],
            baseline_dataset,
            baseline_version,
            "Forest baseline evidence at the EUDR cutoff date",
            "2020-12-31",
        ),
        "forest_loss": _layer(
            "forest_loss",
            f"Forest loss 2021-{temporal_scope['effective_end_year']}",
            artifacts["forest_loss"],
            "hansen_lossyear",
            str(temporal_scope["effective_end_year"]),
            "Post-2020 loss evidence intersected with baseline forest, alongside current forest "
            "and commodity extent for AOI context",
            str(temporal_scope["effective_end_year"]),
        ),
        "tmf_deforestation": _layer(
            "tmf_deforestation",
            f"TMF deforestation 2021-{temporal_scope['effective_end_year']}",
            artifacts.get("tmf_deforestation") or _missing("tmf_deforestation_not_materialized"),
            str(tmf_defo_dataset.get("asset_identifier") or "JRC TMF DeforestationYear"),
            str(tmf_defo_dataset.get("dataset_version") or "unavailable"),
            "JRC TMF DeforestationYear evidence, kept distinct from Hansen loss and TMF degradation",
            str(temporal_scope["effective_end_year"]),
        ),
        "tmf_degradation": _layer(
            "tmf_degradation",
            f"TMF degradation 2021-{temporal_scope['effective_end_year']}",
            artifacts.get("tmf_degradation") or _missing("tmf_degradation_not_materialized"),
            str(tmf_deg_dataset.get("asset_identifier") or "JRC TMF DegradationYear"),
            str(tmf_deg_dataset.get("dataset_version") or "unavailable"),
            "JRC TMF DegradationYear evidence; degradation is not collapsed into deforestation",
            str(temporal_scope["effective_end_year"]),
        ),
        "radd_confirmed": _layer(
            "radd_confirmed",
            "RADD confirmed alerts",
            artifacts.get("radd_confirmed") or _missing("radd_confirmed_not_materialized"),
            str(radd_dataset.get("collection_id") or "projects/radar-wur/raddalert/v1"),
            str(radd_dataset.get("geography") or "unavailable"),
            "RADD Alert=3 confirmed/high-confidence Sentinel-1 disturbance alerts",
            str(radd_dataset.get("date_window_end") or "") or None,
        ),
        "radd_low_confidence": _layer(
            "radd_low_confidence",
            "RADD low-confidence alerts",
            artifacts.get("radd_low_confidence")
            or _missing("radd_low_confidence_not_materialized"),
            str(radd_dataset.get("collection_id") or "projects/radar-wur/raddalert/v1"),
            str(radd_dataset.get("geography") or "unavailable"),
            "RADD Alert=2 unconfirmed/low-confidence Sentinel-1 disturbance alerts",
            str(radd_dataset.get("date_window_end") or "") or None,
        ),
        "radd_alerts": _layer(
            "radd_alerts",
            "RADD alerts by confidence",
            artifacts.get("radd_alerts") or _missing("radd_alerts_not_materialized"),
            str(radd_dataset.get("collection_id") or "projects/radar-wur/raddalert/v1"),
            str(radd_dataset.get("geography") or "unavailable"),
            "RADD Sentinel-1 alerts with confirmed/high-confidence and low-confidence classes distinguished",
            str(radd_dataset.get("date_window_end") or "") or None,
        ),
        "commodity": _layer(
            "commodity",
            "Commodity layer",
            artifacts["commodity_layer"],
            str(commodity.get("dataset") or "commodity_layer"),
            str(commodity.get("version") or "unavailable"),
            "Configured commodity evidence layer",
            str(commodity.get("observation_year") or "") or None,
        ),
        "baseline_commodity": _layer(
            "baseline_commodity",
            "Baseline commodity layer",
            artifacts.get("baseline_commodity_layer")
            or _missing("baseline_commodity_layer_not_materialized"),
            str(commodity.get("dataset") or "commodity_layer"),
            str(commodity.get("version") or "unavailable"),
            "Configured commodity evidence layer at the baseline observation year",
            str(commodity.get("baseline_observation_year") or "") or None,
        ),
        "intersection": _layer(
            "intersection",
            "Intersection",
            artifacts["intersection"],
            "derived_intersection",
            SCHEMA_VERSION,
            "Intersection evidence used for review prioritization",
            str(temporal_scope["effective_end_year"]),
        ),
        "fdp_new_commodity": _layer(
            "fdp_new_commodity",
            "New FDP coffee after baseline",
            artifacts.get("fdp_new_commodity") or _missing("fdp_new_commodity_not_materialized"),
            "derived_fdp_commodity_change",
            SCHEMA_VERSION,
            "FDP latest commodity evidence minus baseline-year FDP commodity evidence",
            str(commodity.get("observation_year") or "") or None,
        ),
        "mapbiomas_new_commodity": _layer(
            "mapbiomas_new_commodity",
            "New MapBiomas coffee after baseline",
            artifacts.get("mapbiomas_new_commodity")
            or _missing("mapbiomas_new_commodity_not_materialized"),
            "derived_mapbiomas_commodity_change",
            SCHEMA_VERSION,
            "MapBiomas latest commodity evidence minus baseline-year MapBiomas commodity evidence",
            str(commodity.get("observation_year") or "") or None,
        ),
        "source_specific_conversion": _layer(
            "source_specific_conversion",
            "Source-specific conversion",
            artifacts.get("source_specific_conversion")
            or _missing("source_specific_conversion_not_materialized"),
            "derived_source_specific_conversion",
            SCHEMA_VERSION,
            "Post-2020 baseline forest loss intersected with FDP-only or MapBiomas-only new commodity",
            str(temporal_scope["effective_end_year"]),
        ),
        "both_source_agreement_conversion": _layer(
            "both_source_agreement_conversion",
            "Both-source agreement conversion",
            artifacts.get("both_source_agreement_conversion")
            or _missing("both_source_agreement_conversion_not_materialized"),
            "derived_both_source_agreement_conversion",
            SCHEMA_VERSION,
            "Post-2020 baseline forest loss intersected with new commodity where FDP and MapBiomas agree",
            str(temporal_scope["effective_end_year"]),
        ),
        "before_after": _layer(
            "before_after",
            "Before/after imagery",
            artifacts["before_after"],
            satellite_dataset,
            satellite_version,
            "Optional before/after visual evidence",
            before_after_date,
        ),
        "legend": _layer(
            "legend",
            "Legend",
            artifacts["legend"],
            "generated_legend",
            SCHEMA_VERSION,
            "Legend for evidence bundle layer colors",
            None,
        ),
    }


def _layer(
    id_: str,
    title: str,
    artifact: ArtifactRef,
    dataset: str,
    dataset_version: str,
    purpose: str,
    date: str | None,
) -> LayerEntry:
    return LayerEntry(
        id=id_,
        title=title,
        path=artifact.path,
        dataset=dataset,
        dataset_version=dataset_version,
        purpose=purpose,
        date=date,
        available=artifact.available,
        availability_status=artifact.availability_status,
        checksum_sha256=artifact.checksum_sha256,
    )


def _artifacts(artifacts: Mapping[str, ArtifactRef]) -> dict[str, Any]:
    return {key: asdict(value) for key, value in sorted(artifacts.items())}


def _ref_relpath(block: Any, key: str) -> str | None:
    if not isinstance(block, Mapping):
        return None
    ref = block.get(key)
    if not isinstance(ref, Mapping):
        return None
    relpath = ref.get("relpath")
    return relpath if isinstance(relpath, str) and relpath else None


def _png_from_geojson_ref(
    *,
    bundle_root: Path,
    relpath: str | None,
    output_path: Path,
    color: tuple[int, int, int, int],
    unavailable_reason: str,
    background_raster_path: Path | None = None,
    aoi_geom_wgs84: Any | None = None,
    overlay_path: Path | None = None,
    overlay_color: tuple[int, int, int, int] = _COMMODITY_OVERLAY_COLOR,
    overlay_alpha: float = _COMMODITY_OVERLAY_ALPHA,
) -> ArtifactRef:
    if not relpath:
        return _missing(unavailable_reason)
    src = bundle_root / relpath
    if not src.is_file():
        return _missing(unavailable_reason)
    if background_raster_path is not None and aoi_geom_wgs84 is not None:
        try:
            _write_mask_over_basemap_png(
                geojson_path=src,
                output_path=output_path,
                color=color,
                background_raster_path=background_raster_path,
                aoi_geom_wgs84=aoi_geom_wgs84,
                overlay_geojson_path=overlay_path,
                overlay_color=overlay_color,
                overlay_alpha=overlay_alpha,
            )
            return _available(output_path, output_path.parents[1])
        except ValueError:
            return _missing("source_mask_contains_no_renderable_features")
        except Exception:
            pass  # fall through to the flat-color rendering below
    try:
        _write_geojson_mask_png(src, output_path, color=color)
    except ValueError:
        return _missing("source_mask_contains_no_renderable_features")
    return _available(output_path, output_path.parents[1])


def _write_geojson_mask_png(path: Path, output_path: Path, *, color: tuple[int, int, int, int]) -> None:
    import numpy as np
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds
    from shapely.geometry import shape
    from shapely.ops import unary_union

    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features") if isinstance(payload, Mapping) else None
    if not isinstance(features, list) or not features:
        raise ValueError("no features")
    geoms = []
    for feature in features:
        if not isinstance(feature, Mapping) or not isinstance(feature.get("geometry"), Mapping):
            continue
        geom = shape(feature["geometry"])
        if not geom.is_empty:
            geoms.append(geom)
    if not geoms:
        raise ValueError("no geometry")
    union = unary_union(geoms)
    minx, miny, maxx, maxy = union.bounds
    if minx == maxx:
        minx -= 0.0001
        maxx += 0.0001
    if miny == maxy:
        miny -= 0.0001
        maxy += 0.0001
    width = 640
    height = 420
    transform = from_bounds(minx, miny, maxx, maxy, width, height)
    mask = rasterize(
        [(geom, 1) for geom in geoms],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="uint8",
        all_touched=True,
    )
    if int(np.count_nonzero(mask)) == 0:
        raise ValueError("empty rasterized mask")
    rgba = np.zeros((height, width, 4), dtype=np.uint8)
    rgba[:, :, :] = np.array([248, 250, 247, 255], dtype=np.uint8)
    rgba[mask.astype(bool)] = np.array(color, dtype=np.uint8)
    _write_png_rgba(output_path, rgba)


def _load_report_aoi_geometry(report: Mapping[str, Any], bundle_root: Path) -> Any | None:
    ref = report.get("aoi_geometry_ref")
    if not isinstance(ref, Mapping):
        return None
    value = ref.get("value")
    if not isinstance(value, str) or not value:
        return None
    path = bundle_root / value
    if not path.is_file():
        return None
    from shapely.geometry import shape
    from shapely.ops import unary_union

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, Mapping):
        return None
    if payload.get("type") == "FeatureCollection":
        geoms = [
            shape(f["geometry"])
            for f in payload.get("features", [])
            if isinstance(f, Mapping) and f.get("geometry")
        ]
        return unary_union(geoms) if geoms else None
    if payload.get("type") == "Feature" and payload.get("geometry"):
        return shape(payload["geometry"])
    if payload.get("type"):
        return shape(payload)
    return None


def _clamp_span(lo: float, hi: float, rb_lo: float, rb_hi: float) -> tuple[float, float]:
    """Slide/shrink the interval ``[lo, hi]`` to fit inside ``[rb_lo, rb_hi]``.

    Keeps the interval's own center and length when it already fits (just re-centering against
    the bound if it hangs over one edge); if the interval is wider than the bound itself, the
    bound's full extent is returned instead - there is nothing past it to show.
    """
    span = hi - lo
    available = rb_hi - rb_lo
    if span >= available:
        return rb_lo, rb_hi
    center = (lo + hi) / 2
    half = span / 2
    if center - half < rb_lo:
        center = rb_lo + half
    elif center + half > rb_hi:
        center = rb_hi - half
    return center - half, center + half


def _reproject_raster_to_grid(
    raster_path: Path,
    bounds_geom_wgs84: Any,
    *,
    pad_factor: float = 0.12,
    width: int = 640,
    height: int = 420,
    allow_variable_width: bool = False,
) -> tuple[Any, Any, Any]:
    """Reproject a raster crop framed around ``bounds_geom_wgs84`` onto a fixed-size RGBA grid.

    The padded frame is only ever widened, never narrowed, to match the ``width:height`` pixel
    aspect ratio (in the destination CRS's linear units) before the transform is built - so the
    output always covers the full ``width``x``height`` canvas with uniform (non-distorting)
    scaling and no gray letterbox, while every buffered pixel of ``bounds_geom_wgs84`` stays
    inside the frame. Callers can therefore draw this image with a plain contain-fit; it never
    needs a cover-crop (which would risk cropping into the buffered geometry) to fill its box.

    That widen-only frame assumes the source raster's own extent covers whatever the AOI+pad+
    aspect extension needs. A raster fetched with a tighter buffer than that (e.g. an older
    baseline scene on disk next to a much more widely fetched "recent" scene for the same AOI)
    can leave part of the extension outside the raster's real coverage; left alone, reproject()
    would leave those destination pixels at their zero-initialized value - a solid black no-data
    band, not a cropped-but-real image. When ``allow_variable_width`` is set, the frame is instead
    clamped per-axis to the raster's own bounds (see ``_clamp_span``), and the output pixel
    *height* is held fixed at ``height`` while pixel *width* is re-derived from the clamped frame
    at that same uniform per-pixel scale - so the image may come out narrower or wider than the
    nominal ``width`` instead of distorting or fabricating coverage. Callers that place two such
    crops side by side (stacked along the shared height axis) can still combine them regardless of
    the resulting width; callers that need one fixed pixel size should leave this off.

    Returns ``(rgba, dst_crs, dst_transform)`` with no overlay drawn, so callers can rasterize
    additional vector overlays (AOI outline, evidence masks) into the exact same pixel grid.
    """
    import numpy as np
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds
    from rasterio.warp import reproject, transform_bounds

    minx, miny, maxx, maxy = bounds_geom_wgs84.bounds
    pad_x = (maxx - minx) * pad_factor or 0.001
    pad_y = (maxy - miny) * pad_factor or 0.001
    minx, maxx = minx - pad_x, maxx + pad_x
    miny, maxy = miny - pad_y, maxy + pad_y

    with rasterio.open(raster_path) as ds:
        left, bottom, right, top = transform_bounds("EPSG:4326", ds.crs, minx, miny, maxx, maxy)
        target_aspect = width / height
        frame_w, frame_h = right - left, top - bottom
        frame_aspect = (frame_w / frame_h) if frame_h else target_aspect
        if frame_aspect < target_aspect:
            new_w = frame_h * target_aspect
            cx = (left + right) / 2
            left, right = cx - new_w / 2, cx + new_w / 2
        elif frame_aspect > target_aspect:
            new_h = frame_w / target_aspect
            cy = (bottom + top) / 2
            bottom, top = cy - new_h / 2, cy + new_h / 2

        out_width = width
        out_height = height
        if allow_variable_width:
            rb_left, rb_bottom, rb_right, rb_top = ds.bounds
            left, right = _clamp_span(left, right, rb_left, rb_right)
            bottom, top = _clamp_span(bottom, top, rb_bottom, rb_top)
            scale = (top - bottom) / height
            out_width = max(1, round((right - left) / scale))

        dst_transform = from_bounds(left, bottom, right, top, out_width, out_height)
        dest = np.zeros((3, out_height, out_width), dtype=np.uint8)
        reproject(
            source=rasterio.band(ds, [1, 2, 3]),
            destination=dest,
            src_transform=ds.transform,
            src_crs=ds.crs,
            dst_transform=dst_transform,
            dst_crs=ds.crs,
            resampling=Resampling.bilinear,
        )
        rgb = np.transpose(dest, (1, 2, 0))
        rgba = np.dstack([rgb, np.full((out_height, out_width), 255, dtype=np.uint8)])
        dst_crs = ds.crs
    return rgba, dst_crs, dst_transform


def _reproject_pair_to_shared_grid(
    raster_paths: tuple[Path, Path],
    bounds_geom_wgs84: Any,
    *,
    pad_factor: float = 0.12,
    width: int = 320,
    height: int = 480,
) -> tuple[list[Any], Any, Any]:
    """Reproject two rasters covering the same AOI onto one identical pixel grid.

    `_reproject_raster_to_grid`'s `allow_variable_width` clamps the aspect-widened frame to
    *each raster's own* bounds independently, so two rasters with different real coverage (e.g.
    a tightly-fetched baseline scene next to a much more widely-fetched recent scene for the
    same AOI, see round 16) come out at different pixel widths *and* different meters/pixel
    scales - not just visually unequal before/after panel sizes, but a mismatched zoom level
    between them. This instead clamps the shared aspect-widened frame to the *intersection* of
    both rasters' bounds before deriving one shared transform, then reprojects both rasters onto
    that same grid: every destination pixel is guaranteed to be real data from both sources (no
    no-data band, per round 16's rule), and both crops come out pixel-for-pixel the same size at
    the same ground scale.

    Returns ``(rgba_list, dst_crs, dst_transform)`` in ``raster_paths`` order, with no overlay
    drawn, so callers can rasterize the same AOI outline into each grid themselves.
    """
    import numpy as np
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds
    from rasterio.warp import reproject, transform_bounds

    minx, miny, maxx, maxy = bounds_geom_wgs84.bounds
    pad_x = (maxx - minx) * pad_factor or 0.001
    pad_y = (maxy - miny) * pad_factor or 0.001
    minx, maxx = minx - pad_x, maxx + pad_x
    miny, maxy = miny - pad_y, maxy + pad_y

    datasets = [rasterio.open(p) for p in raster_paths]
    try:
        dst_crs = datasets[0].crs
        left, bottom, right, top = transform_bounds("EPSG:4326", dst_crs, minx, miny, maxx, maxy)
        target_aspect = width / height
        frame_w, frame_h = right - left, top - bottom
        frame_aspect = (frame_w / frame_h) if frame_h else target_aspect
        if frame_aspect < target_aspect:
            new_w = frame_h * target_aspect
            cx = (left + right) / 2
            left, right = cx - new_w / 2, cx + new_w / 2
        elif frame_aspect > target_aspect:
            new_h = frame_w / target_aspect
            cy = (bottom + top) / 2
            bottom, top = cy - new_h / 2, cy + new_h / 2

        for ds in datasets:
            rb_left, rb_bottom, rb_right, rb_top = ds.bounds
            if ds.crs != dst_crs:
                rb_left, rb_bottom, rb_right, rb_top = transform_bounds(
                    ds.crs, dst_crs, rb_left, rb_bottom, rb_right, rb_top
                )
            left, right = _clamp_span(left, right, rb_left, rb_right)
            bottom, top = _clamp_span(bottom, top, rb_bottom, rb_top)

        scale = (top - bottom) / height
        out_width = max(1, round((right - left) / scale))
        dst_transform = from_bounds(left, bottom, right, top, out_width, height)

        rgbas = []
        for ds in datasets:
            dest = np.zeros((3, height, out_width), dtype=np.uint8)
            reproject(
                source=rasterio.band(ds, [1, 2, 3]),
                destination=dest,
                src_transform=ds.transform,
                src_crs=ds.crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                resampling=Resampling.bilinear,
            )
            rgb = np.transpose(dest, (1, 2, 0))
            rgba = np.dstack([rgb, np.full((height, out_width), 255, dtype=np.uint8)])
            rgbas.append(rgba)
    finally:
        for ds in datasets:
            ds.close()
    return rgbas, dst_crs, dst_transform


def _draw_aoi_outline_inplace(
    rgba: Any,
    aoi_geom_wgs84: Any,
    dst_crs: Any,
    dst_transform: Any,
    *,
    min_stroke_px: float = 1.2,
    dash_px: float = 9.0,
    gap_px: float = 6.0,
    color: tuple[int, int, int, int] = (255, 204, 0, 255),
) -> None:
    """Rasterize the AOI boundary as a dotted line (dash-buffer segments along the ring), not a
    solid outline - a dashed AOI marker reads as an overlay/annotation against the satellite
    imagery, where a solid line can be mistaken for a real mapped feature (e.g. a road or field
    edge)."""
    import numpy as np
    from rasterio.features import rasterize
    from rasterio.warp import transform_geom
    from shapely.geometry import mapping, shape
    from shapely.ops import substring, unary_union

    height, width = rgba.shape[0], rgba.shape[1]
    geom_target = shape(transform_geom("EPSG:4326", dst_crs, mapping(aoi_geom_wgs84)))
    pixel_size_m = abs(dst_transform.a)
    stroke_radius = max(pixel_size_m * min_stroke_px, 1e-6)
    dash_len = max(pixel_size_m * dash_px, stroke_radius)
    gap_len = max(pixel_size_m * gap_px, stroke_radius)

    boundary = geom_target.boundary
    lines = list(boundary.geoms) if boundary.geom_type == "MultiLineString" else [boundary]
    dashes = []
    for line in lines:
        length = line.length
        pos = 0.0
        draw = True
        while pos < length:
            end = min(pos + (dash_len if draw else gap_len), length)
            if draw and end > pos:
                dashes.append(substring(line, pos, end))
            pos = end
            draw = not draw
    if not dashes:
        return
    outline = unary_union([seg.buffer(stroke_radius) for seg in dashes if not seg.is_empty])
    outline_mask = rasterize(
        [(mapping(outline), 1)],
        out_shape=(height, width),
        transform=dst_transform,
        fill=0,
        dtype=np.uint8,
        all_touched=True,
    )
    rgba[outline_mask.astype(bool)] = np.array(color, dtype=np.uint8)


def _draw_admin_boundaries_inplace(
    rgba: Any,
    boundaries_geojson_path: Path,
    dst_crs: Any,
    dst_transform: Any,
    *,
    line_color: tuple[int, int, int, int] = (60, 66, 60, 190),
    label_color: tuple[int, int, int, int] = (255, 255, 255, 255),
    halo_color: tuple[int, int, int, int] = (12, 18, 12, 255),
) -> None:
    """Draw real administrative-boundary lines and place-name labels onto ``rgba`` in place.

    Boundaries/names come from a local GeoJSON produced by
    ``scripts/fetch_admin_boundaries.py`` (a real published boundaries dataset, see its
    ``metadata.source_url``); this function does not invent geometry or names. Silently leaves
    ``rgba`` unchanged if the file is missing or unreadable, since this layer is an optional
    regional-context enhancement, not a required evidence artifact.
    """
    import numpy as np
    from PIL import Image, ImageDraw, ImageFont
    from rasterio.features import rasterize
    from rasterio.warp import transform as warp_transform
    from rasterio.warp import transform_geom
    from shapely.geometry import mapping, shape

    payload = json.loads(boundaries_geojson_path.read_text(encoding="utf-8"))
    features = payload.get("features") or []
    if not features:
        return

    height, width = rgba.shape[0], rgba.shape[1]
    pixel_size_m = abs(dst_transform.a)
    boundary_geoms = []
    labels: list[tuple[float, float, str]] = []
    inv_transform = ~dst_transform

    for feature in features:
        geometry = feature.get("geometry")
        if not geometry:
            continue
        geom = shape(geometry)
        if geom.is_empty:
            continue
        geom_target = shape(transform_geom("EPSG:4326", dst_crs, mapping(geom)))
        boundary_geoms.append(geom_target.boundary.buffer(max(pixel_size_m * 1.1, 1e-6)))
        name = (feature.get("properties") or {}).get("name")
        label_point = feature.get("label_point")
        if name and label_point and len(label_point) == 2:
            (lx,), (ly,) = warp_transform("EPSG:4326", dst_crs, [label_point[0]], [label_point[1]])
            col, row = inv_transform * (lx, ly)
            if 0 <= col < width and 0 <= row < height:
                labels.append((col, row, str(name)))

    if boundary_geoms:
        boundary_mask = rasterize(
            [(mapping(g), 1) for g in boundary_geoms],
            out_shape=(height, width),
            transform=dst_transform,
            fill=0,
            dtype=np.uint8,
            all_touched=True,
        )
        rgba[boundary_mask.astype(bool)] = np.array(line_color, dtype=np.uint8)

    if not labels:
        return
    img = Image.fromarray(rgba, mode="RGBA")
    draw = ImageDraw.Draw(img)
    font = ImageFont.load_default(size=20)
    for col, row, name in labels:
        for ox, oy in [(-1, -1), (-1, 1), (1, -1), (1, 1), (0, -1), (0, 1), (-1, 0), (1, 0)]:
            draw.text((col + ox, row + oy), name, font=font, fill=halo_color, anchor="mm")
        draw.text((col, row), name, font=font, fill=label_color, anchor="mm")
    rgba[:] = np.array(img)


def _render_satellite_crop(
    raster_path: Path,
    aoi_geom_wgs84: Any,
    *,
    width: int = 640,
    height: int = 420,
    pad_factor: float = 0.12,
    allow_variable_width: bool = False,
) -> Any:
    rgba, dst_crs, dst_transform = _reproject_raster_to_grid(
        raster_path,
        aoi_geom_wgs84,
        pad_factor=pad_factor,
        width=width,
        height=height,
        allow_variable_width=allow_variable_width,
    )
    _draw_aoi_outline_inplace(rgba, aoi_geom_wgs84, dst_crs, dst_transform)
    return rgba


def _write_satellite_context_png(
    *,
    bundle_root: Path,
    raster_relpath: str | None,
    aoi_geom_wgs84: Any | None,
    output_path: Path,
    width: int = 640,
    height: int = 420,
) -> ArtifactRef:
    if not raster_relpath or aoi_geom_wgs84 is None:
        return _missing("suitable_satellite_imagery_not_available_in_local_bundle")
    src = bundle_root / raster_relpath
    if not src.is_file():
        return _missing("suitable_satellite_imagery_not_available_in_local_bundle")
    try:
        rgba = _render_satellite_crop(
            src,
            aoi_geom_wgs84,
            width=width,
            height=height,
            allow_variable_width=True,
        )
    except Exception:
        return _missing("satellite_raster_could_not_be_rendered")
    _write_png_rgba(output_path, rgba)
    return _available(output_path, output_path.parents[1])


def _write_before_after_png(
    *,
    bundle_root: Path,
    baseline_relpath: str | None,
    recent_relpath: str | None,
    aoi_geom_wgs84: Any | None,
    output_path: Path,
    width: int = BEFORE_AFTER_PANEL_PIXEL_WIDTH,
    height: int = BEFORE_AFTER_PANEL_PIXEL_HEIGHT,
) -> ArtifactRef:
    import numpy as np
    from PIL import Image, ImageDraw

    if not baseline_relpath or not recent_relpath or aoi_geom_wgs84 is None:
        return _missing("suitable_before_after_source_imagery_not_available_in_local_bundle")
    baseline_src = bundle_root / baseline_relpath
    recent_src = bundle_root / recent_relpath
    if not baseline_src.is_file() or not recent_src.is_file():
        return _missing("suitable_before_after_source_imagery_not_available_in_local_bundle")
    try:
        (baseline_rgba, recent_rgba), dst_crs, dst_transform = _reproject_pair_to_shared_grid(
            (baseline_src, recent_src), aoi_geom_wgs84, width=width, height=height
        )
        _draw_aoi_outline_inplace(baseline_rgba, aoi_geom_wgs84, dst_crs, dst_transform)
        _draw_aoi_outline_inplace(recent_rgba, aoi_geom_wgs84, dst_crs, dst_transform)
        divider_w = _BEFORE_AFTER_DIVIDER_PX
        divider = np.full((baseline_rgba.shape[0], divider_w, 4), 255, dtype=np.uint8)
        combined = np.hstack([baseline_rgba, divider, recent_rgba])

        # A swipe-style handle (white circle + right-pointing arrow) centered on the seam reads
        # as "before/after comparison" at a glance; a bare divider line reads as a rendering
        # artifact rather than an intentional split.
        combined_img = Image.fromarray(combined, mode="RGBA")
        draw = ImageDraw.Draw(combined_img)
        cx = baseline_rgba.shape[1] + divider_w / 2
        cy = combined.shape[0] / 2
        radius = 16
        draw.ellipse(
            [cx - radius, cy - radius, cx + radius, cy + radius],
            fill=(255, 255, 255, 255),
            outline=(70, 70, 70, 255),
            width=1,
        )
        arrow_half_h = 7
        arrow_w = 9
        tip_x = cx + 3
        draw.polygon(
            [
                (tip_x - arrow_w, cy - arrow_half_h),
                (tip_x - arrow_w, cy + arrow_half_h),
                (tip_x, cy),
            ],
            fill=(40, 40, 40, 255),
        )
        combined = np.array(combined_img)
    except Exception:
        return _missing("before_after_source_imagery_could_not_be_rendered")
    _write_png_rgba(output_path, combined)
    return _available(output_path, output_path.parents[1])


def _write_regional_overview_png(
    *,
    bundle_root: Path,
    raster_relpath: str | None,
    aoi_geom_wgs84: Any | None,
    output_path: Path,
    pad_factor: float = 3.0,
    admin_boundaries_relpath: str | None = None,
    allow_variable_width: bool = False,
) -> ArtifactRef:
    if not raster_relpath or aoi_geom_wgs84 is None:
        return _missing("regional_overview_imagery_not_available_in_local_bundle")
    src = bundle_root / raster_relpath
    if not src.is_file():
        return _missing("regional_overview_imagery_not_available_in_local_bundle")
    try:
        rgba, dst_crs, dst_transform = _reproject_raster_to_grid(
            src,
            aoi_geom_wgs84,
            pad_factor=pad_factor,
            width=900,
            height=620,
            allow_variable_width=allow_variable_width,
        )
        if admin_boundaries_relpath:
            boundaries_path = bundle_root / admin_boundaries_relpath
            if boundaries_path.is_file():
                try:
                    _draw_admin_boundaries_inplace(rgba, boundaries_path, dst_crs, dst_transform)
                except Exception:
                    pass  # optional regional-context layer; never blocks the base evidence image
        _draw_aoi_outline_inplace(rgba, aoi_geom_wgs84, dst_crs, dst_transform, min_stroke_px=2.0)
    except Exception:
        return _missing("regional_overview_could_not_be_rendered")
    _write_png_rgba(output_path, rgba)
    return _available(output_path, output_path.parents[1])


def _fetch_esri_worldimagery_export_png(
    *,
    bbox_wgs84: tuple[float, float, float, float],
    width: int,
    height: int,
    timeout: float = 20.0,
) -> bytes:
    """Fetch a single, already-mosaicked satellite PNG for ``bbox_wgs84`` from the Esri World
    Imagery export REST service, sized exactly to ``width``x``height`` pixels - no client-side
    tile-stitching required (unlike the raw XYZ tile endpoint), since the export service accepts
    an arbitrary bbox and output size directly."""
    import urllib.parse
    import urllib.request

    min_lon, min_lat, max_lon, max_lat = bbox_wgs84
    params = {
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "bboxSR": "4326",
        "imageSR": "4326",
        "size": f"{width},{height}",
        "format": "png32",
        "f": "image",
    }
    url = ESRI_WORLDIMAGERY_EXPORT_URL + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers={"User-Agent": "eudr-dmi-gil-report/1.0"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def _reproject_esri_worldimagery_to_grid(
    aoi_geom_wgs84: Any,
    *,
    pad_factor: float = 0.12,
    width: int = 640,
    height: int = 420,
) -> tuple[Any, Any, Any]:
    """Fetch an Esri World Imagery crop framed around ``aoi_geom_wgs84`` onto a fixed-size RGBA
    grid, mirroring ``_reproject_raster_to_grid``'s ``(rgba, dst_crs, dst_transform)`` contract so
    the same ``_draw_aoi_outline_inplace``/``_draw_admin_boundaries_inplace`` overlay helpers work
    unchanged regardless of whether the basemap came from a local raster or this live service.

    Padding and aspect-fit are computed in Web Mercator (EPSG:3857, the CRS Esri's own tiles are
    served in), not raw WGS84 degrees, so the AOI's true on-the-ground aspect ratio is preserved
    at any latitude rather than being skewed by degree-per-km distortion.
    """
    import numpy as np
    from PIL import Image
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds
    from rasterio.warp import transform_bounds

    minx, miny, maxx, maxy = aoi_geom_wgs84.bounds
    pad_x = (maxx - minx) * pad_factor or 0.001
    pad_y = (maxy - miny) * pad_factor or 0.001
    minx, maxx = minx - pad_x, maxx + pad_x
    miny, maxy = miny - pad_y, maxy + pad_y

    dst_crs = CRS.from_epsg(3857)
    left, bottom, right, top = transform_bounds("EPSG:4326", dst_crs, minx, miny, maxx, maxy)
    target_aspect = width / height
    frame_w, frame_h = right - left, top - bottom
    frame_aspect = (frame_w / frame_h) if frame_h else target_aspect
    if frame_aspect < target_aspect:
        new_w = frame_h * target_aspect
        cx = (left + right) / 2
        left, right = cx - new_w / 2, cx + new_w / 2
    elif frame_aspect > target_aspect:
        new_h = frame_w / target_aspect
        cy = (bottom + top) / 2
        bottom, top = cy - new_h / 2, cy + new_h / 2

    request_bbox = transform_bounds(dst_crs, "EPSG:4326", left, bottom, right, top)
    image_bytes = _fetch_esri_worldimagery_export_png(bbox_wgs84=request_bbox, width=width, height=height)
    img = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    if img.size != (width, height):
        img = img.resize((width, height), Image.Resampling.LANCZOS)
    rgba = np.array(img)
    dst_transform = from_bounds(left, bottom, right, top, width, height)
    return rgba, dst_crs, dst_transform


def _write_esri_satellite_context_png(
    *,
    aoi_geom_wgs84: Any | None,
    output_path: Path,
    width: int = 640,
    height: int = 420,
    pad_factor: float = 0.12,
) -> ArtifactRef:
    if aoi_geom_wgs84 is None:
        return _missing("aoi_geometry_not_available_for_esri_satellite_context")
    try:
        rgba, dst_crs, dst_transform = _reproject_esri_worldimagery_to_grid(
            aoi_geom_wgs84, pad_factor=pad_factor, width=width, height=height
        )
        _draw_aoi_outline_inplace(rgba, aoi_geom_wgs84, dst_crs, dst_transform)
    except Exception:
        if output_path.is_file():
            return _available(output_path, output_path.parents[1])
        return _missing("esri_satellite_imagery_could_not_be_fetched_or_rendered")
    _write_png_rgba(output_path, rgba)
    return _available(output_path, output_path.parents[1])


def _write_regional_overview_png_esri(
    *,
    bundle_root: Path,
    aoi_geom_wgs84: Any | None,
    output_path: Path,
    pad_factor: float = 3.0,
    admin_boundaries_relpath: str | None = None,
) -> ArtifactRef:
    if aoi_geom_wgs84 is None:
        return _missing("aoi_geometry_not_available_for_esri_regional_overview")
    try:
        rgba, dst_crs, dst_transform = _reproject_esri_worldimagery_to_grid(
            aoi_geom_wgs84, pad_factor=pad_factor, width=900, height=620
        )
        if admin_boundaries_relpath:
            boundaries_path = bundle_root / admin_boundaries_relpath
            if boundaries_path.is_file():
                try:
                    _draw_admin_boundaries_inplace(rgba, boundaries_path, dst_crs, dst_transform)
                except Exception:
                    pass  # optional regional-context layer; never blocks the base evidence image
        _draw_aoi_outline_inplace(rgba, aoi_geom_wgs84, dst_crs, dst_transform, min_stroke_px=2.0)
    except Exception:
        if output_path.is_file():
            return _available(output_path, output_path.parents[1])
        return _missing("esri_regional_overview_could_not_be_fetched_or_rendered")
    _write_png_rgba(output_path, rgba)
    return _available(output_path, output_path.parents[1])


def _write_esri_leaflet_aoi_map_html(
    *,
    aoi_geom_wgs84: Any | None,
    aoi_name: str,
    output_path: Path,
    overlay_layers: list[tuple[str, Path | Any | None, tuple[int, int, int]]] | None = None,
) -> ArtifactRef:
    """Render a standalone interactive Leaflet map: this AOI's own boundary (only - no
    sibling-bundle AOI) over Esri World Imagery and OpenStreetMap tile basemaps, plus zero or more optional,
    independently toggle-able overlay layers (round 7: JRC 2020 baseline / forest loss /
    commodity / intersection masks, so this one file replaces separate per-layer PNG downloads).
    Each ``overlay_layers`` entry is ``(label, geojson-path-or-shapely-geometry-or-None, rgb)`` -
    entries with no geometry (unavailable/empty for this AOI) are skipped and listed in-map, never
    rendered as an empty/broken layer. Uses the Leaflet CDN build (leaflet.js/leaflet.css from
    unpkg), the same approach as the framework repo's
    ``tools/render_two_aoi_geemap_satellite_tiles.py`` reference script - viewing this file later
    requires network access to that CDN, an explicit, documented limitation, not a hidden one."""
    from shapely.geometry import mapping as shapely_mapping

    if aoi_geom_wgs84 is None:
        return _missing("aoi_geometry_not_available_for_interactive_map")
    minx, miny, maxx, maxy = aoi_geom_wgs84.bounds
    center_lat, center_lon = (miny + maxy) / 2, (minx + maxx) / 2
    aoi_feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"name": aoi_name},
                "geometry": shapely_mapping(aoi_geom_wgs84),
            }
        ],
    }

    overlay_js_blocks = []
    overlay_map_entries = []
    omitted_overlay_labels = []
    for index, (label, source, rgb) in enumerate(overlay_layers or []):
        geom = _load_layer_geometry(source)
        if geom is None:
            omitted_overlay_labels.append(label)
            continue
        feature_collection = {
            "type": "FeatureCollection",
            "features": [{"type": "Feature", "properties": {}, "geometry": shapely_mapping(geom)}],
        }
        css_color = f"rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"
        var_name = f"overlayLayer{index}"
        overlay_js_blocks.append(
            f"const {var_name} = L.geoJSON({json.dumps(feature_collection)}, {{\n"
            f"  style: {{color: {json.dumps(css_color)}, weight: 1.5, fillColor: {json.dumps(css_color)}, fillOpacity: 0.55}}\n"
            f"}}).addTo(map);"
        )
        overlay_map_entries.append(f"{json.dumps(label)}: {var_name}")

    safe_title = html.escape(aoi_name)
    layer_control_js = (
        "L.control.layers(baseMaps, "
        f"{{{', '.join(overlay_map_entries)}}}, "
        "{collapsed: false}).addTo(map);"
    )
    omitted_items = "".join(
        f"<li>{html.escape(label)}</li>" for label in omitted_overlay_labels
    )
    omissions_html = (
        f"<div class=\"omissions\"><strong>Omitted empty/unavailable overlays</strong><ul>{omitted_items}</ul></div>"
        if omitted_overlay_labels
        else ""
    )
    doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{safe_title} - satellite AOI map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    html, body, #map {{ height: 100%; margin: 0; background: #101416; }}
    .title {{
      position: absolute; z-index: 1000; left: 54px; top: 12px; max-width: calc(72% - 40px);
      color: #f7fbfb; font: 700 16px Arial, sans-serif;
      background: rgba(7, 10, 12, 0.82); padding: 8px 12px; border-radius: 8px;
    }}
    .omissions {{
      position: absolute; z-index: 1000; left: 14px; bottom: 14px; max-width: min(420px, 74%);
      color: #f7fbfb; font: 12px Arial, sans-serif;
      background: rgba(7, 10, 12, 0.82); padding: 10px 12px; border-radius: 8px;
    }}
    .omissions ul {{ margin: 6px 0 0; padding-left: 18px; }}
  </style>
</head>
<body>
<div id="map"></div>
<div class="title">{safe_title} - satellite and street basemaps</div>
{omissions_html}
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const map = L.map('map').setView([{center_lat}, {center_lon}], 13);
const esriWorldImagery = L.tileLayer({json.dumps(ESRI_WORLDIMAGERY_TILE_URL_TEMPLATE)}, {{
  maxZoom: 19,
  attribution: {json.dumps(ESRI_WORLDIMAGERY_ATTRIBUTION)}
}});
const openStreetMap = L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap contributors'
}});
const baseMaps = {{
  'Esri World Imagery': esriWorldImagery,
  'OpenStreetMap Mapnik': openStreetMap
}};
esriWorldImagery.addTo(map);
const aoiLayer = L.geoJSON({json.dumps(aoi_feature_collection)}, {{
  style: {{color: '#ffcc00', weight: 3, dashArray: '6 5', fillColor: '#ffcc00', fillOpacity: 0.18}}
}}).addTo(map);
{chr(10).join(overlay_js_blocks)}
{layer_control_js}
map.fitBounds(aoiLayer.getBounds(), {{padding: [24, 24]}});
</script>
</body>
</html>
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(doc, encoding="utf-8")
    return _available(output_path, output_path.parents[1])


def _write_mask_over_basemap_png(
    *,
    geojson_path: Path,
    output_path: Path,
    color: tuple[int, int, int, int],
    background_raster_path: Path,
    aoi_geom_wgs84: Any,
    overlay_geojson_path: Path | None = None,
    overlay_color: tuple[int, int, int, int] = _COMMODITY_OVERLAY_COLOR,
    overlay_alpha: float = _COMMODITY_OVERLAY_ALPHA,
) -> None:
    """Composite a GeoJSON evidence mask (semi-transparent color) over a real satellite basemap
    crop, with the AOI boundary drawn on top - used so pages 5/6 show the forest/loss masks in
    their real geographic context instead of a flat color swatch on a blank background.

    When ``overlay_geojson_path`` is given (round 18: the commodity/coffee-plantation mask on page
    5, or the loss-and-commodity intersection on page 6), a second mask is alpha-blended on top of
    the primary mask, before the AOI outline is drawn - so the outline always stays crisply on top
    and existing callers that never pass an overlay render byte-identically to before.
    """
    import numpy as np
    from rasterio.features import rasterize
    from rasterio.warp import transform_geom
    from shapely.geometry import mapping, shape
    from shapely.ops import unary_union

    payload = json.loads(geojson_path.read_text(encoding="utf-8"))
    features = payload.get("features") if isinstance(payload, Mapping) else None
    if not isinstance(features, list) or not features:
        raise ValueError("no features")
    geoms = []
    for feature in features:
        if not isinstance(feature, Mapping) or not isinstance(feature.get("geometry"), Mapping):
            continue
        geom = shape(feature["geometry"])
        if not geom.is_empty:
            geoms.append(geom)
    if not geoms:
        raise ValueError("no geometry")
    mask_union = unary_union(geoms)

    overlay_union = None
    if overlay_geojson_path is not None and overlay_geojson_path.is_file():
        try:
            overlay_payload = json.loads(overlay_geojson_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            overlay_payload = None
        overlay_features = (
            overlay_payload.get("features") if isinstance(overlay_payload, Mapping) else None
        )
        if isinstance(overlay_features, list):
            overlay_geoms = []
            for feature in overlay_features:
                if not isinstance(feature, Mapping) or not isinstance(feature.get("geometry"), Mapping):
                    continue
                geom = shape(feature["geometry"])
                if not geom.is_empty:
                    overlay_geoms.append(geom)
            if overlay_geoms:
                overlay_union = unary_union(overlay_geoms)

    # Frame the crop around the union of the AOI, the mask geometry, and (when present) the
    # overlay geometry, so loss/forest/commodity polygons that extend beyond the tight AOI box
    # stay visible (matches the flat-render fallback's framing).
    frame_parts = [aoi_geom_wgs84, mask_union]
    if overlay_union is not None:
        frame_parts.append(overlay_union)
    frame_geom = unary_union(frame_parts)
    width, height = EVIDENCE_MAP_PIXEL_WIDTH, EVIDENCE_MAP_PIXEL_HEIGHT
    rgba, dst_crs, dst_transform = _reproject_raster_to_grid(
        background_raster_path, frame_geom, pad_factor=0.12, width=width, height=height
    )

    mask_geom_target = shape(transform_geom("EPSG:4326", dst_crs, mapping(mask_union)))
    mask = rasterize(
        [(mapping(mask_geom_target), 1)],
        out_shape=(height, width),
        transform=dst_transform,
        fill=0,
        dtype="uint8",
        all_touched=True,
    )
    if int(np.count_nonzero(mask)) == 0:
        raise ValueError("empty rasterized mask")

    alpha = 0.62
    mask_bool = mask.astype(bool)
    color_arr = np.array(color[:3], dtype=np.float64)
    blended = rgba[mask_bool, :3].astype(np.float64) * (1 - alpha) + color_arr * alpha
    rgba[mask_bool, :3] = blended.astype(np.uint8)
    rgba[mask_bool, 3] = 255

    if overlay_union is not None:
        overlay_geom_target = shape(transform_geom("EPSG:4326", dst_crs, mapping(overlay_union)))
        overlay_mask = rasterize(
            [(mapping(overlay_geom_target), 1)],
            out_shape=(height, width),
            transform=dst_transform,
            fill=0,
            dtype="uint8",
            all_touched=True,
        )
        overlay_bool = overlay_mask.astype(bool)
        if np.count_nonzero(overlay_bool):
            overlay_color_arr = np.array(overlay_color[:3], dtype=np.float64)
            overlay_blended = (
                rgba[overlay_bool, :3].astype(np.float64) * (1 - overlay_alpha)
                + overlay_color_arr * overlay_alpha
            )
            rgba[overlay_bool, :3] = overlay_blended.astype(np.uint8)
            rgba[overlay_bool, 3] = 255

    _draw_aoi_outline_inplace(rgba, aoi_geom_wgs84, dst_crs, dst_transform)
    _write_png_rgba(output_path, rgba)


def _load_layer_geometry(source: Path | Any | None) -> Any | None:
    """Resolve one layer's geometry, whether given as a GeoJSON path or an already-resolved
    Shapely geometry (round 26: the page-6 "current forest" layer is computed in-memory as a
    baseline-minus-loss difference rather than read from its own file, so
    `_write_layered_mask_over_basemap_png`'s callers need to pass either kind interchangeably).
    Returns ``None`` for anything empty/missing/unparseable - never raises, since an absent layer
    is a normal, expected case for every caller of this function, not an error.
    """
    from shapely.geometry import shape
    from shapely.geometry.base import BaseGeometry
    from shapely.ops import unary_union

    if source is None:
        return None
    if isinstance(source, BaseGeometry):
        return None if source.is_empty else source
    path = Path(source)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    features = payload.get("features") if isinstance(payload, Mapping) else None
    if not isinstance(features, list) or not features:
        return None
    geoms = []
    for feature in features:
        if not isinstance(feature, Mapping) or not isinstance(feature.get("geometry"), Mapping):
            continue
        geom = shape(feature["geometry"])
        if not geom.is_empty:
            geoms.append(geom)
    if not geoms:
        return None
    return unary_union(geoms)


def _write_layered_mask_over_basemap_png(
    *,
    layers: list[tuple[Path | Any | None, tuple[int, int, int], float]],
    output_path: Path,
    background_raster_path: Path,
    aoi_geom_wgs84: Any,
) -> None:
    """Composite an ordered stack of evidence-mask layers (each its own color/alpha) over a real
    satellite basemap crop, with the AOI boundary drawn on top last - round 26's generalization of
    `_write_mask_over_basemap_png`'s single-mask-plus-one-overlay case to an arbitrary-length
    stack, so one composite (page 6's "current forest + commodity + loss" evidence map) can show
    every layer relevant to it without a bespoke function per layer combination. Each ``layers``
    entry is ``(source, color_rgb, alpha)``; ``source`` may be a GeoJSON path, an in-memory
    Shapely geometry, or ``None`` - entries that resolve to no geometry (via
    `_load_layer_geometry`) are silently skipped, not an error, so callers can pass every
    candidate layer for an AOI regardless of which ones actually have data. Layers are blended in
    list order, so later entries draw on top of earlier ones. Raises ``ValueError`` only when
    *every* entry is empty - matching `_png_from_geojson_ref`'s existing
    ``source_mask_contains_no_renderable_features`` handling for its caller to catch.
    """
    import numpy as np
    from rasterio.features import rasterize
    from rasterio.warp import transform_geom
    from shapely.geometry import mapping, shape
    from shapely.ops import unary_union

    resolved: list[tuple[Any, tuple[int, int, int], float]] = []
    for source, color, alpha in layers:
        geom = _load_layer_geometry(source)
        if geom is not None:
            resolved.append((geom, color, alpha))
    if not resolved:
        raise ValueError("no renderable layers")

    frame_geom = unary_union([aoi_geom_wgs84, *(geom for geom, _, _ in resolved)])
    width, height = EVIDENCE_MAP_PIXEL_WIDTH, EVIDENCE_MAP_PIXEL_HEIGHT
    rgba, dst_crs, dst_transform = _reproject_raster_to_grid(
        background_raster_path, frame_geom, pad_factor=0.12, width=width, height=height
    )

    for geom, color, alpha in resolved:
        geom_target = shape(transform_geom("EPSG:4326", dst_crs, mapping(geom)))
        mask = rasterize(
            [(mapping(geom_target), 1)],
            out_shape=(height, width),
            transform=dst_transform,
            fill=0,
            dtype="uint8",
            all_touched=True,
        )
        mask_bool = mask.astype(bool)
        if not np.count_nonzero(mask_bool):
            continue
        color_arr = np.array(color, dtype=np.float64)
        blended = rgba[mask_bool, :3].astype(np.float64) * (1 - alpha) + color_arr * alpha
        rgba[mask_bool, :3] = blended.astype(np.uint8)
        rgba[mask_bool, 3] = 255

    _draw_aoi_outline_inplace(rgba, aoi_geom_wgs84, dst_crs, dst_transform)
    _write_png_rgba(output_path, rgba)


def _write_layered_mask_flat_png(
    *,
    layers: list[tuple[Path | Any | None, tuple[int, int, int], float]],
    output_path: Path,
) -> None:
    """`_write_geojson_mask_png`'s multi-layer counterpart, used (like that function) only when no
    background satellite raster is available to composite onto - flat opaque colors on a plain
    background instead of a basemap crop, framed around the union of whichever layers actually
    have geometry. Later entries in ``layers`` paint over earlier ones, same draw order as
    `_write_layered_mask_over_basemap_png`.
    """
    import numpy as np
    from rasterio.features import rasterize
    from rasterio.transform import from_bounds
    from shapely.ops import unary_union

    resolved: list[tuple[Any, tuple[int, int, int]]] = []
    for source, color, _alpha in layers:
        geom = _load_layer_geometry(source)
        if geom is not None:
            resolved.append((geom, color))
    if not resolved:
        raise ValueError("no renderable layers")

    union = unary_union([geom for geom, _ in resolved])
    minx, miny, maxx, maxy = union.bounds
    if minx == maxx:
        minx -= 0.0001
        maxx += 0.0001
    if miny == maxy:
        miny -= 0.0001
        maxy += 0.0001
    width, height = 640, 420
    transform = from_bounds(minx, miny, maxx, maxy, width, height)
    rgba = np.zeros((height, width, 4), dtype=np.uint8)
    rgba[:, :, :] = np.array([248, 250, 247, 255], dtype=np.uint8)
    any_drawn = False
    for geom, color in resolved:
        mask = rasterize(
            [(geom, 1)],
            out_shape=(height, width),
            transform=transform,
            fill=0,
            dtype="uint8",
            all_touched=True,
        )
        mask_bool = mask.astype(bool)
        if not np.count_nonzero(mask_bool):
            continue
        any_drawn = True
        rgba[mask_bool] = np.array((*color, 255), dtype=np.uint8)
    if not any_drawn:
        raise ValueError("empty rasterized mask")
    _write_png_rgba(output_path, rgba)


def _layered_png_from_geojson_refs(
    *,
    layers: list[tuple[Path | Any | None, tuple[int, int, int], float]],
    output_path: Path,
    background_raster_path: Path | None,
    aoi_geom_wgs84: Any | None,
    unavailable_reason: str,
) -> ArtifactRef:
    """`_png_from_geojson_ref`'s multi-layer counterpart: same availability contract (no basemap
    raster falls back to a flat rendering via `_write_layered_mask_flat_png`; every layer empty
    resolves to a `_missing` artifact with the same `source_mask_contains_no_renderable_features`
    reason the single-mask path uses), but for an ordered stack of layers rendered by
    `_write_layered_mask_over_basemap_png` instead of one mask plus one overlay.
    """
    if background_raster_path is not None and aoi_geom_wgs84 is not None:
        try:
            _write_layered_mask_over_basemap_png(
                layers=layers,
                output_path=output_path,
                background_raster_path=background_raster_path,
                aoi_geom_wgs84=aoi_geom_wgs84,
            )
            return _available(output_path, output_path.parents[1])
        except ValueError:
            return _missing("source_mask_contains_no_renderable_features")
    try:
        _write_layered_mask_flat_png(layers=layers, output_path=output_path)
    except ValueError:
        return _missing(unavailable_reason)
    return _available(output_path, output_path.parents[1])


def _write_legend_png(output_path: Path) -> None:
    import numpy as np

    width = 420
    height = 150
    rgba = np.zeros((height, width, 4), dtype=np.uint8)
    rgba[:, :, :] = np.array([255, 255, 255, 255], dtype=np.uint8)
    swatches = [
        (20, 20, (33, 122, 72, 255)),
        (20, 55, (198, 40, 40, 255)),
        (20, 90, (30, 136, 229, 255)),
        (220, 20, (102, 45, 145, 255)),
    ]
    for x, y, color in swatches:
        rgba[y : y + 18, x : x + 42] = np.array(color, dtype=np.uint8)
    _write_png_rgba(output_path, rgba)


def _write_png_rgba(path: Path, rgba: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    height, width, channels = rgba.shape
    if channels != 4:
        raise ValueError("expected RGBA image")
    raw = b"".join(b"\x00" + rgba[row].tobytes() for row in range(height))

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    png = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, level=9))
        + chunk(b"IEND", b"")
    )
    path.write_bytes(png)


def _available(path: Path, report_root: Path) -> ArtifactRef:
    return ArtifactRef(
        path=_relpath(path, report_root),
        available=True,
        availability_status="available",
        checksum_sha256=compute_sha256(path),
        content_type=_content_type(path),
    )


def _missing(reason: str) -> ArtifactRef:
    return ArtifactRef(
        path=None,
        available=False,
        availability_status=reason,
        checksum_sha256=None,
        content_type=None,
    )


def _relpath(path: Path, root: Path) -> str:
    return os.path.relpath(path, start=root).replace("\\", "/")


def _content_type(path: Path) -> str | None:
    suffix = path.suffix.lower()
    return {
        ".json": "application/json",
        ".html": "text/html",
        ".pdf": "application/pdf",
        ".csv": "text/csv",
        ".png": "image/png",
        ".geojson": "application/geo+json",
    }.get(suffix)


def _stable_value_str(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)
