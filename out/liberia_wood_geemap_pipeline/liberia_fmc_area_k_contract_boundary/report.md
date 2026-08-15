# Liberia FMC Area K wood-source screening AOI

AOI: `liberia_fmc_area_k_contract_boundary`

Generated: 2026-08-12T14:53:44Z

Forest Management Contract Area K concession boundary for EUDR wood-source forest baseline, post-2020 disturbance, and source-context screening.

## Screening Interpretation

This run detected post-2020 forest disturbance signals inside the concession-level source context. Because the supplied geometry is a concession boundary, not a harvesting block or log-origin geometry, the result is a human-review screening signal rather than shipment-specific source or legality evidence.

## Inspection Links

- [Interactive map](map.html)
- [Static evidence composite](evidence_composite_2025.png)
- [Before/after Sentinel-2 comparison](before_after_sentinel2.png)
- [Metrics JSON](metrics.json)
- [Metrics CSV](metrics.csv)

## Key Evidence Logic

Forest coverage by end-2025 is JRC GFC2020 forest baseline minus Hansen loss pixels dated 2021-2025. TMF deforestation and TMF degradation are reported separately. The supplied AOI is a concession/source-context geometry, not plot-level or shipment-level proof.

## Glossary

- **AOI**: Area of Interest: the polygon being screened.
- **FMC**: Forest Management Contract: concession-level source context.
- **EUDR**: European Union Deforestation Regulation.
- **GEE**: Google Earth Engine.
- **JRC**: Joint Research Centre.
- **GFC2020**: JRC Global Forest Cover 2020 baseline.
- **Hansen GFC**: UMD/Hansen Global Forest Change lossyear product.
- **TMF**: JRC Tropical Moist Forest product family.
- **FDP**: Forest Data Partnership probability models.
- **ha**: Hectare, 10,000 square meters.
