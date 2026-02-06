# Hansen fixture tiles

Place a small, deterministic Hansen fixture tileset here for local runs/CI.

Expected layout (example):

```
tests/fixtures/hansen/tiles/
  N50_E020/
    treecover2000.tif
    lossyear.tif
```

Configure the example run with:

```
EUDR_DMI_HANSEN_TILE_DIR=tests/fixtures/hansen/tiles
```

These fixtures are for tests and deterministic local examples only. They are not
published to evidence bundles or the Digital Twin.
