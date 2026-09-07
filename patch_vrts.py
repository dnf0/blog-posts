import fs

with open('content/2026-05-12-why-vrts-fail.mdx', 'r', encoding='utf8') as f:
    text = f.read()

text = text.replace(
    'The established Geospatial Data Abstraction Library (GDAL) ecosystem approach',
    'The established Geospatial Data Abstraction Library (GDAL) [1] ecosystem approach'
)

text = text.replace(
    'into thousands or millions of Cloud Optimised GeoTIFFs (COGs).',
    'into thousands or millions of Cloud Optimised GeoTIFFs (COGs) [2].'
)

text = text.replace(
    'whereas Zarr and Lance remain $O(1)$.',
    'whereas Zarr [3] and Lance [4] remain $O(1)$.'
)

text = text.replace(
    'inside the `pytest-benchmark` harness.',
    'inside the `pytest-benchmark` [5] harness.'
)

refs_old = """## References

1. [Geospatial Data Abstraction Library (GDAL)](https://gdal.org/)
2. [Cloud Optimised GeoTIFF specification](https://www.cogeo.org/)
3. [SpatioTemporal Asset Catalogue (STAC)](https://stacspec.org/)
4. [Zarr Storage Format](https://zarr.dev/)
5. [Lance Format](https://lancedb.github.io/lance/)
6. [pytest-benchmark](https://pytest-benchmark.readthedocs.io/)"""

refs_new = """## References

- [1] [Geospatial Data Abstraction Library (GDAL)](https://gdal.org/)
- [2] [Cloud Optimised GeoTIFF specification](https://www.cogeo.org/)
- [3] [Zarr Storage Format](https://zarr.dev/)
- [4] [Lance Format](https://lancedb.github.io/lance/)
- [5] [pytest-benchmark](https://pytest-benchmark.readthedocs.io/)"""

text = text.replace(refs_old, refs_new)

with open('content/2026-05-12-why-vrts-fail.mdx', 'w', encoding='utf8') as f:
    f.write(text)

