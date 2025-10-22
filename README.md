To install dependencies run `uv sync` (poetry probably works too).

Dependency `pypostal` currently has to be installed manually. follow the guide here for installation: https://github.com/openvenues/pypostal


1. Extraction: run `postalcrawl/extract/main.py`
2. Validation: run `postalcrawl/validate/main.py` (requires OSM Nominatim instance)
3. Create dataset: run `postalcrawl/pack/main.py`
