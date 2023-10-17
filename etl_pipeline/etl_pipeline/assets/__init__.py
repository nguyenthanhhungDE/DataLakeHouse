from dagster import load_assets_from_modules, file_relative_path

from . import bronze,silver


bronze_layer_assets = load_assets_from_modules([bronze])
silver_layer_assets = load_assets_from_modules([silver])