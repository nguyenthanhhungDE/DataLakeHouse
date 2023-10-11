from dagster import load_assets_from_modules, file_relative_path

from . import bronze


bronze_layer_assets = load_assets_from_modules([bronze])