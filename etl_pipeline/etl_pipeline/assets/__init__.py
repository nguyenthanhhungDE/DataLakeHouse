from dagster import load_assets_from_modules,load_assets_from_package_module, Definitions
from dagstermill import local_output_notebook_io_manager, ConfigurableLocalOutputNotebookIOManager
from . import bronze,silver,gold,platium,ml,eda

bronze_layer_assets = load_assets_from_modules([bronze])
silver_layer_assets = load_assets_from_modules([silver])
gold_layer_assets = load_assets_from_modules([gold])
platium_layer_assets = load_assets_from_modules([platium])
ml_layer_assets = load_assets_from_modules([ml])
eda_layer_assets = load_assets_from_modules([eda])

# defs = Definitions(
#     assets=load_assets_from_package_module(eda),
#     resources={"output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager()},
# )