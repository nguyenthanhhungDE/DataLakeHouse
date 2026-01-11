<<<<<<< HEAD
from dagster import (
    load_assets_from_modules,
    load_assets_from_package_module,
    Definitions,
)
from dagstermill import (
    local_output_notebook_io_manager,
    ConfigurableLocalOutputNotebookIOManager,
)
from . import bronze, gold, ml, eda, silver, warehouse, silver_staging

bronze_layer_assets = load_assets_from_modules([bronze])
silver_layer_assets = load_assets_from_modules([silver, silver_staging])
gold_layer_assets = load_assets_from_modules([gold])
platium_layer_assets = load_assets_from_modules([warehouse])
ml_layer_assets = load_assets_from_modules([ml])
eda_layer_assets = load_assets_from_modules([eda])
# generate_layer_assets = load_assets_from_modules([generate])
=======
from dagster import load_assets_from_modules,load_assets_from_package_module, Definitions
from dagstermill import local_output_notebook_io_manager, ConfigurableLocalOutputNotebookIOManager
from . import bronze,silver,gold,platium,ml,eda

bronze_layer_assets = load_assets_from_modules([bronze])
silver_layer_assets = load_assets_from_modules([silver])
gold_layer_assets = load_assets_from_modules([gold])
platium_layer_assets = load_assets_from_modules([platium])
ml_layer_assets = load_assets_from_modules([ml])
eda_layer_assets = load_assets_from_modules([eda])
>>>>>>> 3505f1f5c86fece65068d55af2288d4a50d7eb0e

# defs = Definitions(
#     assets=load_assets_from_package_module(eda),
#     resources={"output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager()},
<<<<<<< HEAD
# )
=======
# )
>>>>>>> 3505f1f5c86fece65068d55af2288d4a50d7eb0e
