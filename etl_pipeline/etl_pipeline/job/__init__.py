from dagster import AssetSelection, define_asset_job
# from ..partitions import monthly_partition, weekly_partition
# from ..assets.constant import BRONZE, silver
# from .assets import silver_layer_assets, bronze_layer_assets
BRONZE = "bronze"
SILVER = "silver"

bronze_data_by_week = AssetSelection.groups(BRONZE)
source_data = AssetSelection.groups(SILVER)

# update_pipeline_job = define_asset_job(
#     name="update_pipeline_job",
#     # partitions_def=monthly_partition,
#     selection=AssetSelection.all()
# )

reload_data = define_asset_job(
    name="reload_data",
    selection=bronze_data_by_week,
)

