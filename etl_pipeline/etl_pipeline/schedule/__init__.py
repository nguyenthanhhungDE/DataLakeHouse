from ..job import reload_data 
from dagster import ScheduleDefinition

'''

Crontab Syntax
+---------------- minute (0 - 59)
|  +------------- hour (0 - 23)
|  |  +---------- day of month (1 - 31)
|  |  |  +------- month (1 - 12)
|  |  |  |  +---- day of week (0 - 6) (Sunday is 0 or 7)
|  |  |  |  |
*  *  *  *  *  command to be executed

* means all values are acceptable

'''

reload_data_schedule = ScheduleDefinition(
    job=reload_data,
    cron_schedule="30 21 06 04 *",  # every day at midnight weekend day
)







# Path: etl_pipeline/etl_pipeline/schedule/__init__.py