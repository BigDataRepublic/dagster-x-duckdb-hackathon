import datetime

import dagster as dg

yearly_partitions = dg.TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1 *", start=datetime.datetime(2023, 1, 1), fmt="%Y-%m-%d"
)
destination_partitions = dg.DynamicPartitionsDefinition(name="destination_partitions")


weather_partitions = dg.MultiPartitionsDefinition({"year": yearly_partitions, "destination": destination_partitions})
