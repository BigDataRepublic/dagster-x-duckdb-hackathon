import dagster as dg


@dg.asset(group_name="bronze")
def user_locations(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
