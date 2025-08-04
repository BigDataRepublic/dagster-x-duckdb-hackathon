import dagster as dg


@dg.asset()
def user_locations(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
