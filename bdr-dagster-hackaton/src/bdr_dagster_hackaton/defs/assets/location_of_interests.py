import dagster as dg


@dg.asset(
    deps=["travel_plans", "user_locations"],
    group_name="bronze",
)
def location_of_interests(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
