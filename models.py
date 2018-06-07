import datasources
import orca
import utils
import bulk_insert


@orca.injectable()
def year(iter_var):
    return iter_var


@orca.step('scheduled_development_events')
def scheduled_development_events(hu_forecast,households,year):
    utils.run_scheduled_development(hu_forecast,households,year)


@orca.step('negative_parcel_reducer')
def negative_parcel_reducer(hu_forecast, year):
    utils.run_reducer(hu_forecast, year)


@orca.step('feasibility')
def feasibility(year):
    utils.run_feasibility(year)


@orca.step('residential_developer')
def residential_developer(parcels, households, hu_forecast, regional_controls, jurisdictions, feasibility, year):
    utils.run_developer(parcels=parcels,
                        households=households,
                        hu_forecast=hu_forecast,
                        reg_controls=regional_controls,
                        jurisdictions=jurisdictions,
                        supply_fname="units_added",
                        feasibility=feasibility,
                        year=year)


@orca.step('summary')
def summary(year):
    utils.summary(year)


@orca.step('write_to_sql')
def write_to_sql(parcel_tables, year):
    bulk_insert.run_insert(parcel_tables, year)
