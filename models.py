import datasources
import orca
import utils
import bulk_insert


@orca.injectable()
def year(iter_var):
    return iter_var


@orca.step('scheduled_development_events')
def scheduled_development_events(hu_forecast, households, feasibility,regional_controls,parcels,year):
    utils.run_scheduled_development(hu_forecast, households, feasibility, regional_controls,parcels,year)


@orca.step('negative_parcel_reducer')
def negative_parcel_reducer(hu_forecast, year):
    utils.run_reducer(hu_forecast, year)


@orca.step('feasibility')
def feasibility(year):
    utils.run_feasibility(year)


@orca.step('subregional_share')
def subregional_share(year,households):
    utils.run_subregional_share(year,households)


@orca.step('residential_developer')
def residential_developer(households, hu_forecast, regional_controls, feasibility, year):
    utils.run_developer(households=households,
                        hu_forecast=hu_forecast,
                        reg_controls=regional_controls,
                        supply_fname="units_added",
                        feasibility=feasibility,
                        year=year)


@orca.step('summary')
def summary(year):
    utils.summary(year)


@orca.step('write_to_sql')
def write_to_sql(parcel_tables, year):
    bulk_insert.run_insert(parcel_tables, year)


@orca.step('match_prior_run')
def match_prior_run(run_match_output, hu_forecast):
    utils.run_matching(run_match_output, hu_forecast)
