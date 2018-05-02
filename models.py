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
def feasibility(parcels, year):
    utils.run_feasibility(parcels, year)


@orca.step('residential_developer')
def residential_developer(feasibility, households, hu_forecast, parcels, year, regional_controls, jurisdictions):
    utils.run_developer(forms=None,
                        parcels=parcels,
                        households=households,
                        hu_forecast=hu_forecast,
                        reg_controls=regional_controls,
                        jurisdictions=jurisdictions,
                        supply_fname="units_added",
                        total_units=parcels.residential_units,
                        feasibility=feasibility,
                        year=year,
                        target_vacancy=0.0,
                        form_to_btype_callback=None,
                        add_more_columns_callback=None,
                        bldg_sqft_per_job=400.0)


@orca.step('summary')
def summary(year):
    utils.summary(year)


@orca.step('write_to_sql')
def write_to_sql(year):
    bulk_insert.run_insert(year)
