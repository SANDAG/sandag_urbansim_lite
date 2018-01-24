import datasources
import orca
import utils


@orca.injectable()
def year(iter_var):
    return iter_var


@orca.step('feasibility')
def feasibility(parcels, year):
    utils.run_feasibility(parcels, year)


@orca.step('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year, regional_controls, jurisdictions):
    utils.run_developer(forms=None,
                        parcels=parcels,
                        agents=households,
                        buildings=buildings,
                        reg_controls=regional_controls,
                        jurisdictions=jurisdictions,
                        supply_fname="residential_units",
                        total_units=parcels.residential_units,
                        feasibility=feasibility,
                        year=year,
                        target_vacancy=0.0,
                        form_to_btype_callback=None,
                        add_more_columns_callback=None,
                        bldg_sqft_per_job=400.0)
