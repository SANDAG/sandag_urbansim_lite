import datasources
import orca
import utils


@orca.injectable()
def year(iter_var):
    return iter_var


@orca.step('scheduled_development_events')
def scheduled_development_events(buildings):
     print year
# def scheduled_development_events(scheduled_development_events, buildings):
#     sched_dev = scheduled_development_events.to_frame()
#     sched_dev = sched_dev[sched_dev.year_built==year]
#     if len(sched_dev) > 0:
#         max_bid = buildings.index.values.max()
#         idx = np.arange(max_bid + 1,max_bid+len(sched_dev)+1)
#         sched_dev['building_id'] = idx
#         sched_dev = sched_dev.set_index('building_id')
#         from urbansim.developer.developer import Developer
#         merge = Developer(pd.DataFrame({})).merge
#         b = buildings.to_frame(buildings.local_columns)
#         all_buildings = merge(b,sched_dev[b.columns])
#         orca.add_table("buildings", all_buildings)



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
