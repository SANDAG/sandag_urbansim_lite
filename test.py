from flask import Flask
from flask import request, redirect
from flask import render_template
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
from db import sql
from pysandag import database
db_connection_string = database.get_connection_string('data\config.yml', 'mssql_db')
sql_in_engine = create_engine(db_connection_string)

results_sql = '''  SELECT
  b.[year_built] as year
 ,g.name as jurisdiction
 ,SUM(CASE WHEN run_id = 4 THEN [residential_units] ELSE 0 END) as "Random"
 ,SUM(CASE WHEN run_id = 1 THEN [residential_units] ELSE 0 END) as "Distance To Coast"
  FROM [spacecore].[staging].[urbansim_lite_output] as b
  LEFT JOIN [spacecore].[urbansim].[parcels] as p
  ON p.parcel_id = b.parcel_id
  LEFT JOIN (SELECT [name]
      ,[shape]
  FROM [data_cafe].[ref].[geography_zone]
  WHERE geography_type_id = 150) as g
  on p.shape.STIntersects(g.shape) = 1
  GROUP by b.[year_built], g.name
  ORDER by b.[year_built], g.name '''

results_df = pd.read_sql(results_sql, sql_in_engine, index_col=None)
feature_names = results_df.jurisdiction.unique()

app = Flask(__name__)


@app.route('/')
def my_form_post_pop():

    # Determine the selected feature
    current_feature_name = request.args.get("feature_name")

    if current_feature_name is None:
        current_feature_name = "Carlsbad"
    df = results_df.loc[(results_df.jurisdiction == str(current_feature_name))]

    listx= df.year.unique()
    new_list = []
    for item in listx:
        new_list.append(str(item))

    series = []
    for x in ['Random', 'Distance To Coast']:
        listy = df[x].tolist()
        new_list2 = []
        for item in listy:
            new_list2.append(float(item))
        series.append({"name": str(x), "data": new_list2})

    chart = {"renderTo": 'chart_ID', "type": 'line', "height": 450}
    title = {"text": str(current_feature_name) + ' Trends'}
    xAxis = {"title": {"text": 'Year'}, "categories": new_list}
    yAxis = {"title": {"text": 'Units'}}
    # render template
    html = render_template(
        'result-form.html',
        feature_names=feature_names,
        current_feature_name=current_feature_name,
        chartID='chart_ID', chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis
    )
    return html


if __name__ == "__main__":
	app.run()