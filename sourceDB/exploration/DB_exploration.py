#!/usr/bin/env python
# coding: utf-8

# # Data Base Analysis

# In[ ]:


import sqlalchemy as sa
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.dialects import oracle
from sqlalchemy.sql import select
from sqlalchemy.sql import text

import psycopg2
import mysql.connector as msql
import cx_Oracle

import numpy as np
import pandas as pd
import seaborn as sns
import plotly.express as px
import plotly.graph_objs as go

import dash
import dash_table
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_cytoscape as cyto
from jupyter_dash import JupyterDash
from dash import no_update
from dash.dependencies import Input, Output, State

import networkx as nx
import time
from tqdm import tqdm
import json
import os


# ## Functions

# In[ ]:


def query_w_header(query):
    with engine.connect() as con:
        statement = text(query)
        res = con.execute(statement)
        df = pd.DataFrame(res)

    return df


def query_wo_header(query):
    with engine.connect() as con:

        statement = text(query)
        df = pd.read_sql(statement, con)

    return df


def ego_graph(G, n, radius=1, center=True, undirected=False, distance=None):
    if undirected:
        if distance is not None:
            sp, _ = nx.single_source_dijkstra(
                G.to_undirected(), n, cutoff=radius, weight=distance
            )
        else:
            sp = dict(
                nx.single_source_shortest_path_length(
                    G.to_undirected(), n, cutoff=radius
                )
            )
    else:
        if distance is not None:
            sp, _ = nx.single_source_dijkstra(G, n, cutoff=radius, weight=distance)
        else:
            sp = dict(nx.single_source_shortest_path_length(G, n, cutoff=radius))

    H = G.subgraph(sp).copy()
    if not center:
        H.remove_node(n)
    return H


# ## DB connection

# In[ ]:


try:
    with open("config.json") as json_data_file:
        data = json.load(json_data_file)
except FileNotFoundError:
        print('config.json file missing')


# In[ ]:


connector = data["database"]["connector"]
sqluser = data["database"]["user"]
sqlpass = data["database"]["passwd"]
host = data["database"]["host"]
port = data["database"]["port"]
dbname = data["database"]["db"]
database = data["database"]["database"]
driver = data["database"]["driver"]
schema_name = data["database"]["schema"]

connection = "{}+{}://{}:{}@{}:{}/{}".format(
    driver, connector, sqluser, sqlpass, host, port, dbname
)
engine = create_engine(connection)


# ## Queries SQL
# #### All queries needed to run these notebook

# In[ ]:


if database == "postgres":

    # Query1: Discover schemas
    query1_schemas = """ SELECT schema_name
                 FROM information_schema.schemata; 
                 """

    # Query2: Show all tables and columns

    query2_all_tables = """ select t.table_schema, t.table_name, c.column_name
                    from information_schema."tables" t
                    join information_schema."columns" c
                    on t.table_name = c.table_name
                    where t.TABLE_TYPE = 'BASE TABLE' 
                    """

    # Query3: All keys (primary and foreigner), and target tables

    query3_all_keys = """select kcu.constraint_schema, kcu.table_name, kcu.column_name , kcu.constraint_name, 
                kcu2.table_name as referenced_table, kcu2.column_name as  referenced_column
                from information_schema.key_column_usage kcu 
                join information_schema.referential_constraints rc using(constraint_schema, constraint_name)
                join information_schema.key_column_usage kcu2 
                on kcu2.constraint_schema = rc.unique_constraint_schema 
                and kcu2.constraint_name  = rc.unique_constraint_name
                and kcu2.ordinal_position = kcu.position_in_unique_constraint 
                """

    # Query4: First 10 lines from each table

    query4_first10 = """ select * from {}.{} limit 10 
    """

    # Query5: Columns Stats

    query5_col_stats = """select t.table_schema, t.table_name , c.column_name, c.data_type , stt.n_live_tup, (st.null_frac * stt.n_live_tup)/100
                from information_schema."tables" t
                join information_schema."columns" c
                on t.table_name = c.table_name
                left join pg_stat_all_tables stt
                on stt.relname = t.table_name 
                left join pg_catalog.pg_stats st
                on st. attname  = c.column_name and st.tablename = t.table_name 
                where t.TABLE_TYPE = 'BASE TABLE' 
                """

    # Query6: Columns commentaries

    query6_col_comments = """SELECT c.table_schema, c.table_name, c.column_name, pgd.description
                FROM pg_catalog.pg_statio_all_tables as st
                inner join pg_catalog.pg_description pgd 
                on pgd.objoid=st.relid
                inner join information_schema.columns c 
                on (pgd.objsubid=c.ordinal_position
                and c.table_schema=st.schemaname 
                and c.table_name=st.relname) 
                """

    # Query7: Tables commentaries - number of rows
    query7_table_comments = """select u1.schemaname, p1.relname, u1.n_live_tup, p2.description 
                from pg_stat_all_tables u1 
                left join pg_Class p1
                on u1.relname = p1.relname
                left join pg_Description p2 
                on p2.ObjOID = u1.relid
                where p2.objsubid = 0
                """

# TODO This is under consideration:
#    query7 = """select u1.schemaname, p1.relname, u1.n_live_tup, p2.description
#                from pg_stat_all_tables u1
#                left join pg_Class p1
#                on u1.relname = p1.relname
#                left join pg_Description p2
#                on p2.ObjOID = p1.OID where ObjSubID = 0 """


# In[ ]:


if database == "oracle":

    # Query1: Discover schemas

    query1_schemas = """select username as schema_name
                from sys.all_users
                order by username
                """

    # Query2: Show all tables and columns

    query2_all_tables = """select owner, table_name , column_name 
                FROM ALL_TAB_COLUMNS 
                """
    # Query3: All keys (primary and foreigner), and target tables

    query3_all_keys = """SELECT uc_r.owner, uc_r.table_name, ucc_r.column_name, uc_r.constraint_name,
                uc_p.constraint_name, uc_p.table_name, ucc_p.column_name
                from all_constraints uc_r
                join all_cons_columns ucc_r on ucc_r.constraint_name = uc_r.constraint_name
                join all_constraints uc_p on uc_p.constraint_name = uc_r.r_constraint_name
                join all_cons_columns ucc_p on ucc_p.constraint_name = uc_p.constraint_name
                and ucc_p.position = ucc_r.position
                where uc_r.constraint_type = 'R' 
                """

    # Query4: First 10 lines from each table

    query4_first10 = """select * from {}.{} WHERE ROWNUM <= 10 
    """

    # Query5: Columns Stats

    query5_col_stats = """SELECT a.owner, ab_.TABLE_NAME, ab_.column_name as "column", ac.DATA_TYPE as Dtype, a.num_rows as "nb_rows", 
                ab_.num_nulls as "null_count"
                FROM ALL_TABLES a
                JOIN ALL_TAB_COL_STATISTICS ab_
                ON a.table_name = ab_.table_name
                JOIN all_tab_cols ac
                ON ac.table_name = ab_.table_name AND ac.COLUMN_NAME = ab_.COLUMN_NAME 
                """

    # Query6: Columns commentaries

    query6_col_comments = """SELECT a.table_name, a.column_name, c.comments  
                from ALL_TAB_COLS a
                join ALL_COL_COMMENTS c
                ON a.table_name = c.table_name AND a.COLUMN_NAME = c.COLUMN_NAME 
                """

    # Query7: Tables commentaries - number of rows

    query7_table_comments = """SELECT c.owner, c.table_name, abl.num_rows,c.comments  
                from  ALL_TAB_COMMENTS c
                JOIN ALL_TABLES abl
                ON c.TABLE_NAME = abl.TABLE_NAME 
                """


# In[ ]:


if database == "mysql":

    # Query1: Discover schemas

    query1_schemas = """select SCHEMA_NAME from information_schema.SCHEMATA s
    """

    # Query2: Show all tables and columns

    query2_all_tables = """select table_schema, table_name, column_name 
                from information_schema.`COLUMNS` c 
                """
    # Query3: All keys (primary and foreigner), and target tables

    query3_all_keys = """SELECT CONSTRAINT_SCHEMA, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME 
                FROM INFORMATION_SCHEMA.key_column_usage 
                """

    # Query4: First 10 lines from each table

    query4_first10 = """select * from {}.{} limit 10 
    """

    # Query5: Columns Stats (#Fro MySql incomplete query)

    query5_col_stats = """SELECT c.table_schema, t.table_name, c.column_name, c.data_type, t.table_rows, t.table_rows
                from information_schema.columns c
                join information_schema.tables t
                on c.table_name = t.table_name 
                """

    # Query6: Columns commentaries

    query6_col_comments = """select table_schema, table_name, column_name, column_comment
                from information_schema.columns 
                """

    # Query7: Tables commentaries - number of rows

    query7_table_comments = """select table_schema, table_name, table_rows, table_comment
                from information_schema.tables  
                """


# ## 1) Discovering DataBase schema

# In[ ]:


df_schema = list(query_w_header(query1_schemas)[0])


# In[ ]:


#schema_name = input()
schema_name = data['database']['schema']


# ## 2) Info schema tables and columns

# #### 2.1 Obtain all the tables names and columns from information.schema.
# #### Schema name of the data base, example: mimiciii for MIMIC and classicmodels for the toy database (avoid systems folders)

# Two data frames are generated:
# 
#     1. df_table: containing all table names and columns names (all database)
#     2. df_table_pk: containing all table names and columns names of only primary and foreig keys.

# In[ ]:


if schema_name != "all":

    if database == "postgres":
        query2_all_tables = " ".join(
            [query2_all_tables, """and t.table_schema ='{}'"""]
        ).format(schema_name)
        query3_all_keys = " ".join(
            [query3_all_keys, """where kcu.constraint_schema ='{}'"""]
        ).format(schema_name)
        query5_col_stats = " ".join(
            [query5_col_stats, """and t.table_schema = '{}';"""]
        ).format(schema_name)
        query7_table_comments = " ".join(
            [query7_table_comments, """or u1.schemaname = '{}';"""]
        ).format(schema_name)

    elif database == "mysql":
        query2_all_tables = " ".join(
            [query2_all_tables, """where TABLE_SCHEMA ='{}' """]
        ).format(schema_name)
        query3_all_keys = " ".join(
            [query3_all_keys, """where TABLE_SCHEMA ='{}' """]
        ).format(schema_name)
        query5_col_stats = " ".join(
            [query5_col_stats, """where c.table_schema = '{}' """]
        ).format(schema_name)
        query6_col_comments = " ".join(
            [query6_col_comments, """where table_schema = '{}' """]
        ).format(schema_name)
        query7_table_comments = " ".join(
            [query7_table_comments, """where table_schema = '{}'; """]
        ).format(schema_name)

    elif database == "oracle":
        query2_all_tables = " ".join(
            [query2_all_tables, """where owner = '{}' """]
        ).format(schema_name)
        query3_all_keys = " ".join(
            [query3_all_keys, """AND uc_r.owner = '{}' """]
        ).format(schema_name)
        query5_col_stats = " ".join(
            [query5_col_stats, """WHERE a.owner = '{}' """]
        ).format(schema_name)
        query6_col_comments = " ".join(
            [query6_col_comments, """WHERE a.owner = '{}' """]
        ).format(schema_name)
        query7_table_comments = " ".join(
            [query7_table_comments, """where c.owner = '{}' """]
        ).format(schema_name)

else:
    pass


# In[ ]:


df_table = query_w_header(query2_all_tables)
df_table.columns = ["schema_name", "table_name", "column_name"]


# #### 2.2) If primary and foreig keys are used, the following query allows the selection of only those columns.

# In[ ]:


try:
    with tqdm(total=1) as progress:
        df_table_pk = query_w_header(query3_all_keys)
        progress.update()
    print(progress.format_interval(progress.format_dict["elapsed"]))
except:
    df_table_pk = pd.DataFrame()


# In[ ]:


df_table_pk


# In[ ]:


if database == "oracle":
    df_table_pk.columns = [
        "schema_name",
        "table_name",
        "column_name",
        "key",
        "key2",
        "target",
        "target_label",
    ]
    df_table_pk.drop_duplicates(inplace=True, ignore_index=True)
    df_table_pk2 = df_table_pk.dropna(axis=0)
    df_table_pk2.columns = [
        "schema",
        "from",
        "label",
        "key",
        "key2",
        "target",
        "target_label",
    ]
    df_table_pk2.reset_index(inplace=True, drop=True)
    df_table_pk2.drop_duplicates(inplace=True, ignore_index=True)
    df_table_pk.fillna(value=np.nan, inplace=True)

else:
    df_table_pk.columns = [
        "schema_name",
        "table_name",
        "column_name",
        "key2",
        "target",
        "target_label",
    ]
    df_table_pk.drop_duplicates(inplace=True, ignore_index=True)
    df_table_pk2 = df_table_pk.dropna(axis=0)
    df_table_pk2.columns = ["schema", "from", "label", "key", "target", "target_label"]
    df_table_pk2.reset_index(inplace=True, drop=True)
    df_table_pk2.drop_duplicates(inplace=True, ignore_index=True)
    df_table_pk.fillna(value=np.nan, inplace=True)


# In[ ]:


df_table_pk.head(3)


# #### 2.3) Obtaining first 10 rows of each table

# In[ ]:


list_tables = list(df_table.table_name.unique())


# In[ ]:


dict_schema_table = (
    df_table[["schema_name", "table_name"]].set_index("table_name").T.to_dict("list")
)


# In[ ]:


rows_tables = {}

with tqdm(total=len(list_tables)) as progress:
    for k, v in dict_schema_table.items():
        try:
            h = k
            j = "".join(v)
            df = pd.DataFrame()
            #df = query_wo_header(query4_first10.format(j, k))
            table_name = h + "_rows"
            rows_tables[table_name] = df
            progress.update()
        except:
            print("error ->", j)
            continue
        
        
print(progress.format_interval(progress.format_dict["elapsed"]))


# ## 3) Info columns per table (stats)

# #### General stats from all data base, number of tables, number of columns, amount of data.

# In[ ]:


with tqdm(total=1) as progress:
    df = query_wo_header(query5_col_stats)
    df.columns = [
        "schema_name", 
        "table_name", 
        "column_name", 
        "Dtype", 
        "nb_rows", 
        "null_count"
    ]
    progress.update()
print(progress.format_interval(progress.format_dict["elapsed"]))


# In[ ]:


with tqdm(total=1) as progress:
    for i in range(len(df)):
        if df.loc[i,"nb_rows"] != 0:
            df.loc[i, "percentage"] = ((df.loc[i,"nb_rows"]-df.loc[i,"null_count"]) * 100)/ df.loc[i,"nb_rows"]
            progress.update()
        else:
            df.loc[i,'percentage'] = 0
    print(progress.format_interval(progress.format_dict["elapsed"]))

df.fillna(value=np.nan, inplace=True)


# In[ ]:


df.head()


# ## 3.2 Generation of .CSV files    
#     1. Column.csv: table name, column name, nb of filled rows, data type, completion %, primary/foreign key, table related (target) , target column name, exmple of data, comments (from information_schema).
#     2. Table.csv: names, number of rows, number of columns, primary key, # tables related), comments

# #### 3.2.1 Column CSV generation:

# In[ ]:


def columns_data(df, df_table_pk, rows_tables, query6_col_comments):

    # generate the table backbone merging all columns stats (df) and keys information(df_table_pk)
    columns_csv = pd.merge(
        df, df_table_pk, how="left", on=["schema_name","table_name", "column_name"]
    )

    # generate the columns preview (summary)
    summary2_df = pd.DataFrame(columns=["table_name", "column_name", "example"])

    for i in rows_tables:
        columnas = list(rows_tables[i])
        for j in columnas:
            a = list(rows_tables[i][j].head(5).unique())
            dff7 = pd.DataFrame(
                {"table_name": [i[:-5]], "column_name": [j], "example": [a]}
            )
            summary2_df = summary2_df.append(dff7, True)

    # merge the columns preview with the first backbone
    columns_csv = columns_csv.merge(
        summary2_df, how="left", on=["table_name", "column_name"]
    )

    # obtain the columns commentaries
    try:
        df_table_comments = query_w_header(query6_col_comments)
        df_table_comments.columns = [
            "table_name",
            "column_name",
            "comments",
        ]
    except:
        df_table_comments = pd.DataFrame(
            columns=["table_name", "column_name", "comments"]
        )

    # merge the columns commentaries with table backbone
    columns_csv = columns_csv.merge(
        df_table_comments, how="left", on=["table_name", "column_name"]
    )
    columns_csv.fillna(value=np.nan, inplace=True)

    return columns_csv


# In[ ]:


columns_csv = columns_data(df, df_table_pk, rows_tables, query6_col_comments)


# In[ ]:


columns_csv.to_csv("columns_info.csv", index=False)


# #### 3.2.2 Table CSV generation:

# In[ ]:


def table_data(columns, df_table_pk):

    # obtain general table information through columns_csv analysis
    # tables name, nb of columns
    table_name = columns.groupby(["schema_name", "table_name"])["column_name"].count()
    table_name = pd.DataFrame(table_name)
    table_name.reset_index(inplace=True)

    # keys names
    keys_list = (
        columns.groupby(["schema_name", "table_name"])["key2"]
        .unique()
        .apply(list)
        .apply(lambda x: [i for i in x if str(i) != "nan"])
    )
    key_type = pd.DataFrame(keys_list)
    key_type.reset_index(inplace=True)

    # merging table name and nb of columns with key info
    table_csv = table_name.merge(key_type, how="left", on=["schema_name", "table_name"])

    # columns key name
    key_list2 = (
        df_table_pk.groupby(["schema_name", "table_name"])["column_name"]
        .unique()
        .apply(list)
        .apply(lambda x: [i for i in x if str(i) != "nan"])
    )
    key_name = pd.DataFrame(key_list2)
    key_name.reset_index(inplace=True)

    # merging of columns key name
    table_csv = table_csv.merge(key_name, how="left", on=["schema_name", "table_name"])

    # obtaining target tables
    target_table = (
        columns.groupby(["schema_name", "table_name"])["target"]
        .unique()
        .apply(list)
        .apply(lambda x: [i for i in x if str(i) != "nan"])
    )
    target = pd.DataFrame(target_table)
    target.reset_index(inplace=True)

    table_csv = table_csv.merge(target, how="left", on=["schema_name", "table_name"])

    # obtaining table comments
    try:
        table_rows = query_w_header(query7_table_comments)
        table_rows.columns = [
            "schema_name",
            "table_name",
            "table_rows",
            "table_comment",
        ]
    except:
        table_rows = pd.DataFrame(
            columns=["schema_name", "table_name", "table_rows", "table_comment"]
        )

    # merging to final table
    table_csv = table_csv.merge(
        table_rows, how="left", on=["schema_name", "table_name"]
    )

    # if an oracle database is analysed, final table will have 9 columns, for other databases table info will have 
    # 8 columns
    if len(table_csv.columns) == 8:

        table_csv.columns = [
            "schema_name",
            "table_name",
            "nb_columns",
            "keys",
            "keys_columns",
            "target_table",
            "nb_rows",
            "table_comments",
        ]
        cols = [
            "schema_name",
            "table_name",
            "nb_columns",
            "nb_rows",
            "keys",
            "keys_columns",
            "target_table",
           "table_comments"
        ]
        
        table_csv = table_csv[cols]
    elif len(table_csv.columns) == 9:

        table_csv.columns = [
            "schema_name",
            "table_name",
            "nb_columns",
            "keys",
            "keys2",
            "keys_columns",
            "target_table",
            "nb_rows",
            "table_comments",
        ]
        cols = [
            "schema_name",
            "table_name",
            "nb_columns",
            "nb_rows",
            "keys",
            "keys2",
            "keys_columns",
            "target_table",
            "table_comments",
        ]
        table_csv=table_csv[cols]

    return table_csv


# In[ ]:


table_csv = table_data(columns_csv, df_table_pk)


# In[ ]:


# Save to CSV
table_csv.to_csv('tables_info.csv', index = False)


# ## 4) First impression DB

# ### Number of tables and their relations

# In[ ]:


#Number of schemas/owners
print("Number of schemas =", df_table.schema_name.nunique())
# Number of tables
print("Number of tables =", df_table.table_name.nunique())
# Number of unique columns names
print("Number of unique columns = ", df_table.column_name.nunique())
# Number of columns (including keys)
print("Number of columns = ", df_table.column_name.count())


# In[ ]:


stats = pd.DataFrame(np.array([['N° Schemas', df_table.schema_name.nunique()],['N° Tables', df_table.table_name.nunique()],['N° unique columns', df_table.column_name.nunique()],['N° columns', df_table.column_name.count()]]),columns = ['','Count'])


# #### Fig 3. Relative amount of data per table
#     Each big square represents a table, inside each of the columns and if the columns are important keys.
#     Each a column is a key, then the target table is indicated.
#     Color scaled indicators of column filling in percentage (amount of not-null data / total number of rows).
#     Uncomment if needed.

# In[ ]:


#fig_3 = px.treemap(
#    columns_csv,
#    path=["schema_name", "table_name", "column_name", "key2"],
#    color="percentage",
#    color_continuous_scale="RdBu",
#    color_continuous_midpoint=50,
#)
#
#fig_3.show()


# ## 5) Table conections

# In[ ]:


if database == "postgres":
    df_table3 = (
        df_table_pk2.groupby(["schema", "from", "target"])["label"]
        .apply(list)
        .reset_index(name="keys")
    )

    df_table3["size"] = df_table3["keys"].apply(lambda x: len(x))

    df_table3 = df_table3.unstack().unstack().T
    
elif database == "oracle":
    df_table_pk2["combined"] = df_table_pk2.apply(
        lambda x: list([x["key"], x["key2"]]), axis=1
    )
    df_table3 = (
        df_table_pk2.groupby(["schema", "from", "target", "label"])["combined"]
        .apply(list)
        .reset_index(name="keys")
    )

    df_table3["size"] = df_table3["keys"].apply(lambda x: len(x))

    df_table3 = df_table3.unstack().unstack().T


# In[ ]:


df_table3


# ## Fig5. Interactive Dashboard

# The reconstruction of the relationships is done with all the columns or the primary/foreign keys, according to the chose (primary/all).
# Relationship is reconstructed usign Networkx and interactively ploted in Dash/Plotly.

# In[ ]:


G = nx.from_pandas_edgelist(df_table3, source="from", target="target", edge_attr=True)

pos = nx.layout.spring_layout(G)

elements = nx.cytoscape_data(G)

nodes = elements["elements"]["nodes"]

palette = sns.color_palette(
    None, len(nodes)
)  # TODO Change color for schema and not nodes!

col_swatch = list(palette.as_hex())


for i in range(len(nodes)):
    a = nodes[i]["data"]["id"]
    b = {"position": {"x": 1000 * pos[a][0], "y": 1000 * pos[a][1]}}
    nodes[i].update(b)
for i in range(len(nodes)):
    c = {"selectable": True}
    nodes[i]["data"].update(c)
edges = elements["elements"]["edges"]

for i in range(len(nodes)):
    d = {"color_node": col_swatch[i], "size": 20}
    nodes[i]["data"].update(d)

elements_dash = nodes + edges


# In[ ]:


stylesheets = [
    {
        "selector": "edge",
        "style": {"color": "data(keys)", "line-color": "lightgray", "width": 3},
    },
    {
        "selector": "node",
        "style": {
            "label": "data(id)",
            "font-size": 20,
            "background-color": "data(color_node)",
            "line-color": "data(color_node)",
            "width": "data(size)",
            "height": "data(size)",
        },
    },
]


# In[ ]:


styles = {"pre": {"border": "thin lightgrey solid", "overflowX": "scroll"}}


# In[ ]:


app = JupyterDash(__name__, external_stylesheets=[dbc.themes.SPACELAB])

#### LAYOUT DASHBOARD ####


app.layout = html.Div(
    [
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.H1(
                                    "Data Base exploration",
                                    className="bg-primary text-white text-center",
                                )
                            ]
                        )
                    ]
                )
            ]
        ),
        html.Div([dbc.Row([html.Br([])])]),
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(width=1),
                        dbc.Col(
                            dbc.Container(
                                [
                                    dbc.Row(
                                        [html.H5("Schema:", className="text-info")]
                                    ),
                                    dbc.Row(
                                        [
                                            dcc.Dropdown(
                                                id="dpdn5",
                                                multi=True,
                                                clearable=True,
                                                options=[
                                                    {
                                                        "label": name,
                                                        "value": name,
                                                    }
                                                    for name in list(
                                                        columns_csv[
                                                            "schema_name"
                                                        ].unique()
                                                    )
                                                ],
                                                style=dict(width="100%"),
                                            )
                                        ]
                                    ),
                                    dbc.Row([html.H5("Table:", className="text-info")]),
                                    dbc.Row(
                                        [
                                            dcc.Dropdown(
                                                id="dpdn6",
                                                multi=True,
                                                clearable=True,
                                                options=[
                                                    {
                                                        "label": name,
                                                        "value": name,
                                                    }
                                                    for name in list(
                                                        columns_csv[
                                                            "table_name"
                                                        ].unique()
                                                    )
                                                ],
                                                style=dict(width="100%"),
                                            )
                                        ]
                                    ),
                                    dbc.Row(
                                        [
                                            html.H5(
                                                "Select Network Layout:",
                                                className="text-info",
                                            )
                                        ]
                                    ),
                                    dbc.Row(
                                        [
                                            dcc.Dropdown(
                                                id="dpdn1",
                                                value="preset",
                                                clearable=False,
                                                options=[
                                                    {
                                                        "label": name.capitalize(),
                                                        "value": name,
                                                    }
                                                    for name in [
                                                        "breadthfirst",
                                                        "grid",
                                                        "random",
                                                        "circle",
                                                        "cose",
                                                        "concentric",
                                                    ]
                                                ],
                                                style=dict(width="100%"),
                                            )
                                        ]
                                    ),
                                    dbc.Row(
                                        [
                                            html.H5(
                                                "Select Neighbors Distance:",
                                                className="text-info",
                                            )
                                        ]
                                    ),
                                    dbc.Row(
                                        [
                                            dcc.Dropdown(
                                                id="dpdn2",
                                                value=1,
                                                clearable=False,
                                                options=[
                                                    {
                                                        "label": name,
                                                        "value": name,
                                                    }
                                                    for name in [
                                                        1,
                                                        2,
                                                        3,
                                                    ]
                                                ],
                                                style=dict(width="100%"),
                                            )
                                        ]
                                    ),
                                    dbc.Row(
                                        [
                                            html.Button(
                                                "Update",
                                                id="update-button",
                                                className="mr-3",
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            width=3,
                        ),
                        dbc.Col(
                            [
                                dbc.Container(
                                    cyto.Cytoscape(
                                        id="cytoscape-node",
                                        layout={"name": "preset"},
                                        style={
                                            "width": "100%",
                                            "height": "500px",
                                        },
                                        elements=elements_dash,
                                        stylesheet=stylesheets,
                                    )
                                )
                            ],
                            width=5,
                        ),
                        dbc.Col(
                            dbc.Container(
                                [
                                    dbc.Row(
                                        [
                                            html.H5(
                                                "Connection description:",
                                                className="text-info",
                                            )
                                        ]
                                    ),
                                    dbc.Row(
                                        [
                                            html.Pre(
                                                id="cytoscape-tapEdgeData-json",
                                                style=styles["pre"],
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            width=2,
                        ),
                        dbc.Col(width=1),
                    ]
                )
            ]
        ),
        html.Div([dbc.Row([html.Br([])])]),
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(width=1),
                        dbc.Col(
                            dbc.Container([
                                dbc.Row([html.H5("Database size:", className="text-info")]),
                                dbc.Row([
                            dash_table.DataTable(
                                id="table2",
                                columns=[{"name": i, "id": i} for i in stats.columns],
                                data=stats.to_dict("records"),
                            ),
                                ])]),
                            width=2),
                        
                            
                        dbc.Col(width=1),
                        dbc.Col(
                            dbc.Container([dcc.Graph(id="my-graph")]),
                            width=8,
                        ),
                        
                    ]
                )
            ]
        ),
        html.Div([dbc.Row([html.Br([])])]),
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(width=5),
                        dbc.Col(
                            html.H5(
                                "Table Preview",
                                className="text-info",
                            )
                        ),
                    ]
                )
            ]
        ),
        html.Div([dbc.Row([html.Br([])])]),
        html.Div(
            [
                dbc.Row(
                    [
                        dbc.Col(width=1),
                        dbc.Col(
                            dbc.Container(
                                [
                                    html.Table(
                                        id="table",
                                        className="dbc_light",
                                    )
                                ]
                            ),
                        ),
                        dbc.Col(width=1),
                    ]
                )
            ]
        ),
    ],
)


# Schema Table selection


@app.callback(Output("dpdn6", "options"), [Input("dpdn5", "value")])
def update_tables(selected_schema):

    if selected_schema == None:
        df = columns_csv
        return [
            {"label": name, "value": name} for name in list(df["table_name"].unique())
        ]

    else:
        df = df_table3[df_table3["schema"].isin(selected_schema)]
        return [
            {"label": name, "value": name} for name in list(df["from"].unique())
        ]


# -  Network Format using the dropdown menu


@app.callback(Output("cytoscape-node", "layout"), Input("dpdn1", "value"))
def update_layout(layout_value):
    if layout_value == "breadthfirst":
        return {"name": layout_value, "animate": True}
    else:
        return {"name": layout_value, "animate": True}


# Network Neighbors degrees


@app.callback(
    Output("cytoscape-node", "elements"),
    [Input("update-button", "n_clicks")],
    [
        State("cytoscape-node", "elements"),
        State("dpdn6", "value"),
        State("dpdn2", "value"),
    ],
)
def keep_nodes(_, elements, data, relation_value=1):
    if data is None:
        return no_update
    else:

        radius = relation_value
        D = ego_graph(
            G, data[0], radius=radius, center=True, undirected=False, distance=None
        )
        elements = nx.cytoscape_data(D)
        nodes = elements["elements"]["nodes"]

        for i in range(len(nodes)):
            a = nodes[i]["data"]["id"]
            b = {"position": {"x": 1000 * pos[a][0], "y": 1000 * pos[a][1]}}
            nodes[i].update(b)
        for i in range(len(nodes)):
            c = {"selectable": True}
            nodes[i]["data"].update(c)
        edges = elements["elements"]["edges"]
        for i in range(len(nodes)):
            d = {"color_node": col_swatch[i]}
            nodes[i]["data"].update(d)
        edges = elements["elements"]["edges"]
        elements_dash = nodes + edges
        return elements_dash
    return elements


@app.callback(
    Output("cytoscape-tapEdgeData-json", "children"),
    Input("cytoscape-node", "tapEdgeData"),
)
def displayTapNodeData(data):
    return json.dumps(data, indent=2)


@app.callback(Output("my-graph", "figure"), Input("dpdn6", "value"))
def update_nodes(data):
    
    if data is None:
        return no_update

    else:
        label = data[0]
        dff = columns_csv[columns_csv["table_name"] == label]
        fig = px.bar(
            dff,
            x="column_name",
            y="percentage",
            title="Percentage of filled data per column in: {}".format(label),
            color_discrete_sequence=["indianred", "RebeccaPurple", "darkgreen"],
        )
        fig.update_xaxes(categoryorder="total descending")
        fig.add_hline(y=100)
        fig.update_layout(template="plotly_white", showlegend=False)
        fig.update_traces(marker_line_color="RebeccaPurple", marker_line_width=1.5)
        return fig


# - Show first 10 lines of selected table


@app.callback(Output("table", "children"), 
              [Input("update-button", "n_clicks")],
              [
                State("dpdn5", "value"),
                State("dpdn6", "value")
              ]
            )

def table(_,schema_name,table_name):
    
    if schema_name is None:
        return no_update
    if table_name is None:
        return no_update
    
    else:
        query4_first10 = """select * from {}.{} WHERE ROWNUM <= 10""".format(schema_name[0],table_name[0])
        df = query_wo_header(query4_first10.format(j, k))
        
        
        
        #table = data[0] + "_rows"
        #dff5 = rows_tables[table]
        table = dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in sorted(df.columns)],
            data=df.to_dict("records"),
        )
        return table


app.run_server(port=8081)


# In[ ]:




