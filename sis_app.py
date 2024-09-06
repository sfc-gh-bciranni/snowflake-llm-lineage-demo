# Import python packages
import streamlit as st
import pandas as pd

from typing import Any
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout='wide')

# Write directly to the app
st.title("❄️ Snowflake LineageAI Demo 📚")
st.write(
    """Select a **Schema**, **Table**, and **Column** to Explore ✨ AI-Generated ✨ Docs on how they were transformed, column by column!
    """
)

def get(key: str) -> Any:
    return st.session_state[key]

def set(key: str, val: Any) -> None:
    st.session_state[key] = val
    

# Get the current credentials
session = get_active_session()

# defaults
if 'tables' not in st.session_state:
    st.session_state.tables = session.table('information_schema.tables').to_pandas()
    st.session_state.tables = st.session_state.tables.loc[st.session_state.tables.TABLE_SCHEMA != 'INFORMATION_SCHEMA', ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME']]

if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = st.session_state.tables.TABLE_SCHEMA.sort_values().iloc[0]

if 'table_info' not in st.session_state:
    st.session_state.table_info = session.table('lineage.table_lineage_final').to_pandas()

if 'lineage_denormalized' not in st.session_state:
    st.session_state.lineage_denormalized = session.table('lineage.column_lineage_denormalized').to_pandas()

if 'lineage_descriptions' not in st.session_state:
    st.session_state.lineage_descriptions = session.table('lineage.column_lineage_final').to_pandas()

# options
schema_options = get('tables').TABLE_SCHEMA.sort_values().unique()

selected_schema = st.selectbox(
    label='Select a Schema',
    options=schema_options,
    key='selected_schema'
)

tables = get('tables')
table_options = tables.loc[tables.TABLE_SCHEMA == get('selected_schema'), 'TABLE_NAME'].sort_values().unique()

st.markdown('---')

table_col, _, columns_col = st.columns([5,.5,5])

with table_col:
    st.subheader('Table Documentation')

    selected_table = st.selectbox(
        label='Select a Table',
        options=table_options,
        key='selected_table'
    )
    
    tables = get('table_info')
    table_row = tables.loc[tables.FINAL_OBJECT_NAME == f'JAFFLE_DB.{selected_schema}.{selected_table}'].iloc[0]
    
    ai_description, transformations = st.tabs(['✨ Documentation AI', 'Transformations Overview'])
    
    with ai_description:
        with st.container(height=600, border=True):
            st.write(table_row.LLM_RESPONSE)
    
    with transformations:
        with st.container(height=600, border=True):
            st.code(table_row.TRANSFORMATION_STEPS)

with columns_col:
    st.subheader('Columns Documentation')
    
    ld = get('lineage_denormalized')
    ld_tbl = ld.loc[(ld.FINAL_OBJECT_NAME == f'JAFFLE_DB.{selected_schema}.{selected_table}')]
    
    selected_column = st.selectbox(
        label='Select a Column',
        options=ld_tbl.FINAL_COLUMN_NAME.unique(),
        key='selected_column'
    )

    desc = get('lineage_descriptions')
    col_row = desc.loc[(desc.FINAL_OBJECT_NAME == f'JAFFLE_DB.{selected_schema}.{selected_table}') & (desc.FINAL_COLUMN_NAME == selected_column)].iloc[0]

    ai_description, transformations, all_steps = st.tabs(['✨ Documentation AI', 'Transformations Overview', 'All Lineage Steps'])
    
    with ai_description:
        with st.container(height=600, border=True):
            st.write(col_row.LLM_RESPONSE)
    
    with transformations:
        with st.container(height=600, border=True):
            st.code(col_row.TRANSFORMATION_STEPS)


    with all_steps:
        with st.container(height=600, border=True):
            st.dataframe(ld_tbl.loc[ld_tbl.COLUMN_NAME==selected_column].sort_values(by='DEPTH'))
