
  
    

        create or replace transient table jaffle_db.lineage.column_lineage_denormalized
         as
        (

/*

  Lineage Denormalized Table

  Description:
    This script is used to create a table that contains the lineage information for the access history data in Snowflake.
    The lineage information is created by joining the access history data with the information schema columns data.

  Steps:
    1. Create a CTE that denormalizes the access history data.
    2. Create a CTE that creates the recursive lineage information.
    3. Create a CTE that joins the recursive lineage information with the information schema columns data.
    4. Select the final lineage information and qualify the row number to get the latest lineage information.

  Final Output Columns:
    - table_catalog: The catalog of the table (the Database name in Snowflake)
    - table_schema: The schema of the table
    - table_name: The name of the table
    - column_name: The name of the column
    - fully_qualified_table_name: The fully qualified name of the table - table_catalog.table_schema.table_name
    - query_id: The ID of the query in the Snowflake Query History
    - query_text: The text of the query
    - query_hash: The hash of the query from the Snowflake Query History
    - query_start_time: The start time of the query
    - user_name: The name of the user who ran the query
    - source_object_id: The ID of the source object in this particular reference of the object in the lineage
    - source_object_name: The name of the source object in this particular reference of the object in the lineage
    - source_column_name: The name of the source column in this particular reference of the column in the lineage
    - source_column_base_or_direct: The type of the source column - base or direct
    - target_object_name: The name of the target object in this particular reference of the object in the lineage
    - target_column_name: The name of the target column in this particular reference of the column in the lineage
    - final_object_name: The final name of the object in the lineage - this is what we'll filter by
    - final_column_name: The final name of the column in the lineage - this is what we'll filter by
    - depth: The depth of the lineage information

*/with access_history_denormalized as (
    -- Denormalize the access history data
    select
        t.query_id,
        t.query_start_time,
        t.user_name,
        directSources.value:"objectId" as source_object_id,
        directSources.value:"objectName"::varchar as source_object_name,
        directSources.value:"columnName"::varchar as source_column_name,
        'direct' as source_column_base_or_direct,
        om.value:"objectName"::varchar as target_object_name,
        columns_modified.value:"columnName"::varchar as target_column_name
    from
        snowflake.account_usage.access_history t,
        -- Flatten the OBJECTS_MODIFIED column from a JSON array to a table
        lateral flatten(input => t.OBJECTS_MODIFIED) om,
        -- Flatten the columns array from the OBJECTS_MODIFIED column
        lateral flatten(input => om.value:"columns", outer => true) columns_modified,
        -- Flatten the directSources array from the columns array
        lateral flatten(input => columns_modified.value:"directSources", outer => true) directSources

    union all

    select
        t.query_id,
        t.query_start_time,
        t.user_name,
        baseSources.value:"objectId" as source_object_id,
        baseSources.value:"objectName"::varchar as source_object_name,
        baseSources.value:"columnName"::varchar as source_column_name,
        'base' as source_column_base_or_direct,
        om.value:"objectName"::varchar as target_object_name,
        columns_modified.value:"columnName"::varchar as target_column_name
    from
        snowflake.account_usage.access_history t,
        -- Flatten the OBJECTS_MODIFIED column from a JSON array to a table
        lateral flatten(input => t.OBJECTS_MODIFIED) om,
        -- Flatten the columns array from the OBJECTS_MODIFIED column
        lateral flatten(input => om.value:"columns", outer => true) columns_modified,
        -- Flatten the baseSources array from the columns array
        lateral flatten(input => columns_modified.value:"baseSources", outer => true) baseSources
),

recursive_lineage as (
    -- Create the recursive lineage information
    ----------------------------------------------
    -- This CTE will recursively join the access history data to get the full lineage information
    -- Starting from the last reference of the object in the lineage, i.e. the end of transformations
    -- And going back to the first reference of the object in the lineage
    -- Each time, we join the Target to the Source, to get one step of the lineage
    -- We do this recursively until we reach the first reference of the object in the lineage

    select
        ahd.query_id,
        ahd.query_start_time,
        ahd.user_name,
        ahd.source_object_id,
        ahd.source_object_name,
        ahd.source_column_name,
        ahd.source_column_base_or_direct,
        ahd.target_object_name,
        ahd.target_column_name,
        ahd.target_object_name as final_object_name,
        ahd.target_column_name as final_column_name,
        1 as depth
    from access_history_denormalized ahd

    union all

    select
        ahd.query_id,
        ahd.query_start_time,
        ahd.user_name,
        ahd.source_object_id,
        ahd.source_object_name,
        ahd.source_column_name,
        ahd.source_column_base_or_direct,
        r.source_object_name as target_object_name,
        r.source_column_name as target_column_name,
        r.final_object_name,
        r.final_column_name,
        r.depth + 1
    from recursive_lineage r
    join access_history_denormalized ahd
        on ahd.target_object_name = r.source_object_name
        and ahd.target_column_name = r.source_column_name
    where depth < 50 -- Limit the depth of the recursion to avoid infinite loops
),

access_history_by_info_schema_column as (
    -- Join the recursive lineage information with the information schema columns data
    -- This will give us the final lineage information with the fully qualified table names
    -- And the query text from the Snowflake Query History
    select
        col.table_catalog,
        col.table_schema,
        col.table_name,
        col.column_name,
        col.table_catalog || '.' || col.table_schema || '.' || col.table_name as fully_qualified_table_name,
        rl.query_id,
        qh.query_text,
        qh.query_hash,
        rl.query_start_time,
        rl.user_name,
        rl.source_object_id,
        rl.source_object_name,
        rl.source_column_name,
        rl.source_column_base_or_direct,
        rl.target_object_name,
        rl.target_column_name,
        rl.final_object_name,
        rl.final_column_name,
        rl.depth
    from snowflake.account_usage.columns as col
    left join recursive_lineage as rl
        on rl.final_object_name = fully_qualified_table_name
        and rl.final_column_name = col.column_name
    left join snowflake.account_usage.query_history as qh
        on rl.query_id = qh.query_id
    where col.table_catalog in ('JAFFLE_DB')
)

-- Select the final lineage information and qualify the row number to get the latest lineage information
-- This will give us the latest lineage information for each source and target column, based on the most recent query by hash and query_start_time
select
    *
from access_history_by_info_schema_column
qualify row_number() over (partition by source_column_base_or_direct, source_object_name, source_column_name, target_object_name, target_column_name, depth, query_hash order by query_start_time desc) <= 1
        );
      
  