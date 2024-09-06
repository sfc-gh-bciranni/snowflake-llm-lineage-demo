
  
    

create or replace table jaffle_db.lineage.table_lineage_final
 as
(

with table_lineage as (
    select
        final_object_name
        , query_text
        , query_start_time
        , depth
        , max(depth) over (partition by final_object_name) as max_depth
    from jaffle_db.lineage.column_lineage_denormalized
    qualify row_number() over (partition by final_object_name, query_hash order by query_start_time desc) = 1
)

, prompt as (
    -- Generate the formatted prompt
    select
        final_object_name
        , '#### Transformations' || '\n\n' || listagg('Step ' || (max_depth - depth + 1)::varchar || '\n --- \n' || query_text || '```', '--- \n\n') within group (order by depth desc) as transformation_steps
        , 'You are an AI Agent tasked with describing all Operations done on a table. Given a list of transformations, you are to provide a concise, user friendly documentation.' || '\n\n' ||
        'Table: ' || final_object_name || '\n\n' || transformation_steps as llm_prompt
    from table_lineage
    group by 1
)

select
    final_object_name
    , transformation_steps
    , llm_prompt
    , snowflake.cortex.complete('llama3.1-8b', llm_prompt) as llm_response
from prompt
        );
      
  
