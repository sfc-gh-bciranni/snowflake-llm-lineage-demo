
  
    

create or replace table jaffle_db.lineage.column_lineage_final
 as
(

with prompt as (
    -- Generate the formatted prompt
    select
        final_object_name,
        final_column_name,
        '#### Transformation Steps:\n\n' ||
        listagg(
            'Step ' || (max_depth - depth + 1)::varchar || '\n --- \n' ||
            '   - **User**: ' || user_name || '\n' ||
            '   - **Source Table**: ' || source_object_name || '\n' ||
            '   - **Source Column**: ' || source_column_name || '\n' ||
            '   - **Target Table**: ' || target_object_name || '\n' ||
            '   - **Target Column**: ' || target_column_name || '\n' ||
            '   - **Query Text**: ' || query_text || '\n\n'
        ) within group (order by depth desc) as transformation_steps,
        'Given the Below Column Lineage Documenation for the column, Write a concise user-friendly documentation describing how the column was transformed until the current state. Provide a short summary for the transformation of ONLY THIS COLUMN. Provide a 1-sentence concise human-readable explanation for each step, again only discussing this column. DO NOT EXPLAIN ANY OTHER COLUMNS UNLESS RELEVANT. Use Markdown formatting.' || '\n\n' ||
        '### Column Lineage Documentation for ' || final_column_name || '\n\n' ||
        '#### Final Column: ' || final_column_name || '\n' ||
        '- **Table**: ' || final_object_name || '\n\n' ||
        transformation_steps as llm_prompt
    from (
        select 
            query_id,
            query_start_time,
            coalesce(user_name, 'Not found') as user_name,
            coalesce(source_object_name, 'Not found - Imply from Query Text') as source_object_name,
            coalesce(source_column_name, 'Not Found - Imply from Query Text') as source_column_name,
            coalesce(target_object_name, 'Not Found - Imply from Query Text') as target_object_name,
            coalesce(target_column_name, 'Not Found - Imply from Query Text') as target_column_name,
            final_object_name,
            final_column_name,
            query_text,
            depth,
            max(depth) over (partition by final_column_name, final_object_name) as max_depth
        from jaffle_db.lineage.column_lineage_denormalized
        where 1=1
            -- and final_object_name = $table_name
            and source_column_base_or_direct = 'direct'
    )
    group by 1,2
)

select 
    final_object_name, 
    final_column_name,
    transformation_steps,
    llm_prompt, 
    snowflake.cortex.complete('llama3.1-8b', llm_prompt) as llm_response
from prompt
        );
      
  
