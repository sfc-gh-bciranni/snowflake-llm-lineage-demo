# snowflake-llm-lineage-demo

Generate AI Documentation based on Snowflake's Access History View!

![image](https://github.com/user-attachments/assets/292163cf-7578-49c0-9646-263870d8fe75)

---

This Repo contains 3 main tables:
- `column_lineage_denormalized`
  - We recursively construct a table of `source --> destination`, like an edge graph, that tells us which column transforms into what.
- `column_lineage_final`
  - We use this recursively created table, and aggregate together all transformations for a column, from source to final destination.
  - We can then put these transformations through a Snowflake Cortex LLM and have it explain the transformations to us!
- `table_lineage_final`
  - Similar to the above two tables, we'll grab the lineage of Table-Level transformations, and pass them through Snowflake Cortex LLMs.

---

Finally, `sis_app.py` will get you started with a Streamlit-in-Snowflake app to visualize and interact with the lineage.

![image](https://github.com/user-attachments/assets/c5fd1938-8b28-48bb-8e1d-453d8dc5f806)

---

As a final note, the data for this is based on DBT's "Jaffle Shop" Starter Project, and is entirely fictional. If you want to set up an identical database, take a look at the repo [here](https://github.com/dbt-labs/jaffle-shop-classic/tree/main)!
