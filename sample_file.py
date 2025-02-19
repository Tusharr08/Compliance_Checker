3: import sys
4: 
5: sys.path.append("/Workspace/Repos/Supply_Chain/gp_btp_repo")
6: 
7: try:
8:     # First attempt to import from the common path
9:     from common.common_utils import *
10: except ImportError:
11:     # If the first import fails, append the alternative path and try again
12:     sys.path.append("/Workspace/Users/d44c1af5-e99e-4010-8302-464e74d13ba1/.bundle/BTP/files")
13:     try:
14:         from common.common_utils import *
15:     except ImportError:
16:         # If the second import fails, raise an ImportError
17:         raise ImportError("Could not import 'common.common_utils' from any of the provided paths.")
18: from pyspark.sql.functions import col, lit, when
19: source_df = spark.sql(f"""
20:     SELECT *
21:     FROM {gp_catalog}.{pgaero_sch}.aero_pkg_msd_hist
22:     WHERE current_month_cycle = (
23:         SELECT MAX(current_month_cycle)
24:         FROM {gp_catalog}.{pgaero_sch}.aero_pkg_msd_hist
25:         WHERE current_month_cycle != (
26:             SELECT DISTINCT current_month_cycle
27:             FROM {gp_catalog}.{pgaero_sch}.aero_pkg_msd_rpt
28:         )
29:     )
30: """)
31: # Load the target data into a DataFrame
32: target_df = spark.sql(f"SELECT * FROM {vgp_catalog}.{sot_aro}.aero_pkg_msd_rpt")
33: # Perform the join operation
34: merged_df = target_df.alias("b").join(
35:     source_df.alias("a"),
36:     (col("b.row_ref2") == col("a.row_ref2")) | (col("b.row_ref1") == col("a.row_ref1")),
37:     "left"
38: )
39: # Update the target DataFrame with values from the source DataFrame
40: updated_df = merged_df.select(
41:     col("b.*"),
42:     when(col("a.load_date").isNotNull(), col("a.load_date")).otherwise(col("b.load_date")).alias("load_date"),
43:     lit("Update").alias("upd_ins_flag"),
44:     col("a.root_cause1_l1").alias("root_cause1_l1"),
45:     col("a.root_cause1_l2").alias("root_cause1_l2"),
46:     col("a.root_cause2_l1").alias("root_cause2_l1"),
47:     col("a.root_cause2_l2").alias("root_cause2_l2"),
48:     col("a.root_cause3_l1").alias("root_cause3_l1"),
49:     col("a.root_cause3_l2").alias("root_cause3_l2"),
50:     col("a.root_cause1_perc_allocation").alias("root_cause1_perc_allocation"),
51:     col("a.root_cause2_perc_allocation").alias("root_cause2_perc_allocation"),
52:     col("a.root_cause3_perc_allocation").alias("root_cause3_perc_allocation")
53: )
54: # Write the updated DataFrame back to the target table
55: updated_df.write.mode("overwrite").saveAsTable(f"{vgp_catalog}.{sot_aro}.aero_pkg_msd_rpt")
56: email=spark.sql("select current_user()").collect()[0][0]
57: email
58: tbl_list=spark.sql(f"""select distinct table_name from vgpd.information_schema.tables where table_schema='sot_btp' and  table_owner='abhishek.pal@ge.com' and table_name like '%tmp%'""").collect()
59: for t in tbl_list:
60:     print(f'dropping table {t[0]}')    
61:     spark.sql(f"DROP TABLE vgpd.sot_btp.{t[0]}") 
62: count_validation = spark.sql(f"""select 'Target',count(*) from  {vgp_catalog}.{sot_btp}.{v_tgt_name} where source_name='{v_source_name}'
63: union all
64: select 'Source',count(*) from {fed_catalog}.{v_sot_schema}.{v_tgt_name} where source_name='{v_source_name}'""")
65:  
66: display(count_validation)
67: ## using one specific coloum
68: # except query
69: ## to view the count 
70: 
71: spark.sql(f""" select count(*) from {vgp_catalog}.{sot_btp}.{v_tgt_name}_{v_source_name}_tmp """ ).display()
72: 
73: # %sql
74: # select 'tgt' as target, count(*) from vinp.ing_sierra.HR_LOCATIONS_ALL
75: # union all
76: # select 'src' as source, count(*) from gpfp.ing_pnsrop01_sierra.HR_LOCATIONS_ALL where hvr_is_deleted = 0
77: # to view count of specific source names in table
78: #  %sql
79: # select 'GPFP',source_name,count (*) from gpfp.sot_supply_chain.btp_ap_holds_psot group by source_name;
80: #for writting data in tgt table from tmp table
81: df = spark.table(f"{vgp_catalog}.{sot_btp}.{ v_tgt_name }_{ v_source_name }_tmp")  
82: df.write.mode("overwrite").option("replaceWhere", f"source_id = {v_source_id}").format("delta").saveAsTable(f"{vgp_catalog}.{sot_btp}.{v_tgt_name}")
83:  
84: spark.sql(f"""
85:          --{"45342173683:All Files/Monthly Report from Brokers/2021-2023/_Expeditors > EXPEDITORS_October_2020_Europe.xlsx","131009112339:All Files/Monthly Report from Brokers/2020/_Expeditors > EXPEDITORS_October_2020_Europe.xlsx"}
86: 
87: 
88:        --131009112339:All Files/Monthly Report from Brokers/2020/_Expeditors > EXPEDITORS_October_2020_Europe.xlsx","45342173683:All Files/Monthly Report from Brokers/2021-2023/_Expeditors > EXPEDITORS_October_2020_Europe.xlsx"
89:           
90:           
91:            select  concat(     '{{',     concat_ws(',', sort_array(collect_list('"' || parent_folder || ':' || folder_path || ' > ' || file_name || '"'),true)),     '}}' ) AS formatted_output
92: from vgpd.sot_btp.lgs_brk_saving_europe_stg where entry_number = '10823720279000300'  and saving_amount = '268.332300000000000000'
93:  """).display()
94: spark.sql("""
95: CREATE OR REPLACE TEMPORARY FUNCTION INITCAP_custom(input_str STRING)
96: RETURNS STRING
97: RETURN
98:   SELECT
99:   case when input_str is null then null else
100:     concat_ws(
101:       '',
102:     transform(
103:           regexp_extract_all(
104:                 input_str, 
105:                 r'([\p{L}0-9]*[^\p{L}0-9]*)',
106:                 0) , x-> INITCAP(x)
107:           )
108:       ) end
109:     AS result
110:   """)
111: spark.sql(
112:     f"""
113:     create or replace temporary function isNumeric(n string)
114:     returns Boolean
115:     return
116:     select
117:     case
118:         when n is null then true
119:         when n::decimal(38,18) is not null then true
120:     else false end as flg
121:     """
122: )
123: spark.sql(f""" select * from  vgpd.sot_btp.lgs_projects_outbound_cost_LGS_tmp1 limit 10""").display()
