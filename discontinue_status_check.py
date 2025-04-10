from re import sub
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()
st.set_page_config(page_title="Discontinue Status Check", layout="wide")
st.title("Discontinue Status Check")

st.markdown("""
- :ledger: [Readme](https://github.com/cminor7/discontinue-status-check)
- :bookmark_tabs: [Step Errors](https://app.powerbi.com/groups/a7e0dd25-f63c-418b-8a8a-a6711510d0f8/reports/cfd54a87-1c98-467d-94b5-3ebb1007a90f/51f7ae20bba0dad1934f?experience=power-bi)
""")
st.divider()

#################### FILTERS ################################

st.subheader("Filters")
col1, col2 = st.columns(2)
with col1:
    sales_org_name = st.selectbox('sales org', ('GUS', 'GCAN', 'GGS'))
with col2:
    material = sub(r'[^a-zA-Z0-9-/\.]', '', st.text_input("material", "")) 

if material == "":
    st.stop()

sales_org = '0300' # set default to GUS
if sales_org_name == 'GCAN':
    sales_org = '2900'
elif sales_org_name == 'GGS':
    sales_org = '1045'

st.divider()
st.subheader("Results")

#################### MATERIAL STATUS CHECK ################################

query_status = f"""SELECT SALESORG, MATERIAL, SALES_STATUS, SHORT_DESCRIPTION FROM TERADATA.PRD_DWH_VIEW_LMT.MATERIAL_NORTHAMERICAN_V
    WHERE SALESORG = '{sales_org}'
    AND MATERIAL = '{material}'"""

df = session.sql(query_status)

if df.count() == 0:   
    st.write("material doesn't exist / wrong sales org :shrug:")
    st.stop()
elif df.collect()[0]['SALES_STATUS'] in ['DG', 'DV']:
    st.write("material is already in discontinued status")
elif df.collect()[0]['SALES_STATUS'] not in ['WV', 'WG']:
    st.write("WV/WG sales status condition: FAILED :x:")
else:
    st.write("while stock last status condition: PASSED :white_check_mark:")
st.dataframe(df.collect(), use_container_width=True, hide_index=True)

#################### MATERIAL STOCK CHECK ################################

exclude_plant = "''" 
if sales_org == '0300':
    exclude_plant = "SELECT PLANT FROM ISP.RA.DISCONTINUE_EXCLUDE_PLANTS"

stock_condition = "AVAILABLE_UNITS > 0"
if sales_org == '1045':
    stock_condition += " OR IN_QUALITY_INSP_UNITS > 0 OR OPEN_DELIVERY_QTY > 0 OR BLOCKED_UNITS > 0 OR UN_RESTRICTED_UNITS > 0"

query_atp = f"""SELECT MATERIAL_NO, PLANT_NO, 
    IN_QUALITY_INSP_UNITS, BLOCKED_UNITS, OPEN_DELIVERY_QTY, UN_RESTRICTED_UNITS,
    (AVAILABLE_UNITS + IN_QUALITY_INSP_UNITS) AS AVAILABLE_UNITS
    
    FROM TERADATA.PRD_DWH_VIEW_LMT.ATP_AVAILABLETOPROMISE_V AS ATP
    INNER JOIN 
    (
        SELECT PLANT FROM PUBLISH.GSCCE.PLANT_EDV 
        WHERE SALES_ORG = '{sales_org}'
        AND PLANT NOT IN ({exclude_plant})
    ) AS PLANT_V ON ATP.PLANT_NO = PLANT_V.PLANT

    WHERE MATERIAL_NO = '{material}' AND ({stock_condition})"""


df = session.sql(query_atp)

if df.count() > 0:  
    st.write("no stock condition: FAILED :x:")
    st.dataframe(df.collect(), use_container_width=True, hide_index=True)
else:
    st.write("no shippable inventory condition: PASSED :white_check_mark:")
    
#################### MATERIAL OPEN PO CHECK ################################

query_openpo = f"""SELECT DOCNUMBER, LINENUMBER, MATERIAL, DOCDATE, PLANT, SUPPLIER, ORDERQTY, OPENGRQTY
    FROM TERADATA.PRD_DWH_VIEW_LMT.PURCHASE_ORDER_OPEN_SKINNY2_V 
    WHERE MATERIAL = '{material}' AND PURCHASINGDOCTYPE NOT IN ('YB', 'UB')"""

df = session.sql(query_openpo)

if df.count() > 0:
    st.write("no open PO condition: FAILED :x:")
    st.dataframe(df.collect(), use_container_width=True, hide_index=True)
else:
    st.write("no open PO condition: PASSED :white_check_mark:")

#################### MATERIAL SUBMISSION CHECK ################################

query_submit = f"""SELECT ID, FIELD, NEW_VALUE, SALESORG, OLDCODE 
    FROM ISP.RA.CLEAR_TO_DISC_US_MEX 
    WHERE ID = '{material}' 
    AND (NEW_VALUE LIKE 'DG%' OR NEW_VALUE LIKE 'DV%') 
    AND FIELD <> 'MX Sales Status'"""

df = session.sql(query_submit)

if df.count() > 0:
    st.write("Material has been submitted for discontinue :cat2:")
    st.dataframe(df.collect()