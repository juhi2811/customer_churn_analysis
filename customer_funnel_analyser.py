from ctypes.wintypes import SIZE
from email.policy import default
from nis import cat
from turtle import title
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import random

# https://streamlit.io/
# https://docs.streamlit.io/
# https://docs.streamlit.io/library/api-reference

@st.cache


def read_in_data(region):
    """Read in data from a CSV file."""
    print("reading in region", region)
    df_all_regions = pd.read_parquet("/Users/juhic/Downloads/"+"data.parquet")
    df_subcat = pd.read_csv("/Users/juhic/Downloads/subcategories.csv")
    df_subcat["category_code"]= df_subcat["category_code"].astype(str)
    df_all_regions["Region"]=df_all_regions["Region"].apply(lambda x: x.split("DASHBOARD")[0].strip())
    if region!="All":
         df_all_regions=df_all_regions[df_all_regions["Region"]==region]
    df_all_regions["Category_code"] =df_all_regions["Category"].apply(lambda x: convert_subcategory(x))
    print("codes", set(list(df_all_regions["Category_code"].unique()))-set(df_subcat["category_code"].unique()))
    dict_subregions = {df_subcat["category_code"][i]: df_subcat["subcategory_name"][i] for i in range(len(df_subcat)) }
    dict_subregions['611P'] = 'Prisons'
    df_all_regions["subcategory_name"]= df_all_regions["Category_code"].apply(lambda x: dict_subregions.get(x,"unknown"))
    print("Finished legend stuff")
    df_all_regions["Bill Status"]=df_all_regions["Bill Status"].apply(lambda x: x.title())
    df_all_regions["Bill Status"]=df_all_regions["Bill Status"].replace("Estimate","Estimated")
    df_all_regions["Month"]=df_all_regions["Bill Month"].apply(lambda x: str(x).split("-")[1])
    df_all_regions["Year"]=df_all_regions["Bill Month"].apply(lambda x: str(x).split("-")[0])
    df_all_regions["Year_month"]=df_all_regions["Year"]+df_all_regions["Month"] 
    print("Finished year month stuff")

    df_all_regions["Balance_due_last_month"]=df_all_regions.groupby("Account #")["Balance Due"].shift(+1)
    print("finished balance due calculation")

    df_all_regions["paid_0"]=df_all_regions.apply(lambda dfx: ["1" if (dfx["Paid This Month"]==0.0 and dfx["Balance_due_last_month"]>0.0) else "0"][0],axis=1)
    df_all_regions["paid_0_sum"]=df_all_regions.groupby("Account #")["paid_0"].transform("sum")
    print("finished balance paid 0")
    return df_all_regions 
    
    
def read_in_account_data(account):
    df_all_regions = pd.read_parquet("/Users/juhic/Downloads/"+"full_data_ghana_2019_2021.parquet")
    df_all_regions["Account #"]=df_all_regions["Account #"].astype(str)
    df_all_regions = df_all_regions[df_all_regions["Account #"]==str(account)]
    df_all_regions["Bill Status"]=df_all_regions["Bill Status"].apply(lambda x: x.title())
    df_all_regions["Bill Status"]=df_all_regions["Bill Status"].replace("Estimate","Estimated")
    df_all_regions["Month"]=df_all_regions["Bill Month"].apply(lambda x: str(x).split("-")[1])
    df_all_regions["Year"]=df_all_regions["Bill Month"].apply(lambda x: str(x).split("-")[0])
    df_all_regions["Year_month"]=df_all_regions["Year"]+df_all_regions["Month"]
    df_all_regions["Balance Due"]=df_all_regions["Balance Due"].astype(float)
    df_all_regions["Paid This Month"]=df_all_regions["Paid This Month"].astype(float)
    df_all_regions["Account #"]=df_all_regions["Account #"].astype(str)
    df_all_regions["Balance_due_last_month"]=df_all_regions["Balance Due"].shift(+1)
    df_all_regions["due_vs_balance"]=df_all_regions["Balance_due_last_month"]/df_all_regions["Paid This Month"]
    df_all_regions["diff_bal_pay"]= df_all_regions["Balance_due_last_month"]- df_all_regions["Paid This Month"]
    df_all_regions["pay_vs_due"]=df_all_regions["Balance_due_last_month"]/df_all_regions["Paid This Month"]
    df_all_regions["average_due_vs_pay"]= df_all_regions.groupby(["Account #"])["diff_bal_pay"].transform(lambda x: x.mean())
    df_all_regions["%pay_due"]=df_all_regions["Paid This Month"]/df_all_regions["Balance_due_last_month"]
    return df_all_regions 


def read_in_data_custom(feature,filter_name,filter_value):
    """Read in data from a CSV file."""
    print("reading in region", region)
    df_all_regions = pd.read_parquet("/Users/juhic/Downloads/"+"full_data_ghana_2019_2021.parquet")
    df_subcat = pd.read_csv("/Users/juhic/Downloads/subcategories.csv")
    df_subcat["category_code"]= df_subcat["category_code"].astype(str)
    df_all_regions["Region"]=df_all_regions["Region"].apply(lambda x: x.split("DASHBOARD")[0].strip())
    if region!="":
         df_all_regions=df_all_regions[df_all_regions["Region"]==region]
    df_all_regions["Category_code"] =df_all_regions["Category"].apply(lambda x: convert_subcategory(x))
    print("codes", set(list(df_all_regions["Category_code"].unique()))-set(df_subcat["category_code"].unique()))
    dict_subregions = {df_subcat["category_code"][i]: df_subcat["subcategory_name"][i] for i in range(len(df_subcat)) }
    dict_subregions['611P'] = 'Prisons'
    df_all_regions["subcategory_name"]= df_all_regions["Category_code"].apply(lambda x: dict_subregions.get(x,"unknown"))
    print("Finished legend stuff")
    df_all_regions["Bill Status"]=df_all_regions["Bill Status"].apply(lambda x: x.title())
    df_all_regions["Bill Status"]=df_all_regions["Bill Status"].replace("Estimate","Estimated")
    df_all_regions["Month"]=df_all_regions["Bill Month"].apply(lambda x: str(x).split("-")[1])
    df_all_regions["Year"]=df_all_regions["Bill Month"].apply(lambda x: str(x).split("-")[0])
    df_all_regions["Year_month"]=df_all_regions["Year"]+df_all_regions["Month"] 
    print("Finished year month stuff")

    df_all_regions["Balance_due_last_month"]=df_all_regions.groupby("Account #")["Balance Due"].shift(+1)
    print("finished balance due calculation")

    df_all_regions["paid_0"]=df_all_regions.apply(lambda dfx: ["1" if (dfx["Paid This Month"]==0.0 and dfx["Balance_due_last_month"]>0.0) else "0"][0],axis=1)
    df_all_regions["paid_0_sum"]=df_all_regions.groupby("Account #")["paid_0"].transform("sum")
    print("finished balance paid 0")
    return df_all_regions 

def give_customer_bill_value(df):
    df["first_stage"]=list(df[df["stage"]=="All Customer bills"]["value"])[0]
    return df

def convert_subcategory(c):
    if not("P" in str(c) or str(c).lower()=="nan"):
        return str(int(float(c)))
    else:
        return str(c)

regions = ['ACCRA EAST - ACCRA CENTRAL', 'ACCRA EAST - ACCRA EAST',
       'ACCRA EAST - ACCRA NORTH', 'ACCRA EAST - ACCRA NORTHEAST',
       'ACCRA EAST - ADENTA', 'ACCRA EAST - DODOWA', 'ACCRA EAST - DOME',
       'ACCRA EAST - KWABANYA', 'ACCRA EAST - NUNGUA',
       'ACCRA EAST - TESHIE', 'ACCRA WEST - Accra West',
       'ACCRA WEST - Amasaman', 'ACCRA WEST - Bortianor',
       'ACCRA WEST - DANSOMAN NORTH', 'ACCRA WEST - Dansoman South',
       'ACCRA WEST - Kasoa', 'ACCRA WEST - NORTH WEST 1',
       'ACCRA WEST - North West 2', 'ACCRA WEST - Nyanyano',
       'ACCRA WEST - ODORKOR', 'ACCRA WEST - Sowutuom',
       'ACCRA WEST - WEIJA', 'TEMA - ADA', 'TEMA - ASHIAMAN EAST',
       'TEMA - ASHIAMAN WEST', 'TEMA - BATSONA', 'TEMA - Gbetseli',
       'TEMA - KPONG', 'TEMA - PRAMPRAM', 'TEMA - SAKUMONO',
       'TEMA - SANTEO', 'TEMA - TEMA CENTRAL', 'TEMA - TEMA INDUSTRIAL',
       'TEMA - TEMA WEST','All']
components = ["Bill Status","Meter Status","Account Status","Category"]

tab1, tab2, tab3 = st.tabs(["Region level", "Account level", "Custom stats"])

with tab1:
    # set a title
    st.markdown('# GWP Bill payment analysis Dashboard')
    col1, col2 = st.columns([1, 1])
    with col1:
        region = st.selectbox('Pick region', regions) 
    with col2:
        cat_option = st.selectbox('Pick category', components)      
    
    df_filtered = read_in_data(region)  
    #print(df_filtered["paid_0_sum"].unique()[:10])
    df_funnel=pd.DataFrame()
    df_funnel["Meter Status"]=["Working","Faulty","Not working","Unknown"]
    df_funnel["Meter_map"]=["W","F","N","None"]
    df_funnel["All Customer bills"]=df_funnel["Meter_map"].apply(lambda x: len(df_filtered[df_filtered["Meter Status"].astype(str)==str(x)]))
    df_funnel["Actively billed"]=df_funnel["Meter_map"].apply(lambda x: len(df_filtered[(df_filtered["Meter Status"].astype(str)==str(x)) & (df_filtered["Status"]=="ACTB")]))
    df_funnel["Not paid this month"]=df_funnel["Meter_map"].apply(lambda x: len(df_filtered[(df_filtered["Meter Status"].astype(str)==str(x)) & (df_filtered["Status"]=="ACTB") & (df_filtered["paid_0_sum"].str.contains("11111"))]))
    # #010104051982, 010104050321, 010104050321
    stages = ["All Customer bills", "Actively billed","Not paid this month"]
    df_full=pd.DataFrame()
    df=pd.DataFrame()
    df["stage"]=stages
    for m in list(df_funnel["Meter Status"].unique()):
        df_append=df
        df_append["Meter Status"]=m
        df_append["value"]=df_append["stage"].apply(lambda x: df_funnel[(df_funnel["Meter Status"]==m)][x])
        df_full=df_full.append(df_append,ignore_index=True)
    df_full=df_full.groupby(["Meter Status"]).apply(give_customer_bill_value)
    df_full["percent_init"]=100*(df_full["value"]/df_full["first_stage"])
    df_full["percent_init"]=df_full["percent_init"].apply(lambda x: round(x,2))
    fig_funnel = px.funnel(df_full, x='value', y='stage', color="Meter Status",text='percent_init',title=f"Customer funnel: {region}")
    st.plotly_chart(fig_funnel)
    ####BAR PLOT
   

    if cat_option=="Account Status":
        cat_option="Status"
    else:
        if cat_option == "Category":
            cat_option = "subcategory_name"
        else:
            cat_option=cat_option
    df_week_month = df_filtered.groupby(["Year_month","Meter Status"]).count().reset_index()
    df_time_fault = pd.melt(df_week_month, id_vars=["Year_month","Meter Status"], value_vars=["Paid This Month"], value_name="count")
    df_time_fault["Meter Status"]=df_time_fault["Meter Status"].replace(["None",None],["Unknown"]*2)
    df_time_fault= df_time_fault[df_time_fault["Meter Status"]!="W"]
    print("Time fault: ")
    fig_time = px.line(df_time_fault, x="Year_month", y="count",
                             color="Meter Status", title="Number of faulty/not working meters: "+ region, markers="o"
                            )
    fig_time.update_layout(xaxis=dict(showgrid=False),
              yaxis=dict(showgrid=False))                      
    st.plotly_chart(fig_time)

    df_count=df_filtered.groupby(cat_option).count().reset_index()
    fig1 = px.bar(df_count, x=cat_option, y='Paid This Month')
    fig1.update_layout(title="Count per category")
    fig1.update_layout(xaxis=dict(showgrid=False),
              yaxis=dict(showgrid=False))  
    # fig2=px.figure() 
    #fig2=px.box(survival_rate_stat, x='rating_int', y='Paid This Month')
    
    # show the chart on the DB
    st.plotly_chart(fig1)
with tab2:
    st.markdown('# Account level bill payments')
    account_in = st.text_input('Enter some text. (e.g.: 30971351451, 31070309221 or 10104350201)')
    if account_in == "":
        account=10104350201
    else:
        account= account_in


    df_output=read_in_account_data(account)
    # df_output_1["mean_%pay_due"]=df_output_1["%pay_due"].mean()

    threshold_in = st.text_input('Enter fraction threshold for payment (default: 0.5)')
    if threshold_in=="":
        threshold=0.5
    else:
        threshold=float(threshold_in)
    df_output["low_pay"]=df_output["%pay_due"].apply(lambda x: [1.0 if x<= threshold else 0.0][0])
    df_output["count_low_pay"]=df_output["low_pay"].sum()
    df_output["count_account"]=len(df_output)#groupby(["Account #"])["Bill Status"].transform(lambda x: x.count())
    df_output["count_account"]=df_output["count_account"]-1
    df_output["%low_pay_count"]=100*df_output["count_low_pay"]/df_output["count_account"]
    df_to_plot = df_output#[df_output_1['Account #'] == str(account)]#"30971351451"], 31070309221
    percentage_of_low_pay=list(df_to_plot["%low_pay_count"])[0]

    df_selected = df_to_plot[["Account #", "Year_month", "Paid This Month","Balance Due", "Bill Status","Meter Status","Balance_due_last_month"]]
    df_wide = pd.melt(df_selected, id_vars=['Account #', "Year_month","Meter Status"], value_vars=["Paid This Month","Balance_due_last_month"])

    fig2 = px.line(df_wide, x = 'Year_month', y = 'value', color = 'variable')
    fig2.update_layout(xaxis_showgrid=False, yaxis_showgrid=False)
    fig3 = px.scatter(df_wide, x = 'Year_month', y = 'value', color = 'variable', symbol = 'Meter Status')
    fig3.update_traces(marker_size=10)

    figt = go.Figure(data=fig2.data + fig3.data)
    figt.update_layout(title="Percentage of payments for account "+ str(account)+" lower than threshold: "+str(percentage_of_low_pay))
    figt.update_layout(xaxis=dict(showgrid=False),yaxis=dict(showgrid=False))  
    st.plotly_chart(figt)
with tab3:
    # set a title
    st.markdown('# GWP Bill payment analysis Dashboard')
    features = ["Paid This Month","Balance_due_last_month", "Balance Due Last Month,"]
    col1, col2 = st.columns([1, 1])
    with col1:
        region = st.selectbox('Pick feature', regions) 
    with col2:
        cat_option = st.selectbox('Pick category', components)      
    
    df_filtered = read_in_data(region)  
    #print(df_filtered["paid_0_sum"].unique()[:10])
    df_funnel=pd.DataFrame()
    df_funnel["Meter Status"]=["Working","Faulty","Not working","Unknown"]
    df_funnel["Meter_map"]=["W","F","N","None"]
    df_funnel["All Customer bills"]=df_funnel["Meter_map"].apply(lambda x: len(df_filtered[df_filtered["Meter Status"].astype(str)==str(x)]))
    df_funnel["Actively billed"]=df_funnel["Meter_map"].apply(lambda x: len(df_filtered[(df_filtered["Meter Status"].astype(str)==str(x)) & (df_filtered["Status"]=="ACTB")]))
    df_funnel["Not paid this month"]=df_funnel["Meter_map"].apply(lambda x: len(df_filtered[(df_filtered["Meter Status"].astype(str)==str(x)) & (df_filtered["Status"]=="ACTB") & (df_filtered["paid_0_sum"].str.contains("11111"))]))
    # #010104051982, 010104050321, 010104050321
    stages = ["All Customer bills", "Actively billed","Not paid this month"]
    df_full=pd.DataFrame()
    df=pd.DataFrame()
    df["stage"]=stages
    for m in list(df_funnel["Meter Status"].unique()):
        df_append=df
        df_append["Meter Status"]=m
        df_append["value"]=df_append["stage"].apply(lambda x: df_funnel[(df_funnel["Meter Status"]==m)][x])
        df_full=df_full.append(df_append,ignore_index=True)
    df_full=df_full.groupby(["Meter Status"]).apply(give_customer_bill_value)
    df_full["percent_init"]=100*(df_full["value"]/df_full["first_stage"])
    df_full["percent_init"]=df_full["percent_init"].apply(lambda x: round(x,2))
    fig_funnel = px.funnel(df_full, x='value', y='stage', color="Meter Status",text='percent_init',title=f"Customer funnel: {region}")
    st.plotly_chart(fig_funnel)
st.stop() 
