import pandas as pd
import streamlit as st
import plotly.express as px
import sqlite3
from datetime import datetime
import streamlit_authenticator as stauth
from streamlit_option_menu import option_menu
import os
import sqlalchemy as sa
from sqlalchemy import text

# initiate connection & set up db due to size limitation on GitHub
def db_init():
    conn = sqlite3.connect('data.db')

    buyer = pd.read_csv('buyer_info.csv', low_memory=False)
    supplier = pd.read_csv('buyer_info.csv', low_memory=False)
    transaction = pd.read_pickle('transaction.pkl')

    buyer.to_sql('buyer_info', conn, if_exists='replace', index=False)
    supplier.to_sql('supplier_info', conn, if_exists='replace', index=False)
    transaction.to_sql('transaction', conn, if_exists='replace', index=False)
    
    return conn

#if 'data.db' not in os.listdir():
#    conn = db_init()
#else:
#    conn = sqlite3.connect('data.db')

password = 'RiceTrade%40123'
uri = f'''postgresql://postgres.bgcbdtswsqcqrokngeba:{password}@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres'''

engine = sa.create_engine(uri)
conn = engine.connect()

st.set_page_config(
    page_title='Rice Global Trade Handbook | Dashboard',
    layout="wide"
    )

if 'password_correct' in st.session_state:
    with st.sidebar:
        st.write(f"Welcome *{st.session_state['name']}*")
else:
    st.error('Please log in first')
    st.stop()   

hide_streamlit_style = """
                <style>
                div[data-testid="stToolbar"] {
                visibility: hidden;
                height: 0%;
                position: fixed;
                }
                div[data-testid="stDecoration"] {
                visibility: hidden;
                height: 0%;
                position: fixed;
                }
                div[data-testid="stStatusWidget"] {
                visibility: hidden;
                height: 0%;
                position: fixed;
                }
                #MainMenu {
                visibility: hidden;
                height: 0%;
                }
                header {
                visibility: hidden;
                height: 0%;
                }
                footer {
                visibility: hidden;
                height: 0%;
                }
                </style>
                """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

with st.sidebar:          
    selected = option_menu("Main Menu", ["Dashboard", 'Data update'], 
                           icons=['clipboard-data', 'database-add'], 
                           menu_icon="list", default_index=0)
    if selected == 'Data update':
        st.switch_page('pages/update.py')

@st.cache_data
def com_name_words(com_df):
    separated_words = [i.split(' ') for i in com_df['name'].unique().tolist()]
    words = []
    for i in separated_words:
        words += i

    clean_words = []
    for i in words:
        temp = ''.join(char for char in i.strip() if char.isalnum())
        if temp != '':
            clean_words.append(temp)
    clean_words = list(set(clean_words))
    return clean_words

@st.cache_data
def fetch_transaction(connection=conn):
    query = '''
        SELECT * 
        FROM "transaction_v2";
    '''
    data = pd.read_sql(query, conn)
    
    data['ACTUAL ARRIVAL DATE'] = pd.to_datetime(data['ACTUAL ARRIVAL DATE'], format='mixed')
    mintime = data['ACTUAL ARRIVAL DATE'].min().to_pydatetime()
    maxtime = data['ACTUAL ARRIVAL DATE'].max().to_pydatetime()
    
    return data, mintime, maxtime

@st.cache_data
def fetch_com(connection=conn):
    query = '''
        SELECT * 
        FROM "company_info_v2";
    '''
    data = pd.read_sql(query, conn)
    
    return data

def aggregate_filter(df, com, trading_side, transaction, volume, word_filter, tag):
    if tag == 'all':
        df = df.copy()
    else:
        df = df[df['tag'] == tag].copy()
    
    trans, vols, notify = {}, {}, {}
    for side in trading_side:
        trans[side] = df.groupby([side])['WEIGHT (MT)'].count().reset_index().rename(columns={'WEIGHT (MT)':'transaction_no_as_'+side})
        trans[side].rename(columns={side:'comName'}, inplace=True)
        vols[side] = df.groupby([side])['WEIGHT (MT)'].sum().reset_index().rename(columns={'WEIGHT (MT)':'volume_as_'+side})
        vols[side].rename(columns={side:'comName'}, inplace=True)
    
    trans_all = pd.DataFrame()
    for k, v in trans.items():
        if k == 'SUPPLIER':
            v['volume_as_SUPPLIER'] = 0 - v['volume_as_SUPPLIER']
        if trans_all.empty:
            trans_all = v
        else:
            trans_all = trans_all.merge(v, how='outer', on='comName')
    
    vols_all = pd.DataFrame()
    for k, v in vols.items():
        if vols_all.empty:
            vols_all = v
        else:
            vols_all = vols_all.merge(v, how='outer', on='comName')
    
    tran_cols = ['transaction_no_as_'+side for side in trading_side]
    vol_cols = ['volume_as_'+side for side in trading_side]    
    
    trans_all['total_transaction_no'] = trans_all[tran_cols].sum(axis=1)
    vols_all['total_volume'] = vols_all[vol_cols].sum(axis=1)
    
    buyer = com.merge(trans_all, how='left', left_on='name', right_on='comName')
    buyer = buyer.merge(vols_all, how='left', left_on='name', right_on='comName')
    
    # get notify info
    notify_info = df.merge(com, how='left',left_on='NOTIFY PARTY NAME', right_on='name')
    notify_info.rename(columns={'address':'Notify address',
                                'phone':'Notify phone',
                                'email':'Notify email'}, inplace=True)
    
    notify_name = notify_info.groupby(['BUYER'])['NOTIFY PARTY NAME'].apply(set).apply(list).reset_index()
    notify_name.rename(columns={'NOTIFY PARTY NAME':'Notify name'}, inplace=True)
    notify_adress = notify_info.groupby(['BUYER'])['Notify address'].apply(set).apply(list).reset_index()
    notify_phone = notify_info.groupby(['BUYER'])['Notify phone'].apply(set).apply(list).reset_index()
    notify_email = notify_info.groupby(['BUYER'])['Notify email'].apply(set).apply(list).reset_index()
    port_of_lading = notify_info.groupby(['BUYER'])['FOREIGN PORT OF LADING'].apply(set).apply(list).reset_index()
    
    result = buyer[(buyer['total_transaction_no'] > transaction) & (buyer['total_volume'] > volume)]
    result.drop(columns=['comName_x','comName_y'], inplace=True)
    if word_filter != None:
        result['isChosen'] = result['name'].apply(lambda x: not any(word in x.split(' ') for word in word_filter))
    else:
        result['isChosen'] = True
    
    print(result.shape)
    result = result.merge(notify_name, how='left', left_on='name', right_on='BUYER')
    result.drop(columns=['BUYER'], inplace=True)
    print(result.shape)
    result = result.merge(notify_adress, how='left', left_on='name', right_on='BUYER')
    result.drop(columns=['BUYER'], inplace=True)
    print(result.shape)
    result = result.merge(notify_phone, how='left', left_on='name', right_on='BUYER')
    result.drop(columns=['BUYER'], inplace=True)
    print(result.shape)
    result = result.merge(notify_email, how='left', left_on='name', right_on='BUYER')
    result.drop(columns=['BUYER'], inplace=True)
    print(result.shape)
    result = result.merge(port_of_lading, how='left', left_on='name', right_on='BUYER')
    result.drop(columns=['BUYER'], inplace=True)
    print(result.shape)
        
    result.rename(columns={'name':'Buyer name',
                           'address':'Buyer address',
                           'email':'Buyer email',
                           'phone':'Buyer phone',
                           'total_volume':'Total quantity',
                           'total_transaction_no':'Count',
                           'FOREIGN PORT OF LADING':'Port of lading'}, inplace=True)
    
    result = result[result['isChosen']].reset_index(drop=True)
    result = result[['Buyer name','Buyer address','Buyer email','Buyer phone',
                     'Notify name','Notify address','Notify email','Notify phone',
                     'Port of lading','Total quantity','Count']]
    
    result['tag'] = tag
    
    # check if manual update data is available
    sql = text(
    '''SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'
       ''') 

    with engine.connect() as conn:
        results = conn.execute(sql)
    
    tables = [i[0] for i in results.fetchall()]
    
    if 'buyer_info_update' in tables:
        query = '''
        SELECT *
            FROM "buyer_info_update"
        '''
        with engine.connect() as conn:
            buyer_update = pd.read_sql(query, conn)
        
        buyer_update = buyer_update.sort_values(by=['updated_at'], ascending=False).drop_duplicates(subset=['buyerName'], keep='first')
        print(result.shape)
        result = result.merge(buyer_update, how='left', left_on='Buyer name', right_on='buyerName')
        result.loc[result['comAddress'].notna(),'Buyer address'] = result.loc[result['comAddress'].notna(),'comAddress']
        result.loc[result['comEmail'].notna(),'Buyer email'] = result.loc[result['comEmail'].notna(),'comEmail']
        result.loc[result['comPhone'].notna(),'Buyer phone'] = result.loc[result['comPhone'].notna(),'comPhone']
        print(result.shape)
        result = result[['Buyer name','Buyer address','Buyer email','Buyer phone',
                         'Notify name','Notify address','Notify email','Notify phone',
                         'Port of lading','Total quantity','Count',
                         'Email tìm thêm','Phone tìm thêm','Data status',
                         'Email status','Note','Price period','updated_at']]

    
    else:
        result['Email tìm thêm'] = float('nan')
        result['Phone tìm thêm'] = float('nan')	
        result['Data status'] = float('nan')
        result['Email status'] = float('nan')
        result['Note'] = float('nan')
        result['Price period'] = float('nan')

    return result
    
def filter_data_datetime(df, mintime, maxtime):
    return df[df['ACTUAL ARRIVAL DATE'].between(mintime, maxtime, inclusive='both')]

def overall_chart(df):
    pass

timeframe_area = st.empty()
overall_area = st.empty()
filter_area = st.empty()
detail_area = st.empty()

data, mintime, maxtime = fetch_transaction()
com = fetch_com()

with timeframe_area.container(border=True):
    st.markdown('### Set timeframe to consider')  
    timeframe = st.slider('Select timeframe', 
                           min_value=mintime,
                           max_value=maxtime,
                           value=(mintime, maxtime))
    
with overall_area.container(border=True):
    st.markdown('### Overview')    
    stat_area = st.empty()
    
    df = filter_data_datetime(data, timeframe[0], timeframe[1])
        
    with stat_area.container():
        col1, col2, col3, col4 = st.columns([0.15,0.15,0.15,0.15])
        with col1.container(border=True):
            record_count = st.metric(
                label='Total Transaction No.',
                value='{:,}'.format(df.shape[0]),
            )
        with col2.container(border=True):
            volume_sum = st.metric(
                label='Total Volume (in MT)',
                value = '{:,.2f}'.format(df['WEIGHT (MT)'].sum()),
            )
        with col3.container(border=True):
            buyer_count = st.metric(
                label='Buyer No.',
                value='{:,}'.format(len(df['BUYER'].unique())),
            )
        with col4.container(border=True):
            supplier_count = st.metric(
                label='Supplier No.',
                value='{:,}'.format(len(df['SUPPLIER'].unique())),
            )
        
        general_chart = st.empty()        
        with general_chart.container(border=True):
            time_basis = st.radio(label='Time basis',
                                      options=['Monthly','Daily'],
                                      index=0)
            temp = df.copy()
            if time_basis == 'Daily':
                temp['Time'] = temp['ACTUAL ARRIVAL DATE']
                temp['Time'] = temp['Time'].astype(str)
                agg_volume = temp.groupby(['Time'])['WEIGHT (MT)'].sum().reset_index()
                agg_buyer = temp.groupby(['Time'])['BUYER'].nunique().reset_index()
                agg_buyer.rename(columns={'BUYER':'count'}, inplace=True)
                agg_buyer['role'] = 'BUYER'
                agg_supplier = temp.groupby(['Time'])['SUPPLIER'].nunique().reset_index()
                agg_supplier.rename(columns={'SUPPLIER':'count'}, inplace=True)
                agg_supplier['role'] = 'SUPPLIER'
                agg_buyer_sup = pd.concat([agg_buyer,agg_supplier], ignore_index=True)
                
            elif time_basis == 'Monthly':
                temp['Time'] = temp['ACTUAL ARRIVAL DATE'].apply(lambda x: str(x.year) + str(x.month) if x.month > 9 else str(x.year) + '0' + str(x.month)) 
                agg_volume = temp.groupby(['Time'])['WEIGHT (MT)'].sum().reset_index()
                agg_buyer = temp.groupby(['Time'])['BUYER'].nunique().reset_index()
                agg_buyer.rename(columns={'BUYER':'count'}, inplace=True)
                agg_buyer['role'] = 'BUYER'
                agg_supplier = temp.groupby(['Time'])['SUPPLIER'].nunique().reset_index()
                agg_supplier.rename(columns={'SUPPLIER':'count'}, inplace=True)
                agg_supplier['role'] = 'SUPPLIER'
                agg_buyer_sup = pd.concat([agg_buyer,agg_supplier], ignore_index=True)
                agg_buyer_byvol = temp.groupby(['BUYER'])['WEIGHT (MT)'].sum().sort_values(ascending=True)[-10:].reset_index()
                agg_buyer_bytrans = temp.groupby(['BUYER'])['WEIGHT (MT)'].count().sort_values(ascending=True)[-10:].reset_index()
                
            col1, col2 = st.columns([0.5,0.6])
            with col1:
                fig = px.bar(
                    agg_buyer_byvol, 
                    x='WEIGHT (MT)', 
                    y='BUYER',
                    orientation='h',
                    title='Top 10 Buyer in Volume'
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.bar(
                    agg_buyer_bytrans, 
                    x='WEIGHT (MT)', 
                    y='BUYER',
                    orientation='h',
                    title='Top 10 Buyer in Transaction No.'
                )
                st.plotly_chart(fig, use_container_width=True)
                
            col1, col2 = st.columns([0.5,0.6])
            with col1:                
                fig = px.line(
                    agg_volume, 
                    x='Time', 
                    y='WEIGHT (MT)',
                    title='Total Volume Over Time'
                )
                fig.update_layout(
                    xaxis_type='category',
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.bar(
                    agg_buyer_sup, 
                    x='Time', 
                    y='count',
                    color='role',
                    title='No. of Buyers and Supplier Involved in Trades Over Time'
                )
                fig.update_layout(
                    xaxis_type='category',
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with filter_area.container(border=True):
        st.markdown('### Company Filter')
        criteria_area = st.form("Criteria to filter")
        result_area = st.empty()
        export_area = st.empty()
        with criteria_area.container():
            st.markdown('##### Criteria')
            col1, col2 = st.columns([0.3,0.5])
            with col1:
                trading_side = st.multiselect(label='Trading side',
                                              options=['BUYER','NOTIFY PARTY NAME', 'SUPPLIER'],
                                              default='BUYER',
                )
                words_filter = st.multiselect(label='Remove companies which their names includes one of these words',
                                              options=com_name_words(com))
            with col2:
                transaction_threshold = st.number_input(label='Minimum Transaction No.',
                                                        min_value=0,
                                                        max_value=None,
                                                        value='min',
                )
                volume_threshold = st.number_input(label='Minimum Volume (in MT)',
                                                   min_value=0,
                                                   max_value=None,
                                                   step=1,
                                                   value='min'
                )
                tag = st.selectbox(label='Batch',
                                       options=['all','batch1','batch2'],
                                       index=0,
                                       disabled=False
                )
            
            submit_button = st.form_submit_button(label="Generate") 
        
        if submit_button:
            result = aggregate_filter(df, com, trading_side, transaction_threshold, volume_threshold, words_filter, tag)
            st.dataframe(result, height=300)
            datetime_stamp = f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
            
            result.to_excel(f'logs/report_{datetime_stamp}.xlsx', index=None)
            with open(f'logs/report_{datetime_stamp}.xlsx', 'rb') as file:
                byte = file.read()
            
            st.download_button(
                label='Download data',
                data=byte,
                file_name=f'report_{datetime_stamp}.xlsx',
                mime='application/octet-stream',
                )
                
    with detail_area.container(border=True):
        st.markdown('### Company Information')
        criteria_area = st.empty()
        chart_area = st.empty()
        with criteria_area.container():
            com_name = st.selectbox(
                label='Buyer name',
                options=com['name'].tolist(),
                index=None
            )
        if com_name != None:
            com_info = com[com['name'] == com_name]
            st.markdown('##### Infomation')
            st.dataframe(com_info, hide_index=True, use_container_width=True)
            temp1 = data[data['BUYER'] == com_name]
            temp2 = data[data['NOTIFY PARTY NAME'] == com_name]
            temp = pd.concat([temp1, temp2], ignore_index=True)
            col1, col2 = st.columns([0.7,0.3])
            with col2.container(border=True):
                st.metric(
                    label='Supplier No.',
                    value='{:,}'.format(len(temp['SUPPLIER'].unique()))
                )
                st.metric(
                    label='Transaction No.',
                    value='{:,}'.format(temp.shape[0])
                )
                st.metric(
                    label='Total Volume (in MT)',
                    value='{:,.2f}'.format(temp['WEIGHT (MT)'].sum())
                )
                
            with col1.container(border=True):
                col3, col4 = st.columns([0.5,0.5])
                with col3:
                    metric1 = st.empty()
                with col4:
                    metric2 = st.empty()
                    
                temp['Time'] = temp['ACTUAL ARRIVAL DATE'].apply(lambda x: str(x.year) + str(x.month) if x.month > 9 else str(x.year) + '0' + str(x.month)) 
                agg_volume = temp.groupby(['Time'])['WEIGHT (MT)'].sum().reset_index()
                agg_supplier = temp.groupby(['Time'])['SUPPLIER'].nunique().reset_index()
                agg_supplier.rename(columns={'SUPPLIER':'count'}, inplace=True)
                
                with metric1.container(border=True):
                    st.metric(label='No. Transactions as Buyer',
                              value='{:,}'.format(temp1.shape[0])
                    )
                    
                with metric2.container(border=True):
                    st.metric(label='No. Transactions as Notify Party',
                              value='{:,}'.format(temp2.shape[0])
                    )
                    
                fig = px.line(
                    agg_volume, 
                    x='Time', 
                    y='WEIGHT (MT)',
                    markers=True,
                    title='Volume Over Time',
                )
                fig.update_layout(
                    xaxis_type='category',
                )
                st.plotly_chart(fig, use_container_width=True)
               
    
