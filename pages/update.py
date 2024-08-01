import streamlit as st
import pandas as pd
from datetime import datetime
import sqlalchemy as sa
from streamlit_option_menu import option_menu

st.set_page_config(
    page_title='Rice Global Trade Handbook | Data update',
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
        icons=['clipboard-data', 'database-add'], menu_icon="list", default_index=1)
    if selected == 'Dashboard':
        st.switch_page('pages/main.py')
    
def validate(data):
    columns = ['buyerName','comAddress','comPhone','comEmail','Phone tìm thêm','Email tìm thêm',
               'Data status','Email status','Note','Price period']
    missing_cols = []
    for i in columns:
        if i not in data.columns:
            missing_cols.append(i)
    
    if len(missing_cols) > 0:
        return ', '.join(i for i in missing_cols) + 'not found in the data'
    
    if len(data['buyerName'].unique()) != data.shape[0]:
        return 'buyerName column is not unique (duplicated buyerName)'
    
    data['updated_at'] = datetime.now()
    
    return data

def upload(data):
    password = 'RiceTrade%40123'
    uri = f'''postgresql://postgres.bgcbdtswsqcqrokngeba:{password}@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres'''
    
    engine = sa.create_engine(uri)
    with engine.connect() as conn:
        data.to_sql('buyer_info_update', conn, schema='public', if_exists='append', index=False)
    
    return 'Done uploading'
 
def update():
    password = 'RiceTrade%40123'
    uri = f'''postgresql://postgres.bgcbdtswsqcqrokngeba:{password}@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres'''
    engine = sa.create_engine(uri)
    with engine.connect() as conn:
        query = '''
        SELECT * FROM "update_data";
        '''
        newest_data = pd.read_sql(query, conn)
        
        query = '''
        SELECT * FROM "company_info";
        '''
        com_data = pd.read_sql(query, conn)
    
    newest_data = newest_data.sort_values(by=['updateDate'], ascending=False).drop_duplicates(subset=['comName'], keep='first')    
    com_data = com_data.merge(newest_data, how='left', left_on='name', right_on='comName')
    
    with engine.connect() as conn:
        com_data.to_sql('update_company_info', conn, schema='public', if_exists='append', index=False)
        
    return 'Done updating'

upload_area = st.empty()
preview_area = st.empty()
commit_area = st.empty()

with upload_area.container(border=True):
    uploaded_file = st.file_uploader("Choose a file", type=['xlsx'])
    
if uploaded_file is not None:
    with st.spinner('Loading data ...'):
        data = pd.read_excel(uploaded_file)
        
    with preview_area.container(border=True):
        st.markdown('#### Data preview')
        st.dataframe(data)
        
    with commit_area.container(border=True):
        password = st.text_input(
            "Password to update",
            type='password'
        )
        button = st.button(label='Update')
    
    if button:
        if password == 'vnflowai@1234':
            with st.spinner('Updating data ...'):
                data = validate(data)
                if isinstance(data, pd.DataFrame):
                    st.success('Data validated successfully!')
                else:
                    st.error('Invalid data!' + data)
                try:
                    status = upload(data)
                    st.success(status)
                except:
                    st.error('Uploading failed!') 
        else:
            st.error('Password is WRONG!!!')


