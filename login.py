import yaml
from yaml.loader import SafeLoader
from PIL import Image
import streamlit_authenticator as stauth
import streamlit as st

st.set_page_config(
    page_title='Rice Global Trade Handbook - Login',
    page_icon='',
    layout='centered',
)

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

with open('./config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)
    
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['pre-authorized']
)

with st.container(border=True):
    logo, name = st.columns([0.3,0.8], gap='large')
    with logo:
        st.image('image/vnflow_logo.jpg', use_column_width='always')
    with name:
        st.markdown('### Rice Global Trade Handbook')
        
name, authentication_status, username = authenticator.login( 'main','Login')

if authentication_status:
    st.session_state['auth'] = authenticator
    st.session_state['name'] = name
    st.session_state['password_correct'] = True
    st.switch_page('pages/main.py')
elif authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status == None:
    st.warning('Please enter your username and password')