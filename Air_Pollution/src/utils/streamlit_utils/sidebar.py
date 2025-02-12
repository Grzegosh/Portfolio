import sys
sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")


import streamlit as st
from streamlit import session_state as ss


class Sidebar:
    def __init__(self, msg:str, options:tuple):
        self.msg = msg
        self.options = options

    def create_sidebar(self):
        if not isinstance(self.msg, str):
            raise TypeError("msg paramater must be a string.")

        if not isinstance(self.options, tuple):
            raise TypeError("options paranmeter must be a tuple.")

        add_options = st.sidebar.selectbox(
            self.msg,
            self.options
        )
        return add_options


    def toggle_sidebar(self,type_:str):
        if 'sidebar_state' not in ss:
            ss.sidebar_state = "collapsed"

        test = st.set_page_config(initial_sidebar_state=ss.sidebar_state)

        def change():
            ss.sidebar_state = (
                "collapsed" if ss.sidebar_state == "expanded" else "expanded"
            )

        st.button("test", on_click=change)











