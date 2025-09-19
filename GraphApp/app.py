import streamlit as st
import psycopg
import os
import time
import re
from databricks import sdk
from psycopg import sql
from psycopg_pool import ConnectionPool
from pyvis import network as net
import pandas as pd
import streamlit.components.v1 as components

# Database connection setup
workspace_client = sdk.WorkspaceClient()
postgres_password = None
last_password_refresh = 0
connection_pool = None

def refresh_oauth_token():
    """Refresh OAuth token if expired."""
    global postgres_password, last_password_refresh
    if postgres_password is None or time.time() - last_password_refresh > 900:
        print("Refreshing PostgreSQL OAuth token")
        try:
            postgres_password = workspace_client.config.oauth_token().access_token
            last_password_refresh = time.time()
        except Exception as e:
            st.error(f"❌ Failed to refresh OAuth token: {str(e)}")
            st.stop()

def get_connection_pool():
    """Get or create the connection pool."""
    global connection_pool
    if connection_pool is None:
        refresh_oauth_token()
        conn_string = (
            f"dbname={os.getenv('PGDATABASE')} "
            f"user={os.getenv('PGUSER')} "
            f"password={postgres_password} "
            f"host={os.getenv('PGHOST')} "
            f"port={os.getenv('PGPORT')} "
            f"sslmode={os.getenv('PGSSLMODE', 'require')} "
            f"application_name={os.getenv('PGAPPNAME')}"
        )
        connection_pool = ConnectionPool(conn_string, min_size=2, max_size=10)
    return connection_pool

def get_connection():
    """Get a connection from the pool."""
    global connection_pool
    
    # Recreate pool if token expired
    if postgres_password is None or time.time() - last_password_refresh > 900:
        if connection_pool:
            connection_pool.close()
            connection_pool = None
    
    return get_connection_pool().connection()

def get_schema_name():
    """Get the schema name in the format {PGAPPNAME}_schema_{PGUSER}."""
    pgappname = os.getenv("PGAPPNAME", "my_app")
    pguser = os.getenv("PGUSER", "").replace('-', '')
    return f"{pgappname}_schema_{pguser}"

def init_database():
    """Initialize database schema and table."""
    with get_connection() as conn:
        with conn.cursor() as cur:

            schema_name = get_schema_name()

            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name)))
            
            #cur.execute(sql.SQL("drop table if exists {}.nodes").format(sql.Identifier(schema_name)))
            #cur.execute(sql.SQL("drop table if exists {}.edges").format(sql.Identifier(schema_name)))

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.nodes (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                );
            """).format(sql.Identifier(schema_name)))

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.edges (
                    id SERIAL PRIMARY KEY,
                    start_node integer NOT NULL,
                    end_node integer NOT NULL,
                    name TEXT NOT NULL
                );
            """).format(sql.Identifier(schema_name)))

            conn.commit()
            return True

def add_node(node):
    with get_connection() as conn:
        with conn.cursor() as cur:
            schema = get_schema_name()
            cur.execute(sql.SQL("INSERT INTO {}.nodes (name) VALUES (%s)").format(sql.Identifier(schema)), (node.strip(),))
            conn.commit()

def add_edge(start, end):
    node_dict=dict(get_nodes())
    d_swap = {v: k for k, v in node_dict.items()}
    with get_connection() as conn:
        with conn.cursor() as cur:
            schema = get_schema_name()
            cur.execute(sql.SQL("INSERT INTO {}.edges (start_node, end_node, name) VALUES (%s , %s , ' ')").format(sql.Identifier(schema)), (d_swap[start.strip()], d_swap[end.strip()],))
            conn.commit()

def get_edges():
    with get_connection() as conn:
        with conn.cursor() as cur:
            schema = get_schema_name()
            cur.execute(sql.SQL("SELECT start_node, end_node FROM {}.edges").format(sql.Identifier(schema)))
            return cur.fetchall()
        
def get_nodes():
    with get_connection() as conn:
        with conn.cursor() as cur:
            schema = get_schema_name()
            cur.execute(sql.SQL("SELECT id, name FROM {}.nodes").format(sql.Identifier(schema)))
            return cur.fetchall() 
        
def toggle_todo(todo_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            schema = get_schema_name()
            cur.execute(sql.SQL("UPDATE {}.todos SET completed = NOT completed WHERE id = %s").format(sql.Identifier(schema)), (todo_id,))
            conn.commit()


def delete_edge(start, end):
    with get_connection() as conn:
        with conn.cursor() as cur:
            schema = get_schema_name()
            cur.execute(sql.SQL("DELETE FROM {}.edges WHERE start = %s and end_node = %s" ).format(sql.Identifier(schema)), (start, end,))
            conn.commit()

def delete_node(node_id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            schema = get_schema_name()
            cur.execute(sql.SQL("DELETE FROM {}.nodes WHERE name = %s").format(sql.Identifier(schema)), (node_id,))
            cur.execute(sql.SQL("DELETE FROM {}.edges WHERE start = %s OR end_node = %s").format(sql.Identifier(schema)), (node_id, node_id,))
            conn.commit()

@st.fragment
def display_graph():
    
    vertices=get_nodes()
    edge_list=get_edges()


    test_net = net.Network()

    node_ids=[node[0] for node in vertices]
    node_titles=[node[1] for node in vertices]
#    edges=[(edge[0], edge[1]) for edge in edge_list]
    test_net.add_nodes(node_ids, label=node_titles)
    test_net.add_edges(edge_list)
    test_net.show_buttons(filter_=['physics'])

    st.subheader("Interactive Vizualisation in Apps")

    test_net.save_graph("tst.html")
    with open('tst.html', 'r') as file:
        str = file.read()
    return str

@st.fragment
def display_debug():
    st.subheader("State")
    
    node_list = get_nodes()
    node_dict =dict(node_list)
    edge_list = get_edges()
    st.markdown(' '.join([str(x) for x in node_list]))
    d_swap = {v: k for k, v in node_dict.items()}
    st.markdown(' '.join([str(x) for x in d_swap]))
    st.markdown(' '.join([str(x) for x in edge_list]))





# Streamlit UI
def main():
    st.set_page_config(
        page_title="Graph Editing App",
        page_icon="📝",
        layout="wide"
    )
    
    st.title("📝 Graph Editor")
    st.markdown("---")
    
    # Initialize database
    if not init_database():
        st.stop()
    
    # Add new Node
    st.subheader("Add New Node")
    with st.form("add_node_form", clear_on_submit=True):
        new_task = st.text_input("Enter a new node name:", placeholder="name")
        submitted = st.form_submit_button("Add Node", type="primary")
        
        if submitted and new_task.strip():
            if add_node(new_task.strip()):
                st.success("✅ Node added successfully!")

    st.markdown("---")
    st.subheader("Add New Edge")
    with st.form("add_edge_form", clear_on_submit=True):
        new_task = st.text_input("Enter a start node:", placeholder="Start Node")
        new_task2 = st.text_input("Enter a end node:", placeholder="End Node")
        submitted = st.form_submit_button("Add Edge", type="primary")
        
        if submitted and new_task.strip() and new_task2.strip():
            if add_edge(new_task.strip(), new_task2.strip()):
                st.success("✅ Edge added successfully!")

    st.markdown("---")

    strg=display_graph()
    components.html(strg, height=1000)

#    display_debug()
if __name__ == "__main__":
    main() 

    import os






