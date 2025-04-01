import pandas as pd


# For Snowflake
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy import text

# For Keeper
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import hvac


def get_sf_connector(
 
    role_id='bd5',

    secret_id='38a8ac' ,  
    keeper_url="https://keeper.cisco.com",
    keeper_namespace="cloudDB",
    keeper_secret_path="secret/snowflake/prd/cps_cloibm_etl_svc/key",
    sf_account="cisco.us-east-1",
    sf_svc_user="CPS_CLOIBM_ETL_SVC",
    sf_wh="CPS_CLOIBM_ETL_WH",
    sf_role="CPS_CLOIBM_ETL_ROLE",
    sf_db="CPS_DB",
    sf_schema="CPS_CLOIBM_BR",
    return_sqlalchemy_engine=False,
    
):
    try:
        # Establish connection with Vault
        
        # token = hvac.Client(url=keeper_url, namespace=keeper_namespace).auth_approle(
        #     role_id=role_id, secret_id=secret_id, mount_point="approle"
        # )["auth"]["client_token"]

        # when hvac is upgraded to >= 1.0.0 then use this
        token = hvac.Client(url=keeper_url, namespace=keeper_namespace).auth.approle.login(
            role_id=role_id, secret_id=secret_id, mount_point="approle"
        )["auth"]["client_token"]

        secrets_client = hvac.Client(
            url=keeper_url, namespace=keeper_namespace, token=token
        )        
        
        keeper_secrets = secrets_client.read(keeper_secret_path)["data"]
        passphrase = keeper_secrets["SNOWSQL_PRIVATE_KEY_PASSPHRASE"]
        private_key = keeper_secrets["private_key"]


        key = bytes(private_key, "utf-8")
        p_key = serialization.load_pem_private_key(
            key, password=passphrase.encode(), backend=default_backend()
        )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )


        # Create connector
        sf_client = snowflake.connector.connect(
            user=sf_svc_user,
            account=sf_account,
            warehouse=sf_wh,
            role=sf_role,
            database=sf_db,
            schema=sf_schema,
            private_key=pkb,
        )
        print("Snowflake Connector created!")
        return sf_client

    except Exception as e:
        print(f"Unable to establish connection: {str(e)}")


def get_snowflake_connector(
      

    role_id='b',

    secret_id='38a8ab',    


    keeper_url="https://keeper.cisco.com",
    keeper_namespace="cloudDB",
    keeper_secret_path="secret/snowflake/prd/cps_cloibm_etl_svc/key",
    sf_account="cisco.us-east-1",
    sf_svc_user="CPS_CLOIBM_ETL_SVC",
    sf_wh="CPS_CLOIBM_ETL_WH",
    sf_role="CPS_CLOIBM_ETL_ROLE",
    sf_db="CPS_DB",
    sf_schema="CPS_CLOIBM_BR",
    return_sqlalchemy_engine=True,
    
):
    try:
        # Establish connection with Vault
        
        # token = hvac.Client(url=keeper_url, namespace=keeper_namespace).auth_approle(
        #     role_id=role_id, secret_id=secret_id, mount_point="approle"
        # )["auth"]["client_token"]

        # when hvac is upgraded to >= 1.0.0 then use this
        token = hvac.Client(url=keeper_url, namespace=keeper_namespace).auth.approle.login(
            role_id=role_id, secret_id=secret_id, mount_point="approle"
        )["auth"]["client_token"]

        secrets_client = hvac.Client(
            url=keeper_url, namespace=keeper_namespace, token=token
        )        
        
        keeper_secrets = secrets_client.read(keeper_secret_path)["data"]
        passphrase = keeper_secrets["SNOWSQL_PRIVATE_KEY_PASSPHRASE"]
        private_key = keeper_secrets["private_key"]


        key = bytes(private_key, "utf-8")
        p_key = serialization.load_pem_private_key(
            key, password=passphrase.encode(), backend=default_backend()
        )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        if return_sqlalchemy_engine:
            sf_engine = create_engine(
                URL(
                    account=sf_account,
                    user=sf_svc_user,
                    database=sf_db,
                    schema=sf_schema,
                    warehouse=sf_wh,
                    role=sf_role,
                ),
                connect_args={"private_key": pkb},
            )
            print("Snowflake SQLAlchemy Engine created!")
            return sf_engine
        else:
            sf_client = snowflake.connector.connect(
                user=sf_svc_user,
                account=sf_account,
                warehouse=sf_wh,
                role=sf_role,
                database=sf_db,
                schema=sf_schema,
                private_key=pkb,
            )
            print("Snowflake Connector created!")
            return sf_client

        # # Create connector
        # sf_client = snowflake.connector.connect(
        #     user=sf_svc_user,
        #     account=sf_account,
        #     warehouse=sf_wh,
        #     role=sf_role,
        #     database=sf_db,
        #     schema=sf_schema,
        #     private_key=pkb,
        # )
        # print("Snowflake Connector created!")
        # return sf_client

    except Exception as e:
        print(f"Unable to establish connection: {str(e)}")

        

# read from SF

def sf_query_to_df(sf_client, query):
    cursor = sf_client.cursor().execute(query)
    return pd.DataFrame.from_records(
        iter(cursor), columns=[x[0] for x in cursor.description]
    )

# read from SF based on param in SQL

def sf_query_to_df_params(sf_client, query,params):
    cursor = sf_client.cursor().execute(query,params)
    return pd.DataFrame.from_records(
        iter(cursor), columns=[x[0] for x in cursor.description]
    )

# to execute create statements
def run_snowflake_query(sf_client, query):
    results = sf_client.cursor().execute(query).fetchall()
    print(results[0][0])


def check_snowflake_query(sf_client, query):
    results = sf_client.cursor().execute(query).fetchall()
    return results[0][0]


# exports model results df to snowflake
# df: df with model input + explanations, sf_table_name: table name for snowflake,
# sf_engine
def export_df_to_sf(df, sf_table_name, sf_engine):
    df.to_sql(sf_table_name, con=sf_engine, index=False, if_exists="append")


