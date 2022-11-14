import getpass
import json
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit


class SnowflakeTablesCollection:
    
    def __init__(self, session: Session):
        self.services = session.table('ods.xpo_dbo_tblservices').withColumn('parent_service', col('ServiceID'))
        shopped_services = (session.table('ods.xpo_dbo_tblservices')
                                .join(session.table('ods.xpo_dbo_tblservicesuse').select('serviceidfrom', 'serviceidto'),
                                      col('serviceidto') == col('ServiceID'))
                                .drop('serviceidto')
                                .withColumnRenamed('serviceidfrom', 'parent_service'))
        
        self.all_services = self.services.union_all(shopped_services)
        
        self.countries = session.table('ods.ppx_dbo_country')
        self.exceptions = session.table('ods.xpo_dbo_exceptions')
        self.matrix = session.table('ods.xpo_dbo_matrix').filter((col('Status') == lit('P')))
        self.matrix_det = session.table('ods.xpo_dbo_matrixdet')
        self.vendors = session.table('ods.xpo_dbo_tblvendors')
        self.currency = session.table('ods.xpo_dbo_tblcurrency')
        self.prefers = session.table('ods.xpo_dbo_preferredvendor')
        self.nonprefers = session.table('ods.xpo_dbo_preferredvendornon')
        self.parate01 = session.table('ods.ppx_dbo_parate01')
        self.units = session.table('ods.xpo_dbo_units')
        self.ratetype = session.table('ods.xpo_dbo_ratetype')


class SnowflakeQuoterSession:
    
    def __init__(self, configs_path=None, direct_options=None, mode="direct"):
        """
        available config modes include: 
            - env (will look for specific names, sf_<<config name>>) 
            - configs (use configs_path kwarg to point to .json with configs
            - input (have user manually type input, defaults outside username and password)
            - direct (pass in memory dict instead of .json, use direct_options to pass in)
        """
        if mode == "configs":
            if not configs_path:
                raise Exception("No path to config file given in argument configs_path")
            connection_parameters = self.get_connection_params_local(configs_path)
        elif mode == "env":
            connection_parameters = self.get_connection_params_env()
        elif mode == "input":
            connection_parameters = self.get_connection_params_input()
        elif mode == "direct":
            if not direct_options:
                raise Exception("No options passed in direct")
            connection_parameters = direct_options
        else:
            raise Exception("please enter valid mode kwarg per docstring")
        self.session = Session.builder.configs(connection_parameters).create()
        self.tables = SnowflakeTablesCollection(self.session)
        
    
    def __del__(self):
        self.session.close()

    def get_connection_params_local(self, config_path):
        with open(config_path) as f:
            return json.load(f)
        
    def get_connection_params_input(self):
        sf_keys = ["account", "user", "password", "warehouse", "database", "schema"]
        default_keys = {
            "account" : "xf56565.west-us-2.azure",
            "warehouse": "DATACLOUD_COMPUTE_WH",
            "database": "DATACLOUD_TEST",
            "schema": "PUBLIC"
        }
        print("Please set Snowflake connection parameters.  Defaults below, press enter to skip fields you do not wish to override.")
        print(default_keys)
        set_keys = {key: input(f'{key}: ') if key != 'password' else getpass.getpass('password: ') for key in sf_keys}
        return_keys = {key: set_keys.get(key) if set_keys.get(key) else default_keys.get(key) for key in sf_keys}
        return return_keys

    def get_connection_params_env(self):
        return {
            "account": os.environ["sf_account"],
            "user": os.environ["sf_user"],
            "password": os.environ["sf_password"],
            "warehouse": os.environ["sf_warehouse"],
            "database": os.environ["sf_database"],
            "schema": os.environ["sf_schema"]
        }