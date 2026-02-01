import os
from BHRC_tokens import API_KEY
os.environ['MD_AUTH_TOKEN']= API_KEY
import morningstar_data as md
import pandas as pd

# Set display options to show all rows and columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)        # Don't truncate lines
pd.set_option('display.max_colwidth', None) # Show full content in each cell

# List all pre-built datasets
datasets = md.direct.portfolio.get_data_sets()
print(datasets)  # Shows data_set_id and name


# Set display options to show all rows and columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)        # Don't truncate lines
pd.set_option('display.max_colwidth', None) # Show full content in each cell

# Pick a dataset, e.g. data_set_id = '1', then list its data points:
dp_df = md.direct.portfolio.get_data_set_data_points(data_set_id='1')
print(dp_df[['data_point_id', 'name']])
