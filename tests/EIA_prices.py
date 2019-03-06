import pandas as pd
import eia
import json
#%%

API_auth_path = "U:\API_auth.json"

with open(API_auth_path, 'r') as f:
        auth_file = json.load(f)

api_key = auth_file['eia_API']

api = eia.API(api_key)

# EIA annual industrial natural gas prices by state
def get_price_series(api, fuel, time_step):

    price_series = \
        api.search_by_keyword(keyword=[fuel, 'price'],
                              filters_to_keep=['industrial', time_step])

    prices = pd.DataFrame()

    for k in price_series.keys():

        df = pd.DataFrame.from_dict(api.data_by_keyword(k), orient='columns')

        prices = prices.append(df, sort=True)

    return prices


eia_ng_prices = get_price_series(api, 'natural gas', 'annual')

#%%
prices = pd.DataFrame()

for k in eia_ng_prices.keys():

    df = pd.DataFrame.from_dict(api.data_by_keyword(k), orient='columns')

    prices = prices.append(df, sort=True)
#%%
eia_elect_prices = get_price_series(api, 'electricity', 'annual')
#%%
api.search_by_keyword(keyword=['electricity', 'price', 'industrial'],
                      filters_to_remove=['AEO'])
