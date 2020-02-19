
import multiprocessing
import os
import make_load_curve
import pandas as pd





if __name__ == "__main__":

    __spec__ = None

    # Import county heat deamdn

    ff_dict = {'place': 'res_forecasts',
               'county subdivision': 'res_forecasts_csubdiv',
               'tract': 'res_forecasts_tract'}

    rf = res_forecast(geo='tract')

    # Tract projections are made at the total household level and are not
    # disaggregated by hh type. This can be changed in the future, but
    # increased computational resources are needed.
    if rf.geo == 'tract':

        states_geos = pd.DataFrame(rf.res_agg_dict['total']['estimate'])

        states_geos['df'] = 'total'

        states_geos.reset_index(inplace=True)

        states_geos.drop_duplicates(['state', 'geoid'], inplace=True)

        states_geos = tuple(states_geos[['df', 'state', 'geoid']].values)

    else:

        states_geos = pd.concat(
            [rf.res_agg_dict[k]['estimate'] for k in rf.res_agg_dict.keys()],
            axis=1, ignore_index=False
            )

        states_geos = states_geos.reset_index().groupby(
                ['state', rf.geo]
                )[['single', 'multi', 'mobile']].sum()

        states_geos = states_geos.reset_index().melt(id_vars=['state', rf.geo],
                                             value_name='hh', var_name='df')

        states_geos = pd.DataFrame(states_geos[states_geos.hh !=0])

        states_geos = tuple(states_geos[['df', 'state', rf.geo]].values)

    if ff_dict[rf.geo] not in os.listdir(os.getcwd()):

        test = input('Forecast results not found.\n'
                     'Proceed with forecasting? (Y/N): ')

        if test.lower() == 'y':

            res_forecasts = pd.DataFrame()

            with multiprocessing.Pool() as pool:

                results = pool.starmap(rf.run_forecast_parallel, states_geos)

                for ar in results:

                    res_forecasts = res_forecasts.append(
                            pd.Series(ar), ignore_index=True
                            )

            print('complete')

            res_forecasts.to_csv(ff_dict[rf.geo], compression='gzip',
                                 index=False)


    if ('res_forecasts_tract' in os.listdir()):

        forecasts = pd.read_csv('res_forecasts_tract', compression='gzip')

        rf.calc_final_projections(forecasts)
