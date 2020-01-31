
import pandas as pd
import numpy as np
import os
import itertools

class LoadData:

    def __init__(self):

        self.datadir = './calculation_data/'

        self.nfiles = {'sic_N02_file': '1987_SIC_to_2002_NAICS.csv',
                       'N02_N07_file': '2002_to_2007_NAICS.csv',
                       'N07_N12_file': '2007_to_2012_NAICS.csv'}

        # Load factors from EPRI report. Categorized by SIC.
        self.epri_loadfactors = 'epri_load_factors.csv'

        # Monthly load factors calculated from EPA Air Markets Program data.
        # NAICS are 2007; need to be updated to 2012.
        self.usepa_loadfactors = 'epa_amd_load_factor.csv'

        self.epri_load_shapes = 'epri_load_shapes.csv'

        self.usepa_load_shapes = 'epa_amd_load_shapes.csv'

        def import_data(self):

            """
            Import load factor and load shape data. Requires adjusting from
            1997 SIC to 2002 NAICS to 2007 NAICS to 2012 NAICS.
            """

            def create_sic_naics_dfs(file_dir, file):

                sic_naics = pd.read_csv(
                        os.path.join(file_dir + file)
                        )

                sic_naics = sic_naics.iloc[:, [0, 2]]

                sic_naics.set_index(sic_naics.columns[0], inplace=True)

                return sic_naics

            def create_dict(file_dir, file):

                dict_out = dict(pd.read_csv(
                        os.path.join(file_dir + file)
                        ).iloc[:, [0, 2]].values)

                return dict_out

            ndict = {}

            for k, v in self.nfiles.items():

                ndict[k[0:7]] = create_sic_naics_dfs(self.datadir, v)

            # Import load factors and load shapes derived from EPA Air Markets
            # Program data.
            # EPA data use 2007 NAICS, not 2012 NAICS. Will need to remap
            usepa_lf = pd.read_csv(
                os.path.join(self.datadir+self.usepa_loadfactors),
                index_col='PRIMARY_NAICS_CODE',
                usecols=['PRIMARY_NAICS_CODE', 'month', 'HEAT_INPUT_MMBtu']
                )

            usepa_ls = pd.read_csv(
                os.path.join(self.datadir+self.usepa_load_shapes)
                )

            # Import load factors and load shapes from EPRI report.
            epri_lf = pd.read_csv(
                os.path.join(self.datadir+self.epri_loadfactors),
                )

            epri_ls = pd.read_csv(
                os.path.join(self.datadir+self.epri_load_shapes)
                )

            # Melt EPRI load shape data
            epri_ls = epri_ls.melt(
                id_vars=['hour', 'SIC'], var_name='daytype', value_name='load'
                )

            epri_ls.set_index('SIC', inplace=True)

            # Need to match EPRI's two and three-digit SIC, which have
            # no direct matches in SIC-NAICS crosswalk file.
            sic_3 = ndict['sic_N02'].copy(deep=True).reset_index()

            sic_3['SIC'] = sic_3.SIC.apply(lambda x: int(str(x)[0:3]))

            sic_3 = sic_3[sic_3.SIC.between(199, 400)]

            sic_2 = sic_3.copy(deep=True)

            sic_2['SIC'] = sic_2.SIC.apply(lambda x: int(str(x)[0:2]))

            ndict['N02_N07'].index.name = '2002 NAICS'

            ndict['N07_N12'].columns = ['NAICS12']

            def format_epri_SIC(epri_df, ndict):
                """
                Translate SIC codes used by EPRI to 2012 NAICS.
                """

                epri_df = pd.concat(
                    [epri_df.join(
                        df, how='inner', on='SIC'
                        ) for df in [ndict['sic_N02'], sic_3.set_index('SIC'),
                                     sic_2.set_index('SIC')]],
                    axis=0
                    )

                # epri_df = epri_df.join(ndict['sic_N02'], how='left')

                epri_df = epri_df.set_index('2002 NAICS').join(
                        ndict['N02_N07']
                        ).reset_index()

                epri_df = epri_df.set_index('2007 NAICS Code').join(
                        ndict['N07_N12']
                        ).reset_index()

                return epri_df

            epri_lf = format_epri_SIC(epri_lf, ndict)

            epri_ls = format_epri_SIC(epri_ls, ndict)

            #Need to reformat EPRI ls to starting hour == 0.
            epri_ls.hour.update(epri_ls.hour - 1)

            # Remap 2007 NAICS to 2012 NAICS for EPA data.
            # Resulting column named 'NAICS12'
            usepa_lf = usepa_lf.join(ndict['N07_N12']).reset_index(drop=True)

            usepa_ls = usepa_ls.set_index('PRIMARY_NAICS_CODE').join(
                ndict['N07_N12']
                ).reset_index(drop=True)

            # Rename columns to match EPRI load shape data
            usepa_ls.rename(
                columns={'dayofweek':'daytype','OP_HOUR':'hour','mean':'load'},
                inplace=True
                )

            return epri_lf, usepa_lf, epri_ls, usepa_ls

        self.epri_lf, self.usepa_lf, self.epri_ls, self.usepa_ls = \
            import_data(self)

    def select_min_peak_loads(self, load_shape, min_or_max):
        """
        From selected EPRI or EPA load shape, return the peak load by day type
        (weekday, Saturday, and Sunday). EPA data are also defined by month.
        """

        ls = load_shape.copy(deep=True)

        group_cols = ['month', 'daytype']

        df_level = [0,1]

        if 'month' not in ls.columns:

            group_cols.remove('month')

            df_level = [0]

        if min_or_max == 'max':

            loads = ls.groupby(group_cols, as_index=False).load.max()

        else:

            loads = ls.groupby(group_cols, as_index=False).load.min()

        # Check to make sure monthly load shape data has all 12 months.
        # If not, fill with average by daytype.
        if (len(df_level)==2) & (len(loads)<36):

            new_index = pd.DataFrame(index=
                [np.repeat(range(1,13),3),
                 np.tile(['saturday', 'sunday', 'weekday'],12)]
                )

            loads.set_index(['month', 'daytype'], inplace=True)

            loads = loads.reindex(index=new_index.index)

            loads.index.names = ['month', 'daytype']

            loads.reset_index('month', inplace=True)

            loads.update(loads.load.mean(level=0), overwrite=False)

            loads.reset_index(inplace=True)

            loads.set_index(['month', 'daytype', 'load'], inplace=True)

        loads['hour'] = np.nan

        group_cols.append('load')

        ls.set_index(group_cols, inplace=True)

        if loads.index.names != group_cols:

            loads.set_index(group_cols, inplace=True)

        try:

            df_level = [x for x in range(0, len(loads.index.levels))]

        except AttributeError:

            df_level = None

        for i in range(0, len(loads)):

            # if len(group_cols) > 2:

            lookup = [x for x in loads.iloc[i].name]

            # lookup.append(loads.iloc[i][0])

            try:

                loads.loc[[tuple(lookup)], 'hour'] = \
                    ls.xs(lookup, level=df_level).hour[0]

            # If the peak or min load is an average, it will throw a
            # KeyError. Use the mean hour by daytype as an alternate value.
            except KeyError:

                mean_hour = np.round(
                    loads.hour.mean(level=1)[lookup[1]], 0
                    )

                loads.loc[[tuple(lookup)], 'hour'] = mean_hour

            # else:
            #
            #     print('i:', i)
            #
            #     print('loads:', loads)
            #
            #     lookup = loads.iloc[i].values[0:2]
            #
            #     print('lookup 249', lookup)
            #
            #     print(ls.head())
            #
            #     try:
            #
            #         loads.loc[[tuple(lookup)], 'hour'] = \
            #             ls.xs(lookup, level=df_level).hour[0]
            #
            #     # If the peak or min load is an average, it will throw a
            #     # KeyError. Use the mean hour by daytype as an alternate value.
            #     except KeyError:
            #
            #         mean_hour = np.round(
            #             loads.hour.mean(level=1)[lookup[1]], 0
            #             )
            #
            #         loads.loc[[tuple(lookup)], 'hour'] = mean_hour

        loads['type'] = min_or_max

        loads.reset_index(inplace=True)

        return loads

    def select_load_data(self, naics, emp_size):
        """
        Choose load characteristics from eith EPA or EPRI data based
        on NAICS code and employment size class.
        """

        # Preserve original NAICS
        naics_og = naics

        large_sizes = ['n50_99','n100_249','n250_499','n500_999','n1000',
                       'ghgrp']

        type_epa = [p for p in itertools.product(
            self.usepa_lf.NAICS12.unique(), large_sizes
            )]

        source = 'epri'

        # Select EPA load factors for large facilities and if NAICS is in
        # EPA data.
        if (naics, emp_size) in type_epa:

            source = 'epa'

            lf = self.usepa_lf.set_index(['NAICS12']).xs(naics)[
                ['month', 'HEAT_INPUT_MMBtu']
                ]

            lf.reset_index(drop=True, inplace=True)

            # Check if there are 12 months of data. If not, fill with
            # average.
            if len(lf) < 12:

                lf = pd.concat([lf, pd.Series(np.array(range(1,13)))],
                               axis=1)

                lf.month.update(lf[0])

                lf.fillna(lf.HEAT_INPUT_MMBtu.mean(), inplace=True)

                lf.drop([0], axis=1, inplace=True)

            lf.columns = ['month', 'load_factor']

            ls = self.usepa_ls[self.usepa_ls.NAICS12 == naics]

        else:

            try:
                # Also drop manufacturing average (SIC == 33)
                lf = self.epri_lf[self.epri_lf.SIC != 33].set_index(
                    'NAICS12'
                    ).xs(naics)['load_factor'].mean()

                ls = self.epri_ls[self.epri_ls.NAICS12 == naics]

            # If NAICS is not in EPRI data, match at a higher aggregation.
            # Selected load factor is average of the higher aggregation
            # NAICS.
            except KeyError:

                print('no intial NAICS match')

                load_naics_matching = pd.concat(
                    [self.epri_lf.NAICS12, self.epri_lf.NAICS12], axis=1
                    )

                load_naics_matching.columns = ['NAICS12', 'naics_match']

                if len(str(naics)) < 6:

                    n = 6 - len(str(naics))

                else:
                    n = 1

                while naics not in load_naics_matching.naics_match.unique():

                    load_naics_matching.naics_match.update(
                        load_naics_matching.naics_match.apply(
                            lambda x: int(str(x)[0:len(str(x))-n])
                            )
                        )

                    if naics in load_naics_matching.naics_match.unique():

                        break

                    else:

                        naics = str(naics)

                        naics = int(naics[0:len(naics)-1])

                        n = 1

                # Map naics_match from epri_lf to epri_ls
                ls = self.epri_ls.set_index('NAICS12').join(
                    load_naics_matching.set_index('NAICS12')
                    ).reset_index()

                lf = self.epri_lf.set_index('NAICS12').join(
                    load_naics_matching.set_index('NAICS12')
                    ).reset_index()

                # Also drop manufacturing average (SIC == 33)
                lf = lf[(lf.naics_match == naics) & (lf.SIC != 33)]

                lf = lf['load_factor'].mean()

                ls = ls[ls.naics_match == naics]

            ls = ls.groupby(
                ['hour', 'daytype'], as_index=False
                ).load.mean()

            lf = pd.concat(
                [pd.Series(np.array(range(1,13), ndmin=1)),
                 pd.Series(np.repeat(lf, 12))], axis=1
                )

            lf.columns = ['month','load_factor']

        peak_min_loads = pd.concat(
            [self.select_min_peak_loads(ls, l) for l in ['min', 'max']],
            axis=0, ignore_index=True
            )

        peak_min_loads['source'] = source

        return lf, peak_min_loads
