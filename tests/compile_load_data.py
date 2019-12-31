
import pandas as pd
import os

class LoadData:

    def __init__(self):

        self.datadir = 'calculation_data/'

        self.nfiles = {'sic_N02_file': '1987_SIC_to_2002_NAICS.csv',
                       'N02_N07_file': '2002_to_2007_NAICS.csv',
                       'N07_N12_file': '2007_to_2012_NAICS.csv'}

        # Load factors from EPRI report. Categorized by SIC.
        self.epri_loadfactors = 'epri_load_factors.csv'

        # Monthly load factors calculated from EPA Air Markets Program data.
        # NAICS are 2007; need to be updated to 2012.
        self.usepa_loadfactors = 'epa_amd_load_factor.csv'

        def import_data(self):

            """
            Import load factor data. Requires adjusting from 1997 SIC to
            2002 NAICS to 2007 NAICS to 2012 NAICS.
            """

            def create_sic_naics_dfs(file_dir, file):

                sic_naics = pd.read_csv(
                        os.path.join('../', file_dir + file)
                        )

                sic_naics = sic_naics.iloc[:, [0, 2]]

                sic_naics.set_index(sic_naics.columns[0], inplace=True)

                return sic_naics

            def create_dict(file_dir, file):

                dict_out = dict(pd.read_csv(
                        os.path.join('../', file_dir + file)
                        ).iloc[:, [0, 2]].values)

                return dict_out

            ndict = {}

            for k, v in self.nfiles.items():

                ndict[k[0:7]] = create_sic_naics_dfs(self.datadir, v)

            # Import load factors derived from EPA data
            usepa_lf = pd.read_csv(
                os.path.join('../', self.datadir+self.usepa_loadfactors),
                index_col='PRIMARY_NAICS_CODE',
                usecols=['PRIMARY_NAICS_CODE', 'month', 'HEAT_INPUT_MMBtu']
                )

            # Import load factors from EPRI report.
            epri_lf = pd.read_csv(
                os.path.join('../', self.datadir+self.epri_loadfactors),
                index_col='SIC'
                )

            # Need to match EPRI's two and three-digit SIC, which have
            # no direct matches in SIC-NAICS crosswalk file.
            sic_3 = ndict['sic_N02'].copy(deep=True).reset_index()

            sic_3['SIC'] = sic_3.SIC.apply(lambda x: int(str(x)[0:3]))

            sic_3 = sic_3[sic_3.SIC.between(199, 400)]

            sic_2 = sic_3.copy(deep=True)

            sic_2['SIC'] = sic_2.SIC.apply(lambda x: int(str(x)[0:2]))

            epri_lf = pd.concat(
                [epri_lf.join(
                    df, how='inner', on='SIC'
                    ) for df in [ndict['sic_N02'], sic_3.set_index('SIC'),
                                 sic_2.set_index('SIC')]],
                axis=0
                )

            # epri_lf = epri_lf.join(ndict['sic_N02'], how='left')

            ndict['N02_N07'].index.name = '2002 NAICS'

            epri_lf = epri_lf.set_index('2002 NAICS').join(
                    ndict['N02_N07']
                    ).reset_index()

            ndict['N07_N12'].columns = ['NAICS12']

            epri_lf = epri_lf.set_index('2007 NAICS Code').join(
                    ndict['N07_N12']
                    ).reset_index()

            usepa_lf = usepa_lf.join(ndict['N07_N12']).reset_index()

            return epri_lf, usepa_lf

        self.epri_lf, self.usepa_lf = import_data(self)
