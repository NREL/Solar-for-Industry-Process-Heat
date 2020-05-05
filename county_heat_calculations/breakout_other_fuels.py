
import pandas as pd
import os

class Other_fuels:

    def __init__(self, year):

        self.year = year

        formatted_files = ['bio'+str(self.year)+'_formatted.csv',
                           'byp'+str(self.year)+'_formatted.csv',
                           'table5_2_'+str(self.year)+'_formatted.csv']

        for f in formatted_files:

            if f in os.listdir('./calculation_data/'):

                continue

            else:

                print(f, 'does not exist in /calculation_data/', '\n',
                      'Please create it')


    @staticmethod
    def map_GHGRP_fueltypes(energy_ghgrp, ft_file):
        """
        Map GHGRP fuel types to MECS fuel types, either using aggregated
        (ft_file = MECS_FT_IPF.csv) or diaggregated fuel types (ft_file =
        MECS_FT_byp.csv).
        Returns energy_ghgrp dataframe with new fuel type column.
        """

        if ft_file == 'MECS_FT_IPF.csv':

            new_ft_column = 'MECS_FT'

        else:

            new_ft_column = 'MECS_FT_byp'

        ghgrp_mecs_ftxwalk = pd.read_csv('./calculation_data/'+ft_file)

        # Pull out all fuel types used in GHGRP
        ghgrp_fuels = pd.DataFrame(pd.concat(
            [pd.Series(energy_ghgrp[c].dropna()) for c in ['FUEL_TYPE_OTHER',
                                                           'FUEL_TYPE_BLEND',
                                                           'FUEL_TYPE']],
            axis=0, ignore_index=False
            )).sort_index()

        ghgrp_fuels.loc[:, 'index'] = ghgrp_fuels.index

        ghgrp_fuels = pd.merge(
            ghgrp_fuels, ghgrp_mecs_ftxwalk, left_on=0,
            right_on='EPA_FUEL_TYPE', how='left')

        ghgrp_fuels.set_index('index', inplace=True)

        # Fill all remaining fuels as "Other"
        ghgrp_fuels.fillna('Other', inplace=True)

        energy_ghgrp = energy_ghgrp.join(ghgrp_fuels[new_ft_column], how='left')

        return energy_ghgrp

    def breakout_other(self):
        """
        Breakout MECS intensities into byproducts
        """
        #From byproducts Table3.5: Waste_oils_tars_waste_materials,
        #Blast_furnace_coke_oven_gases, Waste_gas, Petroleum_coke
        #From Wood and wood-related Table3.6: create sum of pulping liquor
        # and total biomass for Biomass total.
        bio_table = pd.read_csv('./calculation_data/' + \
                                'bio'+str(self.year)+'_formatted.csv')

        byp_table = pd.read_csv('./calculation_data/' +\
                                'byp'+str(self.year)+'_formatted.csv')

        eu_frac_nonGHGRP = pd.read_csv(
            './calculation_data/eu_frac_nonGHGRP_' + str(2014) + '.csv',
            index_col=['MECS_NAICS'],
            usecols=['MECS_NAICS', 'CHP and/or Cogeneration Process',
                     'Conventional Boiler Use', 'Process Heating', 'MECS_FT']
            )

        eu_frac_nonGHGRP = pd.DataFrame(
            eu_frac_nonGHGRP[eu_frac_nonGHGRP.MECS_FT == 'Other']
            )

        def format_biobyp_tables(df):

            df['naics'].replace({'31-33': 31}, inplace=True)

            for c in df.columns:

                if c in ['region', 'n_naics', 'naics_desc']:

                    continue

                elif c == 'naics':

                    df[c] = df[c].astype('int')

                else:

                    df[c] = df[c].astype('float32')

            return df

        bio_table = format_biobyp_tables(bio_table)

        byp_table = format_biobyp_tables(byp_table)

        # eu_table.set_index(['naics', 'end_use'], inplace=True)

        # adj_total = eu_table.xs(
        #     'TOTAL FUEL CONSUMPTION', level=1
        #     ).total.subtract(
        #             eu_table.xs('End Use Not Reported',
        #                         level=1).total, level=1
        #             )
        #
        # adj_total.name = 'adj_total'
        #
        # eu_table.reset_index('end_use', inplace=True)

        byp_table_final = pd.merge(
            byp_table.set_index(['naics', 'region']),
            pd.DataFrame(
                bio_table.set_index(['naics', 'region'])[
                    ['pulp_liq', 'total_bio']
                    ].sum(axis=1)
                    ), left_index=True, right_index=True, how='left'
            )

        byp_table_final.rename(
            columns={0: 'Biomass', 'pet_coke': 'Petroleum_coke',
                     'blast_furnace': 'Blast_furnace_coke_oven_gases',
                     'waste_gas': 'Waste_gas',
                     'waste_oils': 'Waste_oils_tars_waste_materials'},
            inplace=True
            )

        byp_table_final.index.names = ['MECS_NAICS', 'MECS_REGION']

        # Table 3.5 reports values for NAICS codes not provided in Table 3.6
        missing_bio = byp_table_final[byp_table_final.Biomass.isna()]

        missing_bio.Biomass.update(missing_bio.wood_chips)

        byp_table_final.Biomass.update(missing_bio.Biomass)

        # Remove Blast furnace/Coke oven gas values because these are
        # captured by GHGRP data and non-integrated mills would not have
        # blast furnace gas emissions.
        byp_table_final.loc[:, 'Blast_furnace_coke_oven_gases'] = 0

        # List of byproduct fuels to diaggregate Other fuel use by.
        byp = ['Blast_furnace_coke_oven_gases', 'Waste_gas', 'Petroleum_coke',
               'Waste_oils_tars_waste_materials', 'Biomass']

        byp_table_final.total.update(byp_table_final[byp].sum(axis=1))

        byp_table_final.update(byp_table_final[byp].divide(
            byp_table_final.total, axis=0)
            )

        byp_table_final.fillna(0, inplace=True)

        # Prep table for merge
        byp_table_final = pd.melt(
            byp_table_final[byp].reset_index(),
            id_vars=['MECS_NAICS', 'MECS_REGION'], var_name='MECS_FT_byp'
            )

        byp_table_final = pd.merge(
            byp_table_final[byp], eu_frac_nonGHGRP, left_index=True,
            right_index=True, how='outer'
            )

        return byp_table_final

        # Later, multiply this by eu_frac_nonGHGRP to get
