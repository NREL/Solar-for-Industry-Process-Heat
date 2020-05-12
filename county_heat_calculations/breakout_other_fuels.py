
import pandas as pd
import os

class Other_fuels:

    def __init__(self, year):

        self.year = year

        formatted_files = ['bio'+str(self.year)+'_formatted.csv',
                           'byp'+str(self.year)+'_formatted.csv',
                           'table5_2_'+str(self.year)+'_formatted.csv',
                           'Tables3_'+str(self.year)+'_formatted.xlsx']

        for f in formatted_files:

            if f in os.listdir('./calculation_data/'):

                continue

            else:

                print(f, 'does not exist in ./calculation_data/', '\n',
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

    def breakout_MECS_other(self):
        """
        Breakout MECS intensities into byproducts
        """

        # MECS Table3.6
        bio_table = pd.read_csv('./calculation_data/' + \
                                'bio'+str(self.year)+'_formatted.csv')

        #MECS Table 3.5
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

            try:

                df['naics'].replace({'31-33': 31}, inplace=True)

            except TypeError as e:

                print('Formatting error in bio, byp table import:', e)

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

        # Import table3_2 for total "Other"
        table3_2 = pd.read_excel(
            './calculation_data/Tables3_2014_formatted.xlsx',
            sheet_name='Table3.2', usecols=['NAICS', 'Region', 'Other']
            )

        # Correct negative value
        table3_2.update(table3_2.where(table3_2.Other>0).Other.fillna(0))

        table3_2.replace({'United States':'us'}, inplace=True)

        table3_2['Region'] = table3_2.Region.apply(lambda x: x.lower())

        table3_2.set_index(['NAICS', 'Region'], inplace=True)

        table3_2.index.names = ['MECS_NAICS', 'MECS_Region']

        table3_2.sort_index(inplace=True)

        #Totals show up under NAICS 339, creating duplicate index entries.
        totals3_2 = \
            table3_2[table3_2.index.duplicated(keep='first')].reset_index()

        totals3_2['MECS_NAICS'] = 31

        totals3_2.set_index(['MECS_NAICS', 'MECS_Region'], inplace=True)

        table3_2 = table3_2.loc[~table3_2.index.duplicated(keep='first')]

        table3_2 = pd.concat([table3_2, totals3_2], axis=0)

        other_table = pd.merge(
            byp_table.set_index(['naics', 'region']),
            pd.DataFrame(
                bio_table.set_index(['naics', 'region'])[
                    ['pulp_liq', 'total_bio']
                    ].sum(axis=1)
                    ), left_index=True, right_index=True, how='left'
            )

        other_table.index.names = ['MECS_NAICS', 'MECS_Region']

        other_table.sort_index(inplace=True)

        other_table.rename(
            columns={0: 'Biomass', 'pet_coke': 'Petroleum_coke',
                     'blast_furnace': 'Blast_furnace_coke_oven_gases',
                     'waste_gas': 'Waste_gas',
                     'waste_oils': 'Waste_oils_tars_waste_materials',
                     'total':'Total'}, inplace=True
            )

        other_table.Biomass.fillna(0, inplace=True)

        other_table.Total.update(
            other_table.Total.subtract(
                other_table[['wood_chips', 'pulp_liq']].sum(axis=1)
                ).add(
                    other_table.Biomass
                    )
            )

        #  Calculate purchased steam as difference between total Other
        # and total byproducts and biomass
        # New total may be > original total of other from Table 3.2
        steam = pd.concat([table3_2.Other, other_table.Total], axis=1)

        def check_totals(r):

             if r['Other'] - r['Total'] < 0:

                 return 0

             else:

                return r['Other'] - r['Total']

        steam['Total_star'] = steam.apply(lambda x: check_totals(x), axis=1)

        table3_2.loc[:, 'Steam'] = steam['Total_star']

        # Subtract blast furnace and coke oven gases from total other because
        #these are captured by GHGRP data and non-integrated mills would not
        # have blast furnace gas emissions.
        other_table.Total.update(
            other_table.Total.subtract(
                other_table.Blast_furnace_coke_oven_gases
                )
            )

        # Remove Blast furnace/Coke oven gas values
        other_table.loc[:, 'Blast_furnace_coke_oven_gases'] = 0

        # List of fuels to diaggregate Other fuel use by.
        other_disag = ['Waste_gas','Petroleum_coke',
                       'Waste_oils_tars_waste_materials','Biomass', 'Steam'
                       ]

        other_table = table3_2[['Other', 'Steam']].join(
            other_table, how='left'
            )

        other_table.to_csv('final_before_divide.csv', header=True)

        other_table.update(
            other_table[other_disag].divide(
                other_table[other_disag].sum(axis=1), axis=0
                )
            )

        other_table.fillna(0, inplace=True)

        # Prep tables for merge
        other_table = pd.melt(
            other_table[other_disag].reset_index(),
            id_vars=['MECS_NAICS', 'MECS_Region'], var_name='MECS_FT_byp'
            )

        eu_frac_nonGHGRP = pd.melt(
            eu_frac_nonGHGRP.reset_index(),
            id_vars=['MECS_NAICS', 'MECS_FT'], var_name='End_use'
            )

        other_table = pd.merge(
            other_table, eu_frac_nonGHGRP, on='MECS_NAICS', how='outer'
            )

        other_table['Byp_fraction'] = \
            other_table.value_x.multiply(other_table.value_y)

        other_table.drop(['value_x', 'value_y'], axis=1, inplace=True)

        other_table['MECS_Region'] = other_table.MECS_Region.apply(
            lambda x: x.capitalize()
            )

        other_table.replace({'Us': 'US'}, inplace=True)

        other_table.to_csv('./calculation_data/MECS_byp_breakout.csv')

        return other_table

        # # Later, multiply this by eu_frac_nonGHGRP to get
