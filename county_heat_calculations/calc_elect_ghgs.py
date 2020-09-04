# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 16:38:00 2017

@author: vnarwade
revised by cmcmilla
"""

import pandas as pd
import numpy as np
import os


class electricity():
    """
    Class containing methods used to calculate electricity emission factors
    (MTCO2e/MWh) by county from EPA eGRID data.
    """

    def __init__(self):
        self.data_wd = '../tech_opportunity_analysis/calculation_data'
        self.fips_to_zip_file = os.path.join(self.data_wd,
                                             'COUNTY_ZIP_032014.csv')
        self.e_grid_file = os.path.join(
            self.data_wd, 'power_profiler_zipcode_tool_2014_v7.1_1.xlsx'
            )

    def format_egrid_data(self):
        """

        """

        zip_sub_region_dict = pd.read_excel(
            self.e_grid_file, sheet_name=['Zip-subregion',
                                          'Zip_multiple_Subregions']
            )

        zip_sub_region = pd.concat(
            [zip_sub_region_dict[k] for k in zip_sub_region_dict.keys()],
            axis=0, ignore_index=True
            )

        zip_sub_region = pd.melt(zip_sub_region,
                                 id_vars=['ZIP (character)', 'ZIP (numeric)',
                                          'state'], value_name='SUBRGN')

        return zip_sub_region

    def import_resource_mix(self):
        # Extracted from egrid summary tables for 2014 from
        # https://www.epa.gov/sites/production/files/2020-01/
        # egrid2018_historical_files_since_1996.zip
        resource_mix_file = os.path.join(self.data_wd,
                                         'egrid2014_resource_mix.csv')
        resource_mix = pd.read_csv(resource_mix_file)

        return resource_mix

    def import_fips_zips(self):
        """
        Returns
        -------
        fips_zips : dataframe
            Mapping of county FIPS codes to Zip codes.
        """

        fips_zips = pd.read_csv(self.fips_to_zip_file)
        fips_zips.set_index('ZIP', inplace=True)

        return fips_zips

    def calculate_plant_heatrate(self):
        """

        Returns
        -------
        plant_hr : dataframe
            Plant-level data from 2014 eGRID.
        """
        # Plant-level heat rate data for estimating weighted average heat rate
        # by fuel by eGrid subregion
        plant_hr = pd.read_csv(
            os.path.join(self.data_wd, 'egrid2014_plant_data.csv'),
            usecols=['ORISPL', 'SUBRGN', 'PLFUELCT', 'PLHTRT', 'PLNGENAN']
            ).dropna()

        # Drop negative heat rates
        plant_hr = plant_hr.where(plant_hr.PLHTRT > 0).dropna()

        return plant_hr

    def calc_egrid_emissions_resource_mix(self):
        """
        Estimate county grid emissions factor (metric tons CO2e per MWh),
        resource mix, heat rate (in MMBtu/MWh), and losses as the mean of
        zip code egrid data.

        Returns
        -------
        county_ef_rm : dataframe
            Dataframe of of county heat rates (in MMBtu/MWh) by fuel type
        """

        subregions = self.format_egrid_data()
        subregion_hr = self.calculate_plant_heatrate()
        resource_mix = self.import_resource_mix()
        fips_zips = self.import_fips_zips()

        # Estimate weighted-average heat rate (in by subregion and fuel using
        # plant nominal heat rate and annual net generation.
        subregion_hr['PLFUELCT'] = subregion_hr.PLFUELCT.apply(
            lambda x: x.capitalize()+'_hr'
            )

        subregion_hr.replace({'Gas_hr': 'Natural_gas_hr',
                              'Othf_hr': 'Other_hr',
                              'Ofsl_hr': 'Other_fossil_hr'}, inplace=True)

        # Calculate Generation-weighted heat rate by subregion.
        # PLHTRT in Btu/kWh
        subregion_hr = pd.DataFrame(
            subregion_hr.groupby(['SUBRGN', 'PLFUELCT']).apply(
                lambda x: np.average(x['PLHTRT'], weights=x['PLNGENAN'])
                )
            )

        # Convert to MMBtu/MWh
        subregion_hr = subregion_hr*1000/10**6
        subregion_hr.reset_index(inplace=True)
        subregion_hr = subregion_hr.pivot(index='SUBRGN', columns='PLFUELCT',
                                          values=0)

        hr_cols = subregion_hr.columns

        egrid_ef = pd.read_excel(self.e_grid_file,
                                 sheet_name='eGRID Subregion Emission Factor',
                                 skiprows=[0, 1, 2])

        subregions_ef_rm = pd.merge(subregions, egrid_ef, on=['SUBRGN'],
                                    how='inner')
        subregions_ef_rm = pd.merge(subregions_ef_rm, resource_mix,
                                    on=['SUBRGN'], how='left')
        subregions_ef_rm = pd.merge(subregions_ef_rm, subregion_hr,
                                    left_on=['SUBRGN'], right_index=True,
                                    how='left')

        new_cols = list(set(['SRCO2RTA', 'SRCH4RTA', 'SRN2ORTA']).union(
            resource_mix.columns[1:], subregion_hr.columns
            ))

        subregions_ef_rm = subregions_ef_rm.groupby(
            ['ZIP (numeric)'], as_index=False
            )[new_cols].mean()

        subregions_ef_rm.rename(columns={'ZIP (numeric)': 'ZIP'}, inplace=True)

        # Convert emissions fractors from lb/MWh to metric tons CO2e per MWh
        # (MTCO2e/MWh)
        subregions_ef_rm.loc[:, 'MTCO2e_per_MWh'] = \
            (subregions_ef_rm.SRCO2RTA + subregions_ef_rm.SRCH4RTA * 25 +
             subregions_ef_rm.SRN2ORTA * 298) * (0.453592 / 10**3)

        subregions_ef_rm = subregions_ef_rm.set_index('ZIP').join(fips_zips,
                                                                  how='left')
        final_cols = list(set([
            'grid_losses', 'MTCO2e_per_MWh', 'Natural_gas', 'Coal', 'Oil',
            'Other_fossil', 'Solar', 'Biomass', 'Other', 'Hydro', 'Wind',
            'Nuclear', 'Geothermal'
            ]).union(hr_cols))

        county_ef_rm = subregions_ef_rm.reset_index().groupby(
            'COUNTY_FIPS', as_index=False
            )[final_cols].mean()

        county_ef_rm['COUNTY_FIPS'] = county_ef_rm.COUNTY_FIPS.astype('int')

        county_ef_rm['MECS_FT'] = 'Net_electricity'

        return county_ef_rm

    @staticmethod
    def calculate_elect_ghgs(county_ef_rm, county_electricity):
        """
        Calculate

        Parameters
        ----------
        county_electricity : dataframe

        Returns
        -------
        county_elect_ghgs : dataframe

        """

        county_elect_ghgs = pd.merge(
            county_ef_rm[['MTCO2e_per_MWh', 'COUNTY_FIPS']],
            county_electricity, on='COUNTY_FIPS', how='left'
            )

        county_elect_ghgs.loc[:, 'MTCO2e'] = county_elect_ghgs.MMBtu.multiply(
            county_elect_ghgs.MTCO2e_per_MWh * 0.293297222
            )

        return county_elect_ghgs

    @staticmethod
    def calculate_elect_fuel(county_ef_rm, county_elect_use):
        """
        Calculate the fossil fuel used for grid electricity to meet county
        demand, inclusive of grid losses.

        Parameters
        ----------
        county_ef_rm : dataframe
            County weighted average heat rate by generator type. Calculated
            from EPA eGRID data by the `calc_egrid_emissions_resource_mix`
            method.

        Returns
        -------
        county_elect_fuel : dataframe
            MMBtu of fossil fuels used by grid electricity to meet county
            electricity use, defined by NAICS code and employment size class
        """
        # Heat rate in MMBtu/MWh
        county_elect_fuel = pd.merge(
            county_ef_rm[['COUNTY_FIPS', 'Coal_hr', 'Other_hr', 'grid_losses',
                          'Other_fossil_hr', 'Oil_hr', 'Natural_gas_hr']],
            county_elect_use, on='COUNTY_FIPS', how='left'
            )

        # Account for grid losses
        electricity_fuel = county_elect_fuel.MMBtu.multiply(
            county_elect_fuel.grid_losses+1
            )

        # Multiply by heat rate, converting from MMBtu/MWh to MMBtu/MMBtu
        electricity_fuel = \
            county_elect_fuel[['Coal_hr', 'Other_hr', 'Other_fossil_hr',
                               'Oil_hr', 'Natural_gas_hr']].multiply(
                                electricity_fuel, axis=0
                                ) * 0.293297222  # unit conversion

        county_elect_fuel.update(electricity_fuel, overwrite=True)
        county_elect_fuel.rename(columns={
            'Coal_hr': 'Coal_MMBtu', 'Other_hr': 'Other_MMBtu',
            'Other_fossil_hr': 'Other_fossil_MMBtu', 'Oil_hr': 'Oil_MMBtu',
            'Natural_gas_hr': 'Natural_gas_MMBtu'
            })
        county_elect_fuel.drop(['grid_losses', 'fips_matching',
                                'MECS_NAICS_dummies', 'est_count',
                                'MECS_NAICS', 'MECS_NAICS_dummies_mecs',
                                'fipstate', 'fipscty'], axis=1, inplace=True)

        return county_elect_fuel
