
import pandas as pd
import calc_elect_ghgs
import breakout_other_fuels
import numpy as np

class Emissions:

    def __init__(self, years):

        # Define as a range or list
        if (type(years) != list) | (type(years)!=range):

            years = [years]

        self.years = years

        # Define global warming potentials (100-year IPCC FAR)
        gwp = {'CH4': 25, 'N2O': 298}

        # Define standard emission factors (from EPA) for MECS Fuel types
        self.std_efs = pd.DataFrame(
            [['Coal',np.nan,94.67,11,1.6],
             ['Coke_and_breeze',np.nan,113.67,11,1.6],
             ['Natural_gas',np.nan,53.06,1,0.1],
             ['Diesel',np.nan,73.96,3,0.6],
             ['Other','Petroleum_coke', 102.41,32,4.2],
             ['Residual_fuel_oil', np.nan,75.10,3,0.6],
             ['LPG_NGL',np.nan,61.71,3,0.6],
             ['Other', 'Waste_gas',59,3,.6],
             ['Other', 'Biomass', np.mean([118.17,105.51,93.8]),
              np.mean([32,32,7.2]), np.mean([4.2,4.2,3.6])],
             ['Other', 'Steam',66.33,1.25,0.125],
             ['Other', 'Waste_oils_tars_waste_materials', 74,3,0.6]
              ],
            columns=['MECS_FT', 'MECS_FT_byp', 'kgCO2_per_mmbtu',
                     'gCH4_per_mmbtu','gN2O_per_mmbtu']
            )

        # Convert CH4 and N2O emissions to CO2e; total to MMTCO2e/MMBtu
        self.std_efs['MTCO2e_per_MMBtu'] = (self.std_efs.kgCO2_per_mmbtu +\
            self.std_efs.gCH4_per_mmbtu.multiply(gwp['CH4']/1000) +\
                self.std_efs.gN2O_per_mmbtu.multiply(gwp['N2O']/1000))/1000

        ghgrp_emissions = pd.read_parquet(
            '../results/ghgrp_energy_20200505-1117.parquet', engine='pyarrow',
            columns=['FACILITY_ID', 'REPORTING_YEAR', 'FUEL_TYPE',
                     'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND', 'MTCO2e_TOTAL',
                     'MMBtu_TOTAL']
            )

        ghgrp_emissions = pd.DataFrame(
            ghgrp_emissions[ghgrp_emissions.REPORTING_YEAR.isin(years)]
            )

        of = breakout_other_fuels.Other_fuels(2014)

        def map_byproducts_ghgrp(ghgrp_data):

            # Map aggregated fuel types to GHGRP fuel types
            ghgrp_data = of.map_GHGRP_fueltypes(ghgrp_data, 'MECS_FT_IPF.csv')

            # Map disaggregated fuel types to GHGRP fuel type
            ghgrp_data = of.map_GHGRP_fueltypes(ghgrp_data, 'MECS_FT_byp.csv')

            return ghgrp_data

        ghgrp_emissions = map_byproducts_ghgrp(ghgrp_emissions)

        # Replace Biomass emissions with zero value
        ghgrp_emissions.loc[ghgrp_emissions.MECS_FT_byp == 'Biomass',
                            'MTCO2e_TOTAL'
                            ] = 0

        # Sum emissions (MTCO2e) for specified year(s)
        self.ghgrp_ffc_emissions = ghgrp_emissions.groupby(
                ['FACILITY_ID', 'REPORTING_YEAR', 'MECS_FT', 'MECS_FT_byp']
                ).MTCO2e_TOTAL.sum().dropna()

        # Calculate CO2e intensity (MTCO2e/MMBtu)
        self.ghgrp_CO2e_intensity = self.ghgrp_ffc_emissions.divide(
                ghgrp_emissions.groupby(
                    ['FACILITY_ID', 'REPORTING_YEAR', 'MECS_FT', 'MECS_FT_byp']
                    ).MMBtu_TOTAL.sum().dropna()
                )

        self.ghgrp_CO2e_intensity.name = 'MMTCO2e_per_MMBtu'

        self.ghgrp_CO2e_intensity = pd.DataFrame(self.ghgrp_CO2e_intensity)

        # Import end-use breakdowns
        self.eufrac_ghgrp = \
            pd.read_csv('./calculation_data/eu_frac_GHGRP_2014.csv',
                        usecols=['MECS_NAICS', 'MECS_FT',
                                 'CHP and/or Cogeneration Process',
                                 'Conventional Boiler Use', 'Process Heating'])

        self.eufrac_nonghgrp = \
            pd.read_csv('./calculation_data/eu_frac_nonGHGRP_2014.csv',
                        usecols=['MECS_NAICS', 'MECS_FT',
                                 'CHP and/or Cogeneration Process',
                                 'Conventional Boiler Use', 'Process Heating'])

    def calc_ghgrp_fuel_intensity(self, energy_ghgrp_y):
        """
        NAICS codes of reported GHGRP data may be corrected based on Census
        County Business Patterns data. Final GHGRP GHG intensity is based on
        these corrected NAICS codes.

        GHG intensity is calculated by county, NAICS, and MECS_FT_byp.
        """

        #Drop non-manufacturing industries (MECS_NAICS == 0)
        energy_ghgrp_y = energy_ghgrp_y[energy_ghgrp_y.MECS_NAICS !=0]

        energy_ghgp_y = map_byproducts_ghgrp(energy_ghgp_y)

        energy_ghgrp_y = energy_ghgrp_y.groupby(
                ['FACILITY_ID','MECS_Region', 'COUNTY_FIPS',
                 'PRIMARY_NAICS_CODE','MECS_NAICS','MECS_FT', 'MECS_FT_byp'],
                as_index=False
                ).MMBtu_TOTAL.sum()

        energy_ghgrp_y['COUNTY_FIPS'] = energy_ghgrp_y.COUNTY_FIPS.astype(int)

        energy_ghgrp_y.rename(columns={'PRIMARY_NAICS_CODE':'naics'},
                                       inplace=True)

        final_ghgrp_CO2e_intensity = energy_ghgrp_y.set_index(
            ['REPORTING_YEAR','FACILITY_ID', 'MECS_FT_byp']
            ).MMBtu_TOTAL.multiply(
                self.ghgrp_CO2_intensity.reset_index(
                    'MECS_FT'
                    ).MMTCO2e_per_MMBtu
                )

        final_ghgrp_CO2e_intensity.rename(
            columns={'MMBtu_TOTAL':'MMTCO2e_per_MMBtu'}, inplace=True
            )

        final_ghgrp_CO2e_intensity = final_ghgrp_CO2e_intensity.join(
            energy_ghgrp_y.set_index(
                ['REPORTING_YEAR','FACILITY_ID', 'MECS_FT_byp']
                )['COUNTY_FIPS', 'naics'], how='left'
            )

        final_ghgrp_CO2e_intensity = final_ghgrp_CO2e_intensity.reset_index().groupby(
            ['REPORTING_YEAR', 'COUNTY_FIPS', 'naics', 'MECS_FT',
             'MECS_FT_byp']
             ).MMTCO2e_per_MMBtu.mean()

        # Save for first time
        if not os.path.isfile('./calculation_data/ghgrp_CO2e_intensity.csv'):

            final_ghgrp_CO2e_intensity.to_csv(
                './calculation_data/ghgrp_CO2_intensity.csv', index_col=False
                )

        # Use this fuel intensity for GHGRP data (linked by FACILITY_ID) after
        # corrections for county and NAICS have been made.
        return energy_ghgrp


    def calc_fuel_intensity():
        """
        Calculates the GHG intensity by county, NAICs, and employment size
        class. Emissions from GHGRP-reporting facilities are taken directly
        from EPA data. Emissions from remaining establishments are estimated
        using

        Biogenic emissions resulting from combustion of biomass and biomass
        residuals are not included.
        """

        # Purchased steam emissions factor from
        #https://www.epa.gov/sites/production/files/2020-04/documents/ghg-emission-factors-hub.pdf
        # Assumes steam generated with nat gas at 80% efficiency
# MTCO2e/MMBtu
