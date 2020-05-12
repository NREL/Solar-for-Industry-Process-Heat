import pandas as pd
import calc_elect_ghgs
import breakout_other_fuels
import Match_MECS_NAICS
import numpy as np

class Emissions:

    def __init__(self, years):

        # Define as a range or list
        if (type(years) != list) | (type(years)!=range):

            years = [years]

        self.years = years

        self.data_dir = './calculation_data/'

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

        # Convert CH4 and N2O emissions to CO2e; total to MTCO2e/MMBtu
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

        # Save fuel breakout by MECS byproducts & other. Represented as
        # a portion of MECS_FT, aggregated by REPORTING_YEAR and FACILITY_ID
        def calc_ghgrp_fuel_agg(ghgrp_data):

            ghgrp_other_fuel_agg = ghgrp_data.groupby(
                ['REPORTING_YEAR', 'FACILITY_ID','MECS_FT', 'MECS_FT_byp']
                ).MMBtu_TOTAL.sum()

            ghgrp_other_fuel_agg = ghgrp_other_fuel_agg.divide(
                ghgrp_other_fuel_agg.sum(level=[0,1,2])
                )

            return ghgrp_other_fuel_agg

        if not os.path.exists(
            os.path.join(self.data_dir,'ghgrp_other_fuel_agg.csv')
            ):

            self.ghgrp_other_fuel_agg = calc_ghgrp_fuel_agg(ghgrp_emissions)

            self.ghgrp_other_fuel_agg.to_csv(
                os.path.join(self.data_dir,'ghgrp_other_fuel_agg.csv')
                )

        else:

            self.ghgrp_other_fuel_agg.read_csv(
                os.path.join(self.data_dir,'ghgrp_other_fuel_agg.csv')
                )

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

        self.ghgrp_CO2e_intensity.name = 'MTCO2e_per_MMBtu'

        self.ghgrp_CO2e_intensity = pd.DataFrame(self.ghgrp_CO2e_intensity)

        # Import end-use breakdowns
        self.eufrac_ghgrp = \
            pd.read_csv(os.path.join(self.data_dir,'eu_frac_GHGRP_2014.csv'),
                        usecols=['MECS_NAICS', 'MECS_FT',
                                 'CHP and/or Cogeneration Process',
                                 'Conventional Boiler Use', 'Process Heating'])

        self.eufrac_nonghgrp = \
            pd.read_csv(os.path.join(self.data_dir,'eu_frac_nonGHGRP_2014.csv'),
                        usecols=['MECS_NAICS', 'MECS_FT',
                                 'CHP and/or Cogeneration Process',
                                 'Conventional Boiler Use', 'Process Heating'])


        # import MECS other fuel disaggregation
        self.mecs_other_disagg = \
            pd.read_csv(os.path.join(self.data_dir,'MECS_byp_breakout.csv'))

        # import MECS energy intensities by region, NAICS, and fuel type
        self.mecs_energy_intensity = \
            pd.read_csv(os.path.join(self.data_dir,'MECS_byp_breakout.csv'))

    def calc_ghgrp_and_fuel_intensity(self, energy_ghgrp_y):
        """
        NAICS codes of reported GHGRP data may be corrected based on Census
        County Business Patterns data. Final GHGRP GHG intensity and
        fuel disaggregation are based on these corrected NAICS codes.

        GHG intensity and fuel disaggregation are calculated by county, NAICS,
        and MECS_FT_byp.
        """

        #Drop non-manufacturing industries (MECS_NAICS == 0)
        energy_ghgrp_y = energy_ghgrp_y[energy_ghgrp_y.MECS_NAICS !=0]

        energy_ghgrp_y = map_byproducts_ghgrp(energy_ghgp_y)

        energy_ghgrp_y = energy_ghgrp_y.groupby(
                ['FACILITY_ID','MECS_Region', 'COUNTY_FIPS',
                 'PRIMARY_NAICS_CODE','MECS_NAICS','MECS_FT', 'MECS_FT_byp'],
                as_index=False
                ).MMBtu_TOTAL.sum()

        energy_ghgrp_y['COUNTY_FIPS'] = energy_ghgrp_y.COUNTY_FIPS.astype(int)

        energy_ghgrp_y.rename(columns={'PRIMARY_NAICS_CODE':'naics'},
                                       inplace=True)

        final_ghgrp_fuel_disagg = energy_ghgrp_y.groupby(
            ['COUNTY_FIPS','PRIMARY_NAICS_CODE', 'MECS_FT', 'MECS_FT_byp']
            ).MMBtu_TOTAL.sum()

        final_ghgrp_fuel_disagg = final_ghgrp_fuel_disagg.divide(
            final_ghgrp_fuel_disagg.sum(level=[0,1,2])
            ).reset_index()

        final_ghgrp_CO2e_intensity = energy_ghgrp_y.set_index(
            ['REPORTING_YEAR','FACILITY_ID', 'MECS_FT_byp']
            ).MMBtu_TOTAL.multiply(
                self.ghgrp_CO2_intensity.reset_index(
                    'MECS_FT'
                    ).MTCO2e_per_MMBtu
                )

        final_ghgrp_CO2e_intensity.rename(
            columns={'MMBtu_TOTAL':'MTCO2e_per_MMBtu'}, inplace=True
            )

        final_ghgrp_CO2e_intensity = final_ghgrp_CO2e_intensity.join(
            energy_ghgrp_y.set_index(
                ['REPORTING_YEAR','FACILITY_ID', 'MECS_FT_byp']
                )['COUNTY_FIPS', 'naics'], how='left'
            )

        final_ghgrp_CO2e_intensity = \
            final_ghgrp_CO2e_intensity.reset_index().groupby(
                ['REPORTING_YEAR', 'COUNTY_FIPS', 'naics', 'MECS_FT',
                 'MECS_FT_byp'], as_index=False
                ).MTCO2e_per_MMBtu.mean()

        # Save for first time
        if not os.path.isfile(
            os.path.join(self.data_dir,'ghgrp_CO2e_intensity.csv')
            ):

            final_ghgrp_CO2e_intensity.to_csv(
                os.path.join(self.data_dir,'ghgrp_CO2_intensity.csv'),
                index_col=False
                )

        return final_ghgrp_CO2e_intensity, final_ghgrp_fuel_disagg

    def calc_fuel_emissions(self, final_ghgrp_CO2e_intensity,
                            final_ghgrp_fuel_disagg, county_energy_temp):

        """
        Calculates the GHG intensity by county, NAICs, and employment size
        class. Emissions from GHGRP-reporting facilities are taken directly
        from EPA data. Emissions from remaining establishments are estimated
        using

        Biogenic emissions resulting from combustion of biomass and biomass
        residuals are not included.
        """

        if type(county_energy_temp.index) != pd.core.indexes.range.RangeIndex:

            county_energy_temp.reset_index(inplace=True)

        ghgrp_emissions = pd.DataFrame()

        #Calculate ghgrp emissions first, if contained in DataFrame.
        try:

            ghgrp_emissions = \
                county_energy_temp[county_energy_temp.Emp_Size == 'ghgrp']

        except KeyError as e:

            print('No GHGRP emissions in dataframe')

        else:

            # Split out byproducts
            ghgrp_emissions = pd.merge(
                ghgrp_emissions, final_ghgrp_fuel_disagg, how='left',
                on=['COUNTY_FIPS', 'naics', 'MECS_FT'], suffixes=('', '_inten')
                )

            ghgrp_emissions = pd.merge(
                ghgrp_emissions, final_ghgrp_CO2e_intensity, how='left',
                on=['COUNTY_FIPS', 'naics', 'MECS_FT']
                )

            ghgrp_emissions['MTCO2e_TOTAL'] =\
                ghgrp_emissions.MMBtu_TOTAl.multiply(
                    ghgrp_emissions.MMBtu_TOTAL_inten.multiply(
                        ghgrp_emissions.MTCO2e_per_MMBtu
                        )
                    )

            ghgrp_emissions['MTCO2e_TOTAL'] = \
                ghgrp_emissions.MTCO2e_TOTAL.astype('float32')

        # Calculate GHG emissions for non-GHGRP data
        finally:

            nonGHGRP_emissions = \
                county_energy_temp[county_energy_temp.Emp_Size != 'ghgrp']

            # Work around for re-matching naics to MECS_NAICS
            naics6d = pd.DataFrame(
                nonGHGRP_emissions.naics.unique(), columns=['naics'],
                index=range(0, len(nonGHGRP_emissions.naics.unique()))
                )

            naics6d = Match_MECS_NAICS.Match(naics6d, 'naics',
                                             naics_vintage=2012)

            nonGHGRP_emissions = pd.merge(
                nonGHGRP_emissions, naics6d, on=['naics'], how='left'
                )

            mecs_GHG_disagg = pd.merge(
                self.mecs_other_disagg, self.std_efs.dropna()[
                    ['MECS_FT_byp', 'MTCO2e_per_MMBtu']
                    ], how='left', on='MECS_FT_byp'
                )

            mecs_GHG_disagg.MTCO2e_per_MMBtu.update(
                mecs_GHG_disagg.Byp_fraction.multiply(
                    mecs_GHG_disagg.MTCO2e_per_MMBtu
                    )
                )

            mecs_GHG_disagg['MTCO2e_per_MMBtu'] = \
                mecs_GHG_disagg.MTCO2e_per_MMBtu.astype('float32')

            mecs_GHG_disagg.set_index(
                ['MECS_Region', 'MECS_NAICS', 'MECS_FT', 'End_use'],
                inplace=True
                )

            nonGHGRP_emissions.set_index(
                ['MECS_Region', 'MECS_NAICS', 'MECS_FT', 'End_use'],
                inplace=True
                )

            nonGHGRP_emissions = pd.merge(
                nonGHGRP_emissions, mecs_GHG_disagg['MTCO2e_per_MMBtu'],
                left_index=True, right_index=True, how='left'
                )

            nonGHGRP_emissions.reset_index(
                ['MECS_Region', 'MECS_NAICS', 'End_use'], drop=False,
                inplace=True
                )

            nonGHGRP_emissions.update(
                self.std_efs[self.std_efs.MECS_FT_byp.isnull()].set_index(
                    'MECS_FT'
                    ).MTCO2e_per_MMBtu, overwrite=False
                )

            nonGHGRP_emissions.reset_index(inplace=True)

            nonGHGRP_emissions.MTCO2e_per_MMBtu.update(
                nonGHGRP_emissions.MTCO2e_per_MMBtu.multiply(
                    nonGHGRP_emissions.MMBtu
                    )
                )

            nonGHGRP_emissions.rename(
                columns={'MTCO2e_per_MMBtu': 'MTCO2e_TOTAL'}, inplace=True
                )

            nonGHGRP_emissions['MTCO2e_TOTAL'] = \
                nonGHGRP_emissions.astype('float32')

            all_emissions = pd.concat([nonGHGRP_emissions, ghgrp_emissions],
                                      axis=0, ignore_index=True)

            return all_emissions
