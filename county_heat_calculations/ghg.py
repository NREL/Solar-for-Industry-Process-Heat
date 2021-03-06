import pandas as pd
import breakout_other_fuels
import Match_MECS_NAICS
import numpy as np
import Match_GHGRP_County_IPH as county_matching
import get_cbp
import os


class Emissions:

    def __init__(self, year):

        self.year = year

        self.data_dir = './calculation_data/'

        # Define global warming potentials (100-year IPCC FAR)
        gwp = {'CH4': 25, 'N2O': 298}

        # Define standard emission factors (from EPA) for MECS Fuel types
        # Set biomass EFs = 0. Reported values for CO2, CH4, and N2O are
        # #np.mean([118.17,105.51,93.8]),np.mean([32,32,7.2]),
        # np.mean([4.2,4.2,3.6])],

        self.std_efs = pd.DataFrame(
            [['Coal', np.nan, 94.67, 11, 1.6],
             ['Coke_and_breeze', np.nan, 113.67, 11, 1.6],
             ['Natural_gas', np.nan, 53.06, 1, 0.1],
             ['Diesel', np.nan, 73.96, 3, 0.6],
             ['Other', 'Petroleum_coke', 102.41, 32, 4.2],
             ['Residual_fuel_oil', np.nan, 75.10, 3, 0.6],
             ['LPG_NGL', np.nan, 61.71, 3, 0.6],
             ['Other', 'Waste_gas', 59, 3, 0.6],
             ['Other', 'Biomass', 0, 0, 0],
             ['Other', 'Waste_oils_tars_waste_materials', 74, 3, 0.6],
             ['Other', 'Steam', 66.33, 1.25, 0.125]],
            columns=['MECS_FT', 'MECS_FT_byp', 'kgCO2_per_mmbtu',
                     'gCH4_per_mmbtu', 'gN2O_per_mmbtu']
            )

        # Convert CH4 and N2O emissions to CO2e; total to MTCO2e/MMBtu
        self.std_efs['MTCO2e_per_MMBtu'] = (
            self.std_efs.kgCO2_per_mmbtu +
            self.std_efs.gCH4_per_mmbtu.multiply(gwp['CH4']/1000) +
            self.std_efs.gN2O_per_mmbtu.multiply(gwp['N2O']/1000)
            )/1000

        # import MECS other fuel disaggregation
        self.mecs_other_disagg = \
            pd.read_csv(os.path.join(self.data_dir, 'MECS_byp_breakout.csv'),
                        index_col=0)

        self.county_data_file = \
            '../results/mfg_eu_temps_20200728_0810.parquet.gzip'

    def calc_mecs_fuel_intensity(self):
        """
        Calculate fuel intensity (disaggregated other fuels) for county data
        estimated from MECS. Used for breaking out 8760 load data into fuels.
        """

        county_data = pd.read_parquet(self.county_data_file)

        county_data = county_data.groupby(
            ['data_source', 'MECS_Region', 'COUNTY_FIPS', 'naics', 'Emp_Size',
             'End_use', 'MECS_FT'], as_index=False
            ).MMBtu.sum()

        county_data_mecs = \
            county_data[county_data.data_source == 'mecs_ipf']

        county_data_mecs = pd.DataFrame(county_data_mecs.groupby(
            ['MECS_Region', 'naics', 'Emp_Size', 'End_use', 'MECS_FT']
            ).MMBtu.sum())

        county_data_mecs['MMBtu'].update(county_data_mecs.MMBtu.divide(
            county_data_mecs.MMBtu.sum(level=[0, 1, 2, 3])
            ))

        naics6d = pd.DataFrame(
            county_data_mecs.index.get_level_values('naics').unique(),
            columns=['naics'],
            index=range(0, len(
                county_data_mecs.index.get_level_values('naics').unique()
                ))
            )

        naics6d = Match_MECS_NAICS.Match(naics6d, 'naics',
                                         naics_vintage=2012)

        county_data_mecs.reset_index(inplace=True)

        county_data_mecs = pd.merge(county_data_mecs,
                                    naics6d[['naics', 'MECS_NAICS']],
                                    on='naics', how='left')

        county_data_mecs = pd.merge(
            county_data_mecs, self.mecs_other_disagg,
            on=['MECS_Region', 'MECS_NAICS', 'MECS_FT', 'End_use'], how='left'
            )

        county_data_mecs.Byp_fraction.fillna(1, inplace=True)

        county_data_mecs.MECS_FT_byp.update(
            county_data_mecs.where(
                county_data_mecs.MECS_FT_byp.isnull()
                ).dropna(thresh=1).MECS_FT
            )

        county_data_mecs = county_data_mecs.where(
            county_data_mecs.Byp_fraction != 0
            ).dropna()

        county_data_mecs.MMBtu.update(county_data_mecs.MMBtu.multiply(
            county_data_mecs.Byp_fraction
            ).astype('float32'))

        county_data_mecs.rename(columns={'MMBtu': 'MMBtu_fraction'},
                                inplace=True)

        county_data_mecs.drop(['MECS_NAICS', 'Byp_fraction'], axis=1,
                              inplace=True)

        # # Group by MECS region
        # county_data_mecs = county_data_mecs.groupby(
        #     ['MECS_Region', 'naics', 'Emp_Size', 'End_use', 'MECS_FT',
        #     'MECS_FT_byp'], as_index=False
        #     ).MMBtu_fraction.mean()

        # Save results
        county_data_mecs.to_csv(
            os.path.join(self.data_dir, 'mecs_fuel_intensity.csv'), index=False
            )

    def calc_ghgrp_intensities(self):
        """
        NAICS codes of reported GHGRP data may be corrected based on Census
        County Business Patterns data. Final GHGRP GHG intensity and
        fuel disaggregation are based on these corrected NAICS codes.

        GHG intensity calculated by county, NAICS, and MECS_FT_byp.
        Fuel disaggregation and intensity includes end use.
        """
        if self.year > 2012:
            naics_column = 'PRIMARY_NAICS_CODE_12'

        else:
            naics_column = 'PRIMARY_NAICS_CODE'

        # This is an updated ghgrp energy file. Bug was fixed on 5/5/2020 that
        # didn't capture MTCO2e_TOTAL values. Energy values are the same
        # as the original calculations.
        ghgrp_energy = pd.read_parquet(
            '../results/ghgrp_energy_20200826-1725.parquet', engine='pyarrow',
            columns=['FACILITY_ID', 'REPORTING_YEAR', 'FUEL_TYPE',
                     'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND', 'COUNTY_FIPS',
                     'MECS_Region', 'MTCO2e_TOTAL', 'PRIMARY_NAICS_CODE',
                     'SECONDARY_NAICS_CODE', 'MMBtu_TOTAL']
            )

        # Drop entries with zero calculated MMBtu
        ghgrp_energy = ghgrp_energy.loc[
            (ghgrp_energy[ghgrp_energy.MMBtu_TOTAL != 0].index), :
            ]

        ghgrp_energy = pd.DataFrame(
            ghgrp_energy[ghgrp_energy.REPORTING_YEAR == self.year]
            )

        of = breakout_other_fuels.Other_fuels(2014)

        # Map aggregated fuel types to GHGRP fuel types
        ghgrp_energy = of.map_GHGRP_fueltypes(ghgrp_energy, 'MECS_FT_IPF.csv')

        # Map disaggregated fuel types to GHGRP fuel type
        ghgrp_energy = of.map_GHGRP_fueltypes(ghgrp_energy, 'MECS_FT_byp.csv')

        # Replace Biomass emissions with zero value
        ghgrp_energy.loc[ghgrp_energy.MECS_FT_byp == 'Biomass',
                         'MTCO2e_TOTAL'] = 0

        # Sum emissions (MTCO2e) for specified year(s)
        ghgrp_ffc_emissions = ghgrp_energy.groupby(
                ['FACILITY_ID', 'REPORTING_YEAR', 'MECS_FT', 'MECS_FT_byp']
                ).MTCO2e_TOTAL.sum().dropna()

        # Calculate CO2e intensity (MTCO2e/MMBtu)
        ghgrp_CO2e_intensity = ghgrp_ffc_emissions.divide(
                ghgrp_energy.groupby(
                    ['FACILITY_ID', 'REPORTING_YEAR', 'MECS_FT', 'MECS_FT_byp']
                    ).MMBtu_TOTAL.sum().dropna()
                )

        ghgrp_CO2e_intensity.name = 'MTCO2e_per_MMBtu'

        ghgrp_CO2e_intensity = pd.DataFrame(ghgrp_CO2e_intensity)

        cbp = get_cbp.CBP(2014)

        tcm = county_matching.County_matching(2014)

        ghgrp_matching = tcm.format_ghgrp(ghgrp_energy, cbp.cbp_matching)

        # Update NAICS codes based on Census Business Patterns Data
        energy_ghgrp_matched = \
            pd.merge(ghgrp_energy,
                     ghgrp_matching[['FACILITY_ID',
                                     naics_column]],
                     on='FACILITY_ID', how='left')

        energy_ghgrp_matched[naics_column] = \
            energy_ghgrp_matched[naics_column].astype('int')

        naics6d = pd.DataFrame(
            energy_ghgrp_matched[naics_column].unique(),
            columns=[naics_column],
            index=range(0, len(energy_ghgrp_matched[naics_column].unique()))
            )

        naics6d = Match_MECS_NAICS.Match(naics6d, naics_column,
                                         naics_vintage=2012)

        energy_ghgrp_matched = pd.merge(
            energy_ghgrp_matched, naics6d, on=naics_column, how='left'
            )

        # Filter out facilities that use PRIMARY_NAICS_CODE == 486210 and
        # NAICS_USED == 0
        energy_ghgrp_matched = energy_ghgrp_matched[
                (energy_ghgrp_matched[naics_column] != 486210) &
                (energy_ghgrp_matched.MECS_NAICS != 0)
                ]

        if naics_column == 'PRIMARY_NAICS_CODE_12':

            energy_ghgrp_matched.drop('PRIMARY_NAICS_CODE', inplace=True,
                                      axis=1)

            energy_ghgrp_matched.rename(
                columns={'PRIMARY_NAICS_CODE_12': 'PRIMARY_NAICS_CODE'},
                inplace=True
                )

        energy_ghgrp_y = energy_ghgrp_matched.groupby(
                ['REPORTING_YEAR', 'FACILITY_ID', 'MECS_Region', 'COUNTY_FIPS',
                 'PRIMARY_NAICS_CODE', 'MECS_NAICS', 'MECS_FT', 'MECS_FT_byp'],
                as_index=False
                ).MMBtu_TOTAL.sum()

        energy_ghgrp_y['COUNTY_FIPS'] = energy_ghgrp_y.COUNTY_FIPS.astype(int)

        energy_ghgrp_y.rename(columns={'PRIMARY_NAICS_CODE': 'naics'},
                              inplace=True)

        ghgrp_byp = energy_ghgrp_y.groupby(
            ['COUNTY_FIPS', 'naics', 'MECS_FT', 'MECS_FT_byp']
            ).MMBtu_TOTAL.sum()

        ghgrp_byp = pd.DataFrame(
            ghgrp_byp.divide(ghgrp_byp.sum(level=[0, 1, 2]))
            )

        county_data = pd.read_parquet(self.county_data_file)

        county_data = county_data.groupby(
            ['data_source', 'COUNTY_FIPS', 'naics', 'Emp_Size', 'End_use',
             'MECS_FT'], as_index=False
            ).MMBtu.sum()

        final_ghgrp_fuel_disagg = \
            county_data[county_data.data_source == 'ghgrp'].groupby(
                ['COUNTY_FIPS', 'naics', 'MECS_FT', 'End_use']
                ).MMBtu.sum()

        final_ghgrp_fuel_disagg = final_ghgrp_fuel_disagg.divide(
            final_ghgrp_fuel_disagg.sum(level=[0, 1, 2])
            )

        final_ghgrp_fuel_disagg = pd.DataFrame(
            final_ghgrp_fuel_disagg.multiply(ghgrp_byp.MMBtu_TOTAL)
            )

        # energy_ghgrp_y.groupby(
        #     ['COUNTY_FIPS', 'MECS_FT', 'naics', 'MECS_FT', 'MECS_FT_byp']
        #     ).MMBtu_TOTAL.sum()
        #
        # final_ghgrp_fuel_disagg = final_ghgrp_fuel_disagg.divide(
        #     final_ghgrp_fuel_disagg.sum(level=[0,1,2,4])
        #     ).reset_index()

        final_ghgrp_fuel_disagg.rename(
            columns={0: 'MMBtu_fraction'}, inplace=True
            )

        final_ghgrp_fuel_disagg.dropna(inplace=True)

        final_ghgrp_fuel_disagg.to_csv(
            os.path.join(self.data_dir,
                         'ghgrp_fuel_disagg_'+str(self.year)+'.csv'),
            index=True
            )

        final_ghgrp_CO2e_intensity = pd.merge(
            energy_ghgrp_y.set_index(
                ['FACILITY_ID', 'REPORTING_YEAR', 'MECS_FT', 'MECS_FT_byp']
                ), ghgrp_CO2e_intensity, left_index=True,
            right_index=True, how='left'
            )

        # Remove MMBtu_TOTAL values of Zero
        final_ghgrp_CO2e_intensity = final_ghgrp_CO2e_intensity.loc[
            (final_ghgrp_CO2e_intensity[
                final_ghgrp_CO2e_intensity.MMBtu_TOTAL != 0
                ].index), :
            ]

        # Created weighted average CO2e intensity by county and naics
        final_ghgrp_CO2e_intensity = pd.DataFrame(
            final_ghgrp_CO2e_intensity.groupby(
                ['REPORTING_YEAR', 'COUNTY_FIPS', 'naics', 'MECS_FT',
                 'MECS_FT_byp']
                 ).apply(lambda x: np.average(x.MTCO2e_per_MMBtu,
                                              weights=x.MMBtu_TOTAL))
            )

        final_ghgrp_CO2e_intensity.rename(
            columns={0: 'MTCO2e_per_MMBtu'}, inplace=True
            )

        # Do a quick QA/QC on average emission factors of standard fuel types
        # If weighted average is +/- 20%, use EPA standard value.
        def calc_ef_range(x, plusminus=0.2):

            if x['MECS_FT_byp'] != 'Other':

                std_ef = self.std_efs[
                    self.std_efs.MECS_FT == x['MECS_FT']
                    ].MTCO2e_per_MMBtu.values[0]

            elif x['MECS_FT_byp'] == 'Other':

                return x['MTCO2e_per_MMBtu']

            else:

                std_ef = self.std_efs[
                    (self.std_efs.MECS_FT == x['MECS_FT']) &
                    (self.std_efs.MECS_FT_byp == x['MECS_FT_byp'])
                     ].MTCO2e_per_MMBtu.values[0]

            ef_range = [std_ef*(1-plusminus), std_ef*(1+plusminus)]

            if ef_range[0] <= x['MTCO2e_per_MMBtu'] <= ef_range[1]:

                return x['MTCO2e_per_MMBtu']

            else:

                return std_ef

        final_ghgrp_CO2e_intensity.reset_index(inplace=True)

        final_ghgrp_CO2e_intensity['pass_qaqc'] = \
            final_ghgrp_CO2e_intensity.apply(lambda x: calc_ef_range(x),
                                             axis=1)

        final_ghgrp_CO2e_intensity.MTCO2e_per_MMBtu.update(
            final_ghgrp_CO2e_intensity.pass_qaqc
            )

        final_ghgrp_CO2e_intensity.drop(['pass_qaqc'], axis=1, inplace=True)

        # fill biomass emission factor = 0
        biomass = final_ghgrp_CO2e_intensity.where(
            final_ghgrp_CO2e_intensity.MECS_FT_byp == 'Biomass'
            ).dropna()

        biomass.loc[:, 'MTCO2e_per_MMBtu'] = 0

        final_ghgrp_CO2e_intensity.update(biomass)

        # Save results
        final_ghgrp_CO2e_intensity.to_csv(
            os.path.join(self.data_dir,
                         'ghgrp_CO2e_intensity_'+str(self.year)+'.csv'),
            index=False
            )

        return final_ghgrp_CO2e_intensity, final_ghgrp_fuel_disagg

    def calc_fuel_emissions(self, county_energy_temp):

        """
        Calculates the GHG intensity by county, NAICs, and employment size
        class. Emissions from GHGRP-reporting facilities are taken directly
        from EPA data. Emissions from remaining establishments are estimated
        using default EPA ghg emissions factors.

        Biogenic emissions resulting from combustion of biomass and biomass
        residuals are not included.
        """

        # Try to import emissions intensity and fuel diaggregation calculated
        # at the county level; if not, run calculations

        ei_file = os.path.join(self.data_dir,
                               'ghgrp_CO2e_intensity_'+str(self.year)+'.csv')

        fd_file = os.path.join(self.data_dir,
                               'ghgrp_fuel_disagg_'+str(self.year)+'.csv')

        if (os.path.exists(ei_file) & os.path.exists(fd_file)):
            final_ghgrp_CO2e_intensity = pd.read_csv(ei_file)
            final_ghgrp_fuel_disagg = pd.read_csv(fd_file)

        else:
            print('Calculating necessary data')
            final_ghgrp_CO2e_intensity, final_ghgrp_fuel_disagg = \
                self.calc_ghgrp_intensities()

        # Aggregate, to county, naics, and fuel types
        final_ghgrp_fuel_disagg = final_ghgrp_fuel_disagg.groupby(
            ['COUNTY_FIPS', 'naics', 'MECS_FT', 'MECS_FT_byp'], as_index=False
            ).MMBtu_fraction.sum()

        if type(county_energy_temp.index) != pd.core.indexes.range.RangeIndex:

            county_energy_temp.reset_index(inplace=True)

        ghgrp_emissions = pd.DataFrame()

        # Calculate ghgrp emissions first, if contained in DataFrame.
        try:

            ghgrp_emissions = \
                county_energy_temp[county_energy_temp.Emp_Size == 'ghgrp']

        except KeyError as e:

            print('No GHGRP emissions in dataframe: {}'.format(e))

        else:

            final_ghgrp_fuel_disagg = final_ghgrp_fuel_disagg[
                final_ghgrp_fuel_disagg.MECS_FT == 'Other'
                ]

            # Split out byproducts
            ghgrp_emissions = pd.merge(
                ghgrp_emissions.set_index(['COUNTY_FIPS', 'naics', 'MECS_FT']),
                final_ghgrp_fuel_disagg.set_index(
                    ['COUNTY_FIPS', 'naics', 'MECS_FT', 'MECS_FT_byp']
                    ),
                how='left', left_index=True, right_index=True
                )

            ghgrp_emissions.reset_index(inplace=True)

            ghgrp_emissions.MMBtu.update(ghgrp_emissions.MMBtu.multiply(
                ghgrp_emissions.MMBtu_fraction
                ).dropna())

            ghgrp_emissions.MECS_FT_byp.update(
                ghgrp_emissions[ghgrp_emissions.MECS_FT_byp.isnull()].MECS_FT
                )

            ghgrp_emissions = pd.merge(
                ghgrp_emissions, final_ghgrp_CO2e_intensity, how='left',
                on=['COUNTY_FIPS', 'naics', 'MECS_FT', 'MECS_FT_byp']
                )

            ghgrp_emissions['MTCO2e_TOTAL'] =\
                ghgrp_emissions.MMBtu.multiply(
                    ghgrp_emissions.MTCO2e_per_MMBtu
                    )

            ghgrp_emissions['MTCO2e_TOTAL'] = \
                ghgrp_emissions.MTCO2e_TOTAL.astype('float32')

            ghgrp_emissions.drop(['MMBtu_fraction', 'REPORTING_YEAR'],
                                 axis=1, inplace=True)

            ghgrp_emissions[
                (ghgrp_emissions[ghgrp_emissions.MECS_FT != 'Other']),
                'MECS_FT_byp'] = np.nan

            ghgrp_emissions.drop(['REPORTING_YEAR', 'MTCO2e_per_MMBtu'],
                                 axis=1, inplace=True)

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
                self.mecs_other_disagg[self.mecs_other_disagg.MECS_FT=='Other'],
                self.std_efs.dropna()[['MECS_FT_byp', 'MTCO2e_per_MMBtu']],
                how='left', on='MECS_FT_byp'
                )

            # mecs_GHG_disagg.MTCO2e_per_MMBtu.update(
            #     mecs_GHG_disagg.Byp_fraction.multiply(
            #         mecs_GHG_disagg.MTCO2e_per_MMBtu
            #         )
            #     )

            # Drop instances of US-avg data
            mecs_GHG_disagg = mecs_GHG_disagg.where(
                (mecs_GHG_disagg.MECS_Region != 'US')
                ).dropna()

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
                nonGHGRP_emissions,
                mecs_GHG_disagg[['MECS_FT_byp', 'Byp_fraction',
                                 'MTCO2e_per_MMBtu']], left_index=True,
                right_index=True, how='left'
                )

            nonGHGRP_emissions.reset_index(
                ['MECS_Region', 'MECS_NAICS', 'End_use'], drop=False,
                inplace=True
                )

            # Breakout Other fuel use.
            nonGHGRP_emissions.MMBtu.update(
                nonGHGRP_emissions.MMBtu.multiply(
                    nonGHGRP_emissions.Byp_fraction
                    )
                )

            nonGHGRP_emissions.update(
                self.std_efs[self.std_efs.MECS_FT_byp.isnull()].set_index(
                    'MECS_FT'
                    ).MTCO2e_per_MMBtu, overwrite=False
                )

            nonGHGRP_emissions.reset_index(inplace=True)

            nonGHGRP_emissions['MTCO2e_TOTAL'] =\
                nonGHGRP_emissions.MTCO2e_per_MMBtu.multiply(
                    nonGHGRP_emissions.MMBtu
                    )

            nonGHGRP_emissions.drop(['MTCO2e_per_MMBtu'], axis=1, inplace=True)

            nonGHGRP_emissions['MTCO2e_TOTAL'] = \
                nonGHGRP_emissions.MTCO2e_TOTAL.astype('float32')

            all_emissions = pd.concat([nonGHGRP_emissions, ghgrp_emissions],
                                      axis=0, ignore_index=True, sort=True)

            return all_emissions

        def calc_tech_opp_emissions(self, tech_opp_fuels, county):
            """

            """

            ei_file = os.path.join(self.data_dir,
                                  'ghgrp_CO2e_intensity_'+str(self.year)+'.csv')

            if (os.path.exists(ei_file)):

                ghgrp_CO2e_intensity = pd.read_csv(ei_file)

            else:

                print('Calculating necessary data')

                ghgrp_CO2e_intensity, final_ghgrp_fuel_disagg = \
                    self.calc_ghg_and_fuel_intensity()

            ghgrp_CO2e_intensity =\
                ghgrp_CO2e_intensity[ghgrp_CO2e_intensity.COUNTY_FIPS==county]

            ghgrp_ghgs = tech_opp_fuels[
                tech_opp_fuels.Emp_Size=='ghgrp'
                ].groupby(
                    ['naics','MECS_FT', 'MECS_FT_byp'], as_index=False
                    ).MW.sum()

            ghgrp_ghgs = pd.merge(
                ghgrp_ghgs, ghgrp_CO2e_intensity,
                on=['naics', 'MECS_FT', 'MECS_FT_byp'], how='left'
                )

            ghgrp_ghgs['MTCO2e'] = ghgrp_ghgs.MW.multiply(
                ghgrp_ghgs.MTCO2e_per_MMBtu
                )/0.293297 # convert from MW(h) to MMBtu

            mecs_ghgs = tech_opp_fuels[
                tech_opp_fuels.Emp_Size!='ghgrp'
                ].groupby(['MECS_FT', 'MECS_FT_byp'], as_index=False).MW.sum()

            mecs_ghgs = pd.merge(
                mecs_ghgs, self.std_efs[self.std_efs.MECS_FT!='Other'][
                    ['MECS_FT', 'MTCO2e_per_MMBtu']
                    ], on='MECS_FT', how='left'
                )

            mecs_ghgs.set_index('MECS_FT_byp', inplace=True)

            mecs_ghgs.update(
                self.std_efs[self.std_efs.MECS_FT=='Other'].set_index(
                    'MECS_FT_byp'
                    ), overwrite=False
                )

            mecs_ghgs['MTCO2e'] = mecs_ghgs.MW.multiply(
                mecs_ghgs.MTCO2e_per_MMBtu
                )/0.293297 # convert from MW(h) to MMBtu

            tech_opp_ghgs = pd.concat(
                [ghgrp_ghgs.groupby(['MECS_FT', 'MECS_FT_byp']).MTCO2e.sum(),
                 mecs_ghgs.groupby(['MECS_FT', 'MECS_FT_byp']).MTCO2e.sum()],
                axis=0, ignore_index=True
                )

            return tech_opp_ghgs
