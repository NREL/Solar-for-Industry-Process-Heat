

class Emissions:

    def __init__(self, years):

        # Define as a range or list
        self.years = years

        ghgrp_emissions = pd.read_parquet(
            '../results/ghgrp_energy_20200505-1117.parquet', engine='pyarrow',
            columns=['FACILITY_ID', 'REPORTING_YEAR', 'FUEL_TYPE',
                     'FUEL_TYPE_OTHER', 'FUEL_TYPE_BLEND', 'MTCO2e_TOTAL',
                     'MMBtu_TOTAL']
            )

        ghgrp_emissions = pd.DataFrame(
            ghgrp_emissions[ghgrp_emissions.REPORTING_YEAR.isin(self.years)
            )

        # Map aggregated fuel types to GHGRP fuel types
        ghgrp_emissions = map_GHGRP_fueltypes(ghgrp_emissions,
                                              'MECS_FT_IPF.csv')

        # Map disaggregated fuel types to GHGRP fuel types
        ghgrp_emissions = map_GHGRP_fueltypes(ghgrp_emissions,
                                                   'MECS_FT_byp.csv')

        # Replace Biomass emissions with zero value
        ghgrp_emissions.loc[ghgrp_emissions.MECS_FT_byp == 'Biomass'),
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

    def calc_ghgrp_fuel_intensity(self):
        """

        """

        ghgrp_fuel_mix = self.ghgrp_emissions.groupby(
            ['FACILITY_ID', 'REPORTING_YEAR', 'MECS_FT', 'MECS_FT_byp']
            ).MTCO2e_TOTAL.sum().divide(
                self.ghgrp_emissions.groupby(
                    ['FACILITY_ID', 'REPORTING_YEAR', 'MECS_FT', 'MECS_FT_byp']
                    ).MMBtu_TOTAL.sum()
                )

        # Use this fuel intensity for GHGRP data (linked by FACILITY_ID) after
        # corrections for county and NAICS have been made.
        return energy_ghgrp

    def breakout_other(year):
        """

        """



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
        steam_ef = (66.33+1.25*25/1000+0.125*298/1000)/1000 # MTCO2e/MMBtu

    def calc_elect_intensity(self, county):
        """
        GHG intensity of grid electricity (MMTCO2e/MMBtu), calculated from
        EPA eGrid.
        """
