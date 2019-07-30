
# Unzip IAC database from URL
# Create employment class bin following data
# Calculate mean and bootstrap confidence intervals (see commercial uncertainty code)
# Group by NAICS code, and year
# fuel columns to compare USAGE_ELEC, USAGE_LPG, USAGE_NAT
import pandas as pd
import numpy as np
import requests
import zipfile
import io
import scipy

#%%
class iac:

    def __init__(self):

        iac_db_url = 'https://iac.university/IAC_Database.zip'

        iac_r = requests.get(iac_db_url)

        with zipfile.ZipFile(io.BytesIO(iac_r.content)) as iac_zip:

            with iac_zip.open('IAC_Database.xls') as iac_file:

                self.iac = pd.read_excel(iac_file, sheet_name='ASSESS')

        # Values in MMBtu, except electricity
        self.iac.rename(
                columns={'EC_plant_usage': 'elec_kwh', 
                         'E2_plant_usage': 'Natural_gas', 
                         'E3_plant_usage': 'LPG_NGL',
                         'E4_plant_usage': 'no1_mmbtu', 
                         'E5_plant_usage': 'Diesel', # No. 2 fuel oil 
                         'E6_plant_usage': 'no4_mmbtu',
                         'E7_plant_usage': 'Residual_fuel_oil', # No. 6 fuel oil
                         'E8_plant_usage': 'Coal',
                         'E9_plant_usage': 'wood_mmbtu'}, inplace=True)
    
        # Convert electricity
        self.iac['elec_mmbtu'] = self.iac.elec_kwh.multiply(0.00341214163)

        # Create "other" category
        self.iac['Other'] = self.iac[
                ['wood_mmbtu', 'no1_mmbtu', 'no4_mmbtu']
                ].sum(axis=1)

        # Create employee size class bins.
        self.iac['Emp_Size']  = pd.cut(
            self.iac['EMPLOYEES'], bins=[0, 49, 99, 249, 499, 999, 1000],
            right=True, include_lowest=True,
            labels=['n1_49', 'n50_99', 'n100_249', 'n250_499', 'n500_999',
                    'n1000']
            )
        
        # Include state FIPS 
        
        fips_data = pd.read_csv('C:/Users/cmcmilla/Solar-for-Industry-Process-Heat/calculation_data/US_FIPS_Codes.csv',
                                usecols=['Abbrev', 'FIPS State'])

        fips_data.drop_duplicates(inplace=True)
        
        fips_data['Abbrev'] = fips_data.Abbrev.apply(lambda x: x.strip())
        
        self.iac['STATE'].update(
                self.iac['STATE'].dropna().apply(lambda x: x.upper())
                )
        
        self.iac = self.iac.set_index('STATE').join(
                fips_data.set_index('Abbrev')
                ).reset_index()
        
        self.iac.rename(columns={'index':'state', 'NAICS': 'naics',
                                 'FIPS State': 'fipstate'},
                        inplace=True)
        
        self.iac = self.iac[self.iac.state.notnull()]
#%%
    def energy_validation(self, mfg_energy):
        """
        """

        if mfg_energy.index.names != [None]:
            
            mfg_energy.reset_index(inplace=True)
            
        iac_state_compare = pd.DataFrame()
            
        iac_indexed = iac[iac.FY > 2012].set_index(
                ['Emp_Size', 'naics']
                )
        
        iac_indexed_state = iac[iac.FY > 2012].set_index(
                ['fipstate', 'Emp_Size', 'naics']
                )
        
        
        for fuel in ['elec_mmbtu', 'Natural_gas', 'Diesel',
                     'Residual_fuel_oil', 'Coal', 'wood_mmbtu']:
        
            for i in iac_index.index.values:
                
                
            
        
        
    
        # Take recent data
        iac_grpd = iac[iac.FY > 2012].groupby(
                ['fipstate', 'Emp_Size', 'naics']
                )

        # Compare by employement size class, NAICS, and state. Compute
        # standard error. Note where data don't exist from IAC.
        county_compare = pd.DataFrame()
        
        for fuel in mfg_energy.MECS_FT.unique():
            
            if fuel in iac.columns:
            
                iac_mean = iac_grpd[fuel].mean()
    
            else:
                
                continue
            
            # Counties may have more than one facility of the same NAICS code
            # Comparison compares the average
            compare = mfg_energy[(mfg_energy.MECS_FT == fuel) &
                                 (mfg_energy.est_count >0)].groupby(
                    ['fipstate', 'Emp_Size', 'naics']
                    ).MMBtu_TOTAL.mean()
            
            compare = compare.divide(
                    mfg_energy[(mfg_energy.MECS_FT == fuel) &
                               (mfg_energy.est_count >0)].set_index(
                        ['fipstate', 'Emp_Size', 'naics']
                        ).est_count
                    )
            
            compare = compare.subtract(iac_mean).dropna()**2
            
            compare.name = fuel
            
            county_compare = pd.concat([county_compare, compare], axis=1)