
import pandas as pd

import numpy as np

class EnergyEndUse(object):
    """
    Calcualte energy end uses by industry subsector.
    """
    county_file = \
        'Y:\\6A20\\Public\\ICET\\Data for calculations\\Data foundation\\' +\
        'County_IndustryDataFoundation_2014_update_20170910-0116.csv'

    mecs_naics_file = 'mecs_naics.csv'

    #Import county energy (in TBtu)
    county_energy = pd.read_csv(
        county_file, index_col=['fips_matching', 'naics'], low_memory=False
        )

    #Need to remap 'MECS_NAICS' back to original NAICS used in MECS.
    mecs_naics_dict = dict(
        pd.read_csv(
            mecs_naics_file, usecols=['MECS_NAICS_dummies', 'MECS_NAICS']
            ).values
        )

    county_energy.MECS_NAICS = county_energy.MECS_NAICS.map(mecs_naics_dict)

    county_energy.MECS_NAICS.fillna(0, inplace=True)

    county_energy.MECS_NAICS = county_energy.MECS_NAICS.apply(lambda x: int(x))

    county_energy.rename(
        columns={'fips_matching.1': 'fips_matching', 'naics.1': 'naics'}, \
        inplace=True
        )


    @classmethod
    def Mfg_calc(cls):
        """
        #Estimate end-use energy by fuel type from MECS 2010 data
        MECS2010_EndUse.xlsx has been cleaned up and formatted from version 
        downloaded from EIA. MECS data have been adjusted using approaches 
        outlined in Fox et al. (2011).
        Note that GHGRP reporters have reported use of fuel types that is not 
        captured in 2010 MECS. As a result, the energy totals calculated by end
        use may be  less than the county-level energy use by fuel type.
        """

        mfg_energy = pd.DataFrame(
            cls.county_energy[(cls.county_energy.naics < 400000) & \
            (cls.county_energy.naics > 300000)], copy=True
            )

        #Create index of missing MECS_NAICS
        mn_index = mfg_energy[mfg_energy.MECS_NAICS == 0].index
        
        mfg_energy.loc[mn_index, 'MECS_NAICS'] = \
            mfg_energy.loc[mn_index, 'naics'].map(dict(
                    mfg_energy[mfg_energy.MECS_NAICS != 0][
                        ['naics', 'MECS_NAICS']
                        ].values
                    )
                )
    	
        #Dict mapping fuel types used in county-level with MECS fuel types (i.e.,
        # {County Fuel Type: MECS Fuel Type}). Does not include "Other".
        FT_dict = {
            'Coal': 'coal', 'Coke_and_breeze': 'coal', \
            'Diesel': 'dist_fuel_oil', 'LPG_NGL': 'LPG_NGL', \
            'Natural_gas': 'NG', 'Net_electricity': 'net_electricity', \
            'Residual_fuel_oil': 'res_fuel_oil', 'Other': 'other OR byproducts'
            }

        MECS_enduse = pd.read_excel(
    		'MECS2010_EndUse.xlsx', sheetname='Adjusted_PyInput_netelec'
    		)
        
        MECS_enduse.dropna(axis=0, inplace=True)

        MECS_enduse.END_USE = MECS_enduse.END_USE.str.strip()

        MECS_enduse.NAICS_CODE = MECS_enduse.NAICS_CODE.astype(int)

        MECS_enduse.set_index(['NAICS_CODE','END_USE'], inplace=True)

        enduses = list(MECS_enduse.index.get_level_values(1).drop_duplicates())

        mfg_enduse = pd.DataFrame(columns=list(mfg_energy.columns))
        
        mfg_enduse.drop(
            ['Total', 'fipscty', 'fipstate'], axis=1, inplace=True
            )


        #Calculate 
        for n in mfg_energy.MECS_NAICS.drop_duplicates():

            for f in FT_dict.keys():

                if f != 'Other':
                    
                    if MECS_enduse.ix[n][FT_dict[f]].sum() > 0:

                        FT_enduse = \
                            mfg_energy[mfg_energy.MECS_NAICS == n][f].apply(
                                lambda x: x * MECS_enduse.ix[n][FT_dict[f]]
                                )

                        FT_enduse.reset_index(inplace=True)

                        FT_enduse = pd.melt(
                            FT_enduse, id_vars=['fips_matching', 'naics'], \
                            value_name=f, var_name='Enduse'
                            )

                        mfg_enduse = mfg_enduse.append(FT_enduse)

                    else:
                        pass             
                
                else:
                    
                    if MECS_enduse.ix[n]['byproducts'].sum() > 0: 

                        o_f = 'byproducts'

                    else:
                        
                        o_f = 'other'

                    if MECS_enduse.ix[n][o_f].sum() > 0:

                        FT_enduse = \
                            mfg_energy[mfg_energy.MECS_NAICS == n][f].apply(
                                lambda x: x * MECS_enduse.ix[n][o_f]
                                )

                        FT_enduse.reset_index(inplace=True)

                        FT_enduse = pd.melt(
                            FT_enduse, id_vars = ['fips_matching', 'naics'], \
                            value_name=f, var_name='Enduse'
                            )

                        mfg_enduse = mfg_enduse.append(
                            FT_enduse, ignore_index=True
                            )

                    else:
                        pass


        mfg_enduse.loc[:, ('fips_matching', 'naics')] = \
            mfg_enduse[['fips_matching', 'naics']].astype(np.int)

        mfg_enduse.loc[:, 'MECS_NAICS'] = \
                    mfg_enduse.loc[:, 'naics'].map(dict(
                            mfg_energy[
                                ['naics', 'MECS_NAICS']
                                ].values
                            )
                        )
