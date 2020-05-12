import pandas as pd
import os

def Match(DF, naics_column, naics_vintage=2012):
    """
    Method for matching 6-digit NAICS codes with adjusted
    MECS NAICS.
    """

    file_dir = './calculation_data/'

    if naics_vintage < 2012:

        naics_file = 'mecs_naics.csv'

    if 2012 <= naics_vintage < 2017:

        naics_file = 'mecs_naics_2012.csv'

    mecs_naics = pd.read_csv(os.path.join(file_dir, naics_file))

    DF[naics_column].fillna(0, inplace = True)

    DF.loc[:, naics_column] = DF[naics_column].astype('int')

    DF_index = DF[DF[naics_column].between(310000, 400000,
                  inclusive=False)]

    nctest = pd.DataFrame(DF.loc[DF_index.index, naics_column])

    for n in ['N6', 'N5', 'N4', 'N3']:

        n_level = int(n[1])

        nctest[n] = nctest[naics_column].apply(
                lambda x: int(str(x)[0:n_level]))


    #Match GHGRP NAICS to highest-level MECS NAICS. Will match to
    #"dummy" "-09" NAICS where available. This is messy, but
    #functional.
    ncmatch = pd.concat(
        [pd.merge(nctest, mecs_naics, left_on=column, right_on='MECS_NAICS',
                  how= 'left')['MECS_NAICS']
            for column in ['N6', 'N5', 'N4', 'N3']], axis=1
        )

    ncmatch.columns = ['N6', 'N5', 'N4', 'N3']

    ncmatch.index = nctest.index

    ncmatch['NAICS_MATCH'] = 0

    for n in range(3, 7):

        column = 'N'+str(n)

        ncmatch.NAICS_MATCH.update(ncmatch[column].dropna())

    #Update GHGRP dataframe with matched MECS NAICS.
    DF['MECS_NAICS'] = 0

    DF['MECS_NAICS'].update(ncmatch.NAICS_MATCH)

    return DF
