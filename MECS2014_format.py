# Code to import and format MECS 2014 tables 3.2 and 5.2
# %%
import pandas as pd
import numpy as np
import os

class Format_seed:
    """
    Create and format iterative proportional fitting (IPF) seed
    """
    def __init__(self):

        self.filepath = \
            "C:\\Users\\cmcmilla\\Solar-for-Industry-Process-Heat\\"

        self.url_3_2 =\
            'https://www.eia.gov/consumption/manufacturing/data/2014/' + \
            'xls/table3_2.xlsx'

        self.url_3_3 = \
            'https://www.eia.gov/consumption/manufacturing/data/2014/' + \
            'xls/table3_3.xlsx'

        self.seed_file = 'IPF_seed.csv'

        def create_unformatted_mecs(self):

            table3_2 = pd.read_excel(
                self.url_3_2, sheet='Table 3.2',index_col=None, skiprows=10
                )

            table3_3 = pd.read_excel(
                self.url_3_3, sheet='Table 3.3', index_col=None, skiprows=10
                )

            for df in [table3_2, table3_3]:

                df.dropna(axis=0, how='all', inplace=True)

                df.iloc[:, 0].fillna(method='ffill', inplace=True)

                df.reset_index(inplace=True, drop=True)

            # These fillnas and replaces weren't working in the for loop above,
            # for some reason.
            table3_2.fillna(axis=1, method='bfill', inplace=True)

            table3_3.fillna(axis=1, method='bfill', inplace=True)

            # Replace nonreported values below 0.5 with value = 0.1. Some of
            # these are adjusted later in manual formatting.
            table3_2.replace(
                to_replace={'*': 0.1, '--': 0, 'Q': np.nan,'W': np.nan},
                value=None, inplace=True
                )

            table3_3.replace(
                to_replace={'*': 0.1, '--': 0, 'Q': np.nan, 'W': np.nan},
                value=None, inplace=True
                )

            for n in [0, 1]:

                table3_2.iloc[:, n] = \
                    table3_2.iloc[:, n].apply(lambda x: str(x).strip())

                if n == 0:

                    table3_3.iloc[:, n] = \
                        table3_3.iloc[:, n].apply(lambda x: str(x).strip())

            # Export for manual final formatting
            writer = pd.ExcelWriter(self.filepath + "MECS2014_unformatted.xlsx")

            table3_2.to_excel(writer, index=False, sheet_name='Table3.2')

            table3_3.to_excel(writer, index=False, sheet_name='Table3.3')

            writer.save()

        # First check if "MECS2014_unformatted.xlsx" exists in filepath.
        # If not, proceed with creating it.
        if 'MECS2014_formatted.xlsx' not in os.listdir(self.filepath):

            create_unformatted_mecs()

        # Re-import formatted sheets
        self.table3_2 = pd.read_excel(self.filepath + "MECS2014_formatted.xlsx",
                                 sheet_name='Table3.2')

        self.table3_3 = pd.read_excel(self.filepath + "MECS2014_formatted.xlsx",
                                 sheet_name='Table3.3')

        self.table3_2.dropna(axis=0, thresh=4, inplace=True)

        self.table3_2 = self.table3_2[(self.table3_2.NAICS_desc != 'Total') &
                            (self.table3_2.Region != 'United States')]

        self.table3_3.dropna(axis=0, thresh=3, inplace=True)

        self.table3_3 = \
            self.table3_3[(self.table3_3.Characteristic != 'Total') &
                          (self.table3_3.Region != 'United States')]

        for df in [self.table3_2, self.table3_3]:

            df.rename(columns={'Region': 'region', 'NAICS': 'naics'},
                      inplace=True)

            df.drop(['Total'], axis=1, inplace=True)

    def import_seed(self):
        """
        Imports and formats seed for manufacturing IPF from specified path.
        """

        def ft_split(s):
            """
            Handles splitting off fuel types with more than one word.
            """
            split = s.split('_')

            ft = split[1]

            for n in range(2, len(split)-1):

                ft = ft + '_' + split[n]

            return ft

        seed_df = pd.read_csv(
            os.path.join(self.filepath, self.seed_file), index_col=None
            )

        seed_df = seed_df.replace({0:1})

        seed_cols = [seed_df.columns[0]]

        for c in seed_df.columns[1:]:
            seed_cols.append(int(c))

        seed_df.columns = seed_cols

        seed_df.loc[:, 'region'] = seed_df.iloc[:, 0].apply(
            lambda x: x.split('_')[0]
            )

        seed_df.loc[:, 'Fuel_type'] = seed_df.iloc[:, 0].apply(
            lambda x: ft_split(x)
            )

        seed_df.loc[:, 'EMPSZES'] = seed_df.iloc[:, 0].apply(
            lambda x: x.split('_')[-1]
            )

        return seed_df

    @staticmethod
    def seed_correct_cbp(seed_df, cbp):
        """
        Changes seed values to zero based on CBP empolyment size count by
        industry and region.
        """

        seed_df.set_index(['region', 'EMPSZES'], inplace=True)

        # Reformat CBP data
        cbp_pivot = cbp.copy(deep=True)

        cbp_pivot.rename(columns={"n50_99": "50-99", "n100_249": "100-249",
                                  "n250_499": "250-499", "n500_999": "500-999"},
                         inplace=True)

        cbp_pivot = cbp_pivot.melt(id_vars=['region', 'naics'],
                                   value_vars=['Under 50', '50-99', '100-249',
                                               '250-499', '500-999', 'Over 1000'],
                                    var_name='EMPSZES')

        cbp_pivot = pd.pivot_table(cbp_pivot, index=['region', 'EMPSZES'],
                                   columns=['naics'], values=['value'],
                                   aggfunc='sum')

        cbp_pivot.columns = cbp_pivot.columns.droplevel()

        shared_cols = []

        for c in cbp_pivot.columns:
            if c in seed_df.columns:
                shared_cols.append(c)

        cbp_mask = cbp_pivot[shared_cols].reindex(seed_df.index).fillna(0)

        seed_df.update(seed_df[shared_cols].where(cbp_mask != 0, 0))

        return seed_df
    # %%

    def seed_correct_MECS(self, seed_df):
        """
        Changes seed values to zero based on MECS fuel use by industry and
        region.
        """
        melt_cols = [0]

        for n in range(3, len(self.table3_2.columns)):
            melt_cols.append(n)

        table3_2_mask = pd.pivot_table(
            self.table3_2.iloc[:, melt_cols].melt(
                id_vars=['region', 'naics'], var_name=['Fuel_type']
                ),
            index=['region', 'Fuel_type'], columns='naics'
            )

        table3_2_mask.columns = table3_2_mask.columns.droplevel()

        seed_df.reset_index(drop=False, inplace=True)

        seed_df.set_index(['region','Fuel_type'], inplace=True)

        table3_2_mask.reindex(seed_df.index, inplace=True)

        shared_cols = []

        for c in table3_2_mask.columns:
            if c in seed_df.columns:
                shared_cols.append(c)

        table3_2_mask = table3_2_mask[shared_cols].reindex(
            seed_df.index
            ).fillna(0)

        seed_df.reset_index(level=['EMPSZES'], drop=False, inplace=True)

        seed_df.update(seed_df[shared_cols].where(table3_2_mask != 0, 0))

        seed_df.reset_index(drop=True, inplace=True)

        seed_df.drop(['EMPSZES'], axis=1, inplace=True)

        return seed_df
