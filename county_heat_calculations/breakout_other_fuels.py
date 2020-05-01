

class

    def __init__(self, year):

        self.year = year

        formatted_files = ['bio'+str(2014)+'_formatted.csv',
                           'byp'+str(2014)+'_formatted.csv']

        for f in formatted_files:

            if f in os.listdir('./calculation_data/'):

                continue

            else:

                print(f, 'does not exist in /calculation_data/', '\n',
                      'Please create it')

        bio_table = pd.read_csv('./calculation_data/' + \
                                'bio' + str(2014) +'_formatted.csv')

        byp_table = pd.read_csv('./calculation_data/' +\
                                'byp' + str(2014) + '_formatted.csv')


        def format_biobyp_tables(df):

            df['naics'].replace({'31-33': 31}, inplace=True)

            for c in df.columns:

                if c in ['region', 'n_naics', 'naics_desc']:

                    continue

                else:

                    df[c] = df[c].astype('float32')

            return df

        bio_table = format_biobyp_tables(bio_table)

        byp_table = format_biobyp_tables(byp_table)

        eu_table.set_index(['naics', 'end_use'], inplace=True)

        adj_total = eu_table.xs(
            'TOTAL FUEL CONSUMPTION', level=1
            ).total.subtract(
                    eu_table.xs('End Use Not Reported',
                                level=1).total, level=1
                    )

        adj_total.name = 'adj_total'

        eu_table.reset_index('end_use', inplace=True)

    def breakout_other(year):
        """

        """
