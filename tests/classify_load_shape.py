
import pandas as pd
import numpy as np
import re

class classification:

    """

    """

    def __init__(self, amd_data):
    #Aggregate AMD data by qpc naics, ORISPL_CODE, holiday, weekday, and hour
        self.class_agg = amd_data.groupby(
            ['qpc_naics', 'ORISPL_CODE', 'holiday', 'dayofweek', 'OP_HOUR']
            ).agg({'HEAT_INPUT_MMBtu': {'mean', 'sum'},
                   'heat_input_fraction': 'mean'})

        self.load_params = pd.read_csv(
                '../calculation_data/heat_loadshape_parameters_20190926.csv',
                index_col=['schedule_type', 'holiday','dayofweek']
                )

    # This has been run and results saved as heat_loadshape_parameters.csv
    def calc_load_params(self, shift_test , cutoff=0.86):
        """
        Caculate average fixed load (baseline/non-operating load) and
        max load (highest operating load) from EPA data for a shift schedule
        based on assumptions of normalized heat input, hour, and cutoff value.

        Makes no distinction between unit types (i.e. conventional boiler,
        CHP/cogen, or process heater).

        Returns a dictionary of mean fixed load and max load by daytype
        and EPA ORISPL codes used to estimate load parameters.

        shift_test is weekday_single, weekday_double, saturday_single,
        saturday_double, sunday_single, sunday_double, or continuous.
        """

        day_norm_dict = {'weekday': 'saturday', 'saturday': 'sunday',
                         'sunday': 'sunday', 'continuous': 'sunday'}

        facs_shift = np.empty(1)

        time_norm1 = 9

        time_norm2 = 20

        if shift_test != 'continuous':

            day_norm, shift_norm = shift_test.split('_')

            shift_agg = self.class_agg['HEAT_INPUT_MMBtu']['mean'].divide(
                self.class_agg['HEAT_INPUT_MMBtu']['mean'].xs([False, day_norm],
                level=['holiday', 'dayofweek'])
                )

            if shift_norm == 'single':

                facs_shift = np.append(
                    facs_shift, shift_agg.xs(
                        [day_norm_dict[day_norm], time_norm1],
                        level=('dayofweek', 'OP_HOUR')
                        ).where(shift_agg.xs(
                            [day_norm_dict[day_norm], time_norm1],
                            level=('dayofweek', 'OP_HOUR')
                            )<cutoff).dropna().index.get_level_values(
                                'ORISPL_CODE'
                                ).values
                    )

                facs_shift = np.append(
                    facs_shift, shift_agg.xs(
                        [day_norm_dict[day_norm], time_norm2],
                        level=('dayofweek', 'OP_HOUR')
                        ).where(shift_agg.xs(
                            [day_norm_dict[day_norm], time_norm2],
                            level=('dayofweek', 'OP_HOUR')
                            )>cutoff).dropna().index.get_level_values(
                                'ORISPL_CODE'
                                ).values
                    )

            else:

                facs_shift = np.append(
                    facs_shift, shift_agg.xs(
                        [day_norm_dict[day_norm], time_norm1],
                        level=('dayofweek', 'OP_HOUR')
                        ).where(shift_agg.xs(
                            [day_norm_dict[day_norm], time_norm1],
                            level=('dayofweek', 'OP_HOUR')
                            )<cutoff).dropna().index.get_level_values(
                                'ORISPL_CODE'
                                ).values
                    )

                facs_shift = np.append(
                    facs_shift, shift_agg.xs(
                        [day_norm_dict[day_norm], time_norm2],
                        level=('dayofweek', 'OP_HOUR')
                        ).where(shift_agg.xs(
                            [day_norm_dict[day_norm], time_norm2],
                            level=('dayofweek', 'OP_HOUR')
                            )<cutoff).dropna().index.get_level_values(
                                'ORISPL_CODE'
                                ).values
                    )

        else:

            day_norm = 'continuous'

            shift_agg = self.class_agg['HEAT_INPUT_MMBtu']['mean'].divide(
                self.class_agg['HEAT_INPUT_MMBtu']['mean'].xs([False, 'weekday'],
                level=['holiday', 'dayofweek'])
                )

            facs_shift = np.append(
                facs_shift, shift_agg.xs(
                    [day_norm_dict[day_norm], time_norm1],
                    level=('dayofweek', 'OP_HOUR')
                    ).where(shift_agg.xs(
                        [day_norm_dict[day_norm], time_norm1],
                        level=('dayofweek', 'OP_HOUR')
                        )>cutoff).dropna().index.get_level_values(
                            'ORISPL_CODE'
                            ).values
                )

            facs_shift = np.append(
                facs_shift, shift_agg.xs(
                    [day_norm_dict[day_norm], time_norm2],
                    level=('dayofweek', 'OP_HOUR')
                    ).where(shift_agg.xs(
                        [day_norm_dict[day_norm], time_norm2],
                        level=('dayofweek', 'OP_HOUR')
                        )>cutoff).dropna().index.get_level_values(
                            'ORISPL_CODE'
                            ).values
                )

        fac, n = np.unique(facs_shift, return_counts=True)

        facs_shift = fac[n > 1]

        # Calculate fixed and max loads of boiler during weekday, saturday, and
        # sunday
        # Load is expressed as fraction of total energy use of relevant
        # facilities

        def calc_min_max_load(facs_shift, min_or_max):

            min_max_load = self.class_agg.reset_index()[
                self.class_agg.reset_index().ORISPL_CODE.isin(facs_shift)
                ].set_index(
                    ['holiday', 'dayofweek', 'OP_HOUR']
                    )['HEAT_INPUT_MMBtu']['sum'].sum(
                        level=['holiday', 'dayofweek', 'OP_HOUR']
                        )

            min_max_load = min_max_load.divide(
                min_max_load.sum(level=['holiday'])
                )

            if min_or_max == 'min':

                min_max_load = pd.DataFrame(min_max_load.min(level=[0,1]))

            else:

                min_max_load = pd.DataFrame(min_max_load.max(level=[0,1]))

            min_max_load.reset_index(inplace=True)

            min_max_load['sum'].update(
                min_max_load.where(min_max_load.dayofweek=='weekday')['sum']/5
                )

            min_max_load.set_index(['holiday', 'dayofweek'], inplace=True)

            return min_max_load

        fixed_load = calc_min_max_load(facs_shift, 'min')

        max_load = calc_min_max_load(facs_shift, 'max')

        # fixed_load = pd.concat(
        #     [class_agg.heat_input_fraction.xs(
        #         n, level='ORISPL_CODE'
        #         ).min(level=2) for n in facs_shift], axis=0, ignore_index=False
        #     ).mean(level=0)
        #
        # max_load = pd.concat(
        #     [class_agg.heat_input_fraction.xs(
        #         n, level='ORISPL_CODE'
        #         ).max(level=2) for n in facs_shift], axis=0, ignore_index=False
        #     ).mean(level=0)
        #
        params_dict = {'fixed_load': fixed_load, 'max_load': max_load,
                       'fac_ORISPL': facs_shift}

        return params_dict


    def fill_schedule(self, op_schedule):
        """
        Takes operational schedule (op_schedule; True/False designation of
        operation based on shift scheduling) and returns hourly fraction of
        annual energy.

        Currently does not account for holiday schedules.
        """

        schedule_type = op_schedule.iloc[0, 3]

        op_col = op_schedule.columns[2]

        op_schedule.set_index(['dayofweek', 'hour'], inplace=True)

        op_schedule = op_schedule.join(
            self.load_params.xs([schedule_type, False], level=[0,1])
            )

        if schedule_type == 'continuous':

            op_schedule['load'] = op_schedule.fixed_load.multiply(
                op_schedule[op_col]
                )

        else:

            op_schedule['load'] = op_schedule.max_load.multiply(
                op_schedule[op_col]
                )

            op_schedule.load.replace(0, np.nan, inplace=True)

            min_sched = op_schedule.reset_index('hour').dropna(
                subset=['load']
                ).apply(lambda x: x.hour-5, axis=1).min(level=0)

            max_sched = op_schedule.reset_index('hour').dropna(
                subset=['load']
                ).apply(lambda x: x.hour+5, axis=1).max(level=0)

            # fill in fixed load at 4 hours before and after shift
            for i in min_sched.index:

                 op_schedule.load.update(
                    op_schedule.loc[
                        (i, range(0, min_sched[i]+1)), 'load'
                        ].fillna(
                            op_schedule.loc[
                                (i, range(0,min_sched[i]+1)), 'fixed_load'
                                ]
                            )
                    )

            for i in max_sched.index:

                if max_sched[i] > 23:

                    continue

                else:

                    op_schedule.load.update(
                        op_schedule.loc[
                            (i,range(max_sched[i],23+1)),'load'
                            ].fillna(
                                op_schedule.loc[(i,max_sched[i]), 'fixed_load']
                                )
                        )

            if len(min_sched.index) < 7:

                for day in set.difference(set(range(0,7)), min_sched.index):

                    op_schedule.load.update(
                        op_schedule.loc[(day, range(0, 24)), 'load'].fillna(
                            op_schedule.loc[(day, range(0, 24)), 'fixed_load']
                            )
                        )

            op_schedule.reset_index(inplace=True)

            op_schedule.load.interpolate(method='cubic', limit_area='inside',
                                    inplace=True)

            op_schedule.set_index(['dayofweek', 'hour'], inplace=True)

        # Load is equivalent to day of week fraction of annual energy use.
        op_schedule['fraction_annual_energy'] = op_schedule.load

        # # Calculate hourly load fraction of daily energy
        # op_schedule['fraction_annual_energy'] = op_schedule.load.divide(
        #     op_schedule.load.sum(level=0)
        #     )

        return op_schedule[['fraction_annual_energy']]
