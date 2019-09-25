

def classification(amd_data):

    """
    Compares
    """
    #Aggregate AMD data by qpc naics, ORISPL_CODE, holiday, weekday, and hour
    class_agg = amd_data.groupby(
        ['qpc_naics', 'ORISPL_CODE', 'holiday', 'dayofweek', 'OP_HOUR']
        ).agg({'HEAT_INPUT_MMBtu': 'mean', 'heat_input_fraction':'mean'})


    def calc_load_params(class_agg, shift_test , cutoff=0.86):
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

            shift_agg = class_agg['HEAT_INPUT_MMBtu'].divide(
                class_agg['HEAT_INPUT_MMBtu'].xs([False, day_norm],
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

            shift_agg = class_agg['HEAT_INPUT_MMBtu'].divide(
                class_agg['HEAT_INPUT_MMBtu'].xs([False, 'weekday'],
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
        fixed_load = pd.concat(
            [class_agg.heat_input_fraction.xs(
                n, level='ORISPL_CODE'
                ).min(level=2) for n in facs_shift], axis=0, ignore_index=False
            ).mean(level=0)

        max_load = pd.concat(
            [class_agg.heat_input_fraction.xs(
                n, level='ORISPL_CODE'
                ).max(level=2) for n in facs_shift], axis=0, ignore_index=False
            ).mean(level=0)

        params_dict = {'fixed_load': fixed_load, 'max_load': max_load,
                       'fac_ORISPL': facs_shift}

        return params_dict


    def fill_schedule(op_schedule, NAICS, weekday_shifts, saturday_shifts,
                      sunday_shifts):

        # import load params summary
        load_params = pd.read_csv('heat_load_parameters.csv')

        load_params = load_params.xs(NAICS, level='qpc_naics')

        op_schedule.where(op_schedule == False).fillna(load_param.max_load)
    # # Define a one-shift weekday work schedule by facili.
    #
    # shift_agg_single = class_agg['HEAT_INPUT_MMBtu'].divide(
    #     class_agg['HEAT_INPUT_MMBtu'].xs([False, 'weekday'],
    #     level=['holiday', 'dayofweek'])
    #     )
    #
    # # Define a one-shift weekday work schedule by saturday:weekday < 0.86.
    # # Get ORISPL codes of facilities that meet this criterion.
    # facs_oneshift = shift_agg.xs(
    #     ['saturday', 9], level=('dayofweek', 'OP_HOUR')
    #     ).where(shift_agg.xs(
    #         ['saturday', 9], level=('dayofweek', 'OP_HOUR')
    #         )<0.86).dropna().index.get_level_values('ORISPL_CODE').values
