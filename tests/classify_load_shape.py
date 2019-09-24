

def classification(amd_data):

  """
  Compares
  """
  #Aggregate AMD data by qpc naics, ORISPL_CODE, holiday, weekday, and hour
  class_agg = amd_data.groupby(
    ['qpc_naics', 'ORISPL_CODE', 'holiday', 'dayofweek', 'OP_HOUR']
    ).agg({'HEAT_INPUT_MMBtu': 'mean', 'heat_input_fraction':'mean'})


  shift_agg = class_agg['HEAT_INPUT_MMBtu'].divide(
    class_agg['HEAT_INPUT_MMBtu'].xs([False, 'weekday'],
    level=['holiday', 'dayofweek'])
    )

  # Define a one-shift weekday work schedule by saturday:weekday < 0.86.
  # Get ORISPL codes of facilities that meet this criterion.
    facs_oneshift = shift_agg.xs(
      ['saturday', 9], level=('dayofweek', 'OP_HOUR')
      ).where(shift_agg.xs(
        ['saturday', 9], level=('dayofweek', 'OP_HOUR')
        )<0.86).dropna().index.get_level_values('ORISPL_CODE').values



  # Calculate fixed and max loads of boiler during weekday, saturday, and sunday
  fixed_load = []

  fixed_load.append([
    class_agg.xs(n, level='ORISPL_CODE').min() for n in facs_oneshift
    ])

  max
