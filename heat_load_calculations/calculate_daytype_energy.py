

def energy_load(shift_schedule, naics, unit_type):
    """
    Caculate energy load by weekday, saturday and Sunday based on
    shift schedule, NAICS code (listed in Census QPC), and unit type (
    boiler, CHP/cogen, process heater). As of 9/30/2019 there is no distinction
    between load shapes for unit types.
    """

    
