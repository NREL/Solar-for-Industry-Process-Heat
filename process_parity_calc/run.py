"""
List of things that need to be updated year by year:
corporate tax dataset, subsidies from DSire
MACRS depreciation schedule
manually assign discount rate
update the chemical engineering cost index
update heat content using https://www.eia.gov/totalenergy/data/monthly/#appendices spreadsheets
"""
import Create_LCOH_Obj as CLO
from create_format import FormatMaker
from bisection_method import bisection

# create LCOH object 1
format1 = FormatMaker().create_format()
LCOH1 = CLO.LCOHFactory().create_LCOH(format1)

LCOH1.calculate_LCOH()
print("The LCOH is {:.2f} cents/kwh.".format(LCOH1.LCOH_val*3600*100))

# create LCOH object 2
# =============================================================================
# format2 = FormatMaker().create_format()
# LCOH2 = CLO.LCOHFactory().create_LCOH(format2)
# 
# # import parameters
# LCOH1.import_param()
# LCOH2.import_param()
# =============================================================================

# create LCOH methods - assume object 2 is fixed 
LCOH_val_1 = LCOH1.iterLCOH
LCOH_val_2 = 5

root = bisection(lambda x: LCOH_val_1(x)*3600*100 - LCOH_val_2, 5, 25, 100)
print(root)

