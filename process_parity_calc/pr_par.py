"""
List of things that need to be updated year by year:
corporate tax dataset, subsidies from DSire
MACRS depreciation schedule
manually assign discount rate
update the chemical engineering cost index
update heat content using https://www.eia.gov/totalenergy/data/monthly/#appendices spreadsheets
manually update fuel escalation rates using EERC until I figure out how to automate

"""
import Create_LCOH_Obj as CLO
from create_format import FormatMaker
from bisection_method import bisection
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import seaborn as sns

class PrPar:
    
    def __init__(self, new_obj = True):
        """ solar_tech, comb_tech should be appropriate factory objects"""
        county_list = [1133, 5149, 4027, 6115, 8125, 9015, 10005, 12133, 13321, 19197, 16087, 17203,
                       18183, 20209, 21239, 22127, 25027, 24510, 23031, 26165, 27173, 29510, 28163, 30111, 37199, 38105,
                       31185, 33019, 34041, 35061, 32510, 36123, 39175, 40153, 41071, 42133, 44009, 45091, 46137, 47189,
                       48507, 49057, 51840, 50027, 53077, 55141, 54109, 56045]

        #county_list = [6085, 6085, 6085, 6085, 6085, 6085]
        #heat_list = [5000,10000, 15000, 20000, 25000, 30000]
        #solar_tech = ["PVHP", "PVHP"]
        #comb_tech = ["BOILER", "CHP"]
        if new_obj:

            print("\nCreating solar object")
            self.s_form = FormatMaker().create_format({"county": county_list})
                
            print("\nCreating combustion object")  
            self.c_form = FormatMaker().create_format({"county": county_list})
            
        #this node is executed by default when the object is reseting
            
        self.solar = [CLO.LCOHFactory().create_LCOH(i) for i in self.s_form]
        self.comb = [CLO.LCOHFactory().create_LCOH(i) for i in self.c_form]
        self.solar_current = list(map(lambda x: x.calculate_LCOH(), self.solar))
        self.comb_current = list(map(lambda x: x.calculate_LCOH(), self.comb))
            
            
    def reset(self):
        """ Reset the object to default state after running an analysis"""
        self.__init__(False)
        
    def mc_analysis(self, index):
        """ Process Monte Carlo results"""
        
        # Grab Monte Carlo Dataframes from each LCOH object
        mc_solar = self.solar[index].simulate("MC")
        mc_comb = self.comb[index].simulate("MC")
        
        # Plot distributions of the LCOH variable
        sns.distplot(mc_solar["LCOH Value US c/kwh"], kde = True, axlabel = "LCOH Cents/KWH", label = self.solar[index].tech_type)
        sns.distplot(mc_comb["LCOH Value US c/kwh"], kde = True, axlabel = "LCOH Cents/KWH", label = self.comb[index].tech_type)
        plt.legend()
        
        self.reset()
        
    def to_analysis(self, index):
        """Process Tornado Diagram Data"""
        
        # Read in the tornado diagram simulation dataframes
        
        to_solar = self.solar[index].simulate("TO").reset_index(drop = True)
        to_comb = self.comb[index].simulate("TO").reset_index(drop = True)

        def plot_to(df, techtype):
            """ Plot Tornado Diagrams"""
            # Drop unnecessary columns
            df.drop(columns = ["Capital Cost", "Operating Cost"], inplace = True)
            # Grab default LCOH value
            val_LCOH = df["LCOH Value US c/kwh"][0]
            # Find % change from default LCOH value
            df["LCOH Value US c/kwh"] = (df["LCOH Value US c/kwh"] - val_LCOH)/val_LCOH * 100
            # Grab no. of parameters, no. of data points per parameter
            length = len(df.columns) - 1
            no_vals = int(len(df)/length)
            # grab original copy
            copy = df.copy(deep = True)
            # list of column strings
            columns = [[i] for i in df.columns]
            
            #grab absolute max change in LCOH for each parameter and stores the old index order
            val = [abs(df.loc[no_vals*i+1:no_vals*i+no_vals, ["LCOH Value US c/kwh"]]).abs().max().values[0] for i in range(length)]
            old_ind = [x[0] for x in sorted(enumerate(val),key=lambda i:i[1], reverse = True)]
            
            # Rearranges data frame from the most sensitive to least sensitive parameters
            for i in range(length):
                df.loc[no_vals*i+1:no_vals*i+no_vals, :] = copy.loc[no_vals*old_ind[i]+1:no_vals*old_ind[i]+no_vals,:].values
            
            # Beginning of tornado plot code
            fig , ax = plt.subplots(length,1, figsize = (15,10))
            title = fig.suptitle(techtype + " Tornado Diagram", fontsize = 30, fontweight="bold", x = 0.58)
    
            for i in range(length):
                # Define masks for LCOH changes greater and less than 0
                pmask = df.loc[no_vals*i+1:no_vals*i+no_vals, ["LCOH Value US c/kwh"]] > 0
                nmask = df.loc[no_vals*i+1:no_vals*i+no_vals:, ["LCOH Value US c/kwh"]] < 0
                # if both masks are empty, delete the axis/parameter since no sensitivity
                if (not any(pmask["LCOH Value US c/kwh"])) and (not any(nmask["LCOH Value US c/kwh"])):
                    fig.delaxes(ax[i])
                    continue

                sns.barplot(df[pmask]["LCOH Value US c/kwh"], ax = ax[i], color = "b", ci=None)
                sns.barplot(df[nmask]["LCOH Value US c/kwh"], ax = ax[i], color = "r", ci=None)
                ax[i].xaxis.set_visible(False)
                ax[i].set_xlim([-120, 120])
                [s.set_visible(False) for s in ax[i].spines.values()]
                ax[i].set_yticklabels(columns[old_ind[i]], fontsize = 15, fontweight = "bold")
                ax[i].yaxis.set_tick_params(length = 0)
                
            del copy
            # add the part of graph that you want
            ax[0].spines['top'].set_visible(True)
            ax[0].xaxis.set_visible(True)
            ax[0].xaxis.tick_top()
            ax[0].set_xlabel("\u0394LCOH% (c/USD)", size = 15)
            ax[0].xaxis.set_label_position('top')
            plt.tight_layout(pad = 0.5)
            title.set_y(0.95)
            fig.subplots_adjust(top=0.85)
            
        plot_to(to_solar, self.solar[index].tech_type)
        plot_to(to_comb, self.comb[index].tech_type)
        
        self.reset()
        
    def pp_1D(self, iter_name):
        
        # Fuel unit dictionary for better readability
        fuel_units = {"NG" : "$/thousand cuf", "PETRO" : "$/gallon", "COAL" : "$/short ton"}
        roots = []
        
        if iter_name.upper() == "INVESTMENT":
            
            for i in range(len(self.solar)):
                self.solar[i].iter_name = "INVESTMENT"
                root = bisection(lambda x: self.solar[i].iterLCOH(x) - self.comb_current[i], -1 * 10**8, 10**8, 100)
             
                if root == None:
                    print("No solution")
                
                else: 
                    roots.append(root)
                    
                #print("The investment price for process parity is {:.2f} $USD".format(root))
        
        elif iter_name.upper() == "FUELPRICE":
            for i in range(len(self.comb)):
                self.comb[i].iter_name = "FUELPRICE"
                root = bisection(lambda x: self.comb[i].iterLCOH(x) - self.solar_current[i], 0, 20, 100)

                if root == None:
                    print("No solution")
                    
                else: 
                    roots.append(root)
                    #print("The fuel price of {} for process parity is {:.2f} {}".format(self.comb.fuel_type, root, fuel_units[self.comb.fuel_type]))
        else:
            print("Not a valid iteration name.")
            return None
        
        self.reset()
        
        return roots
        
    def pp_nD(self, tol = 10, no_val = 3):
        
        no_plots = len(self.solar)
        """Solution Space"""
        for i in range(no_plots):
            self.solar[i].iter_name = "INVESTMENT"
        for i in range(no_plots):
            self.comb[i].iter_name = "FUELPRICE"
            
        #df_sol = pd.DataFrame(columns = ["fuelprice", "investment", "LCOH"])
        i_vals = np.linspace(0 ,10 ** 8, no_val)
        
        fig, ax = plt.subplots(no_plots)

        # for each capital investment (i_vals) iterate to find associated fuel price
        for i in range(no_plots):
            all_roots = []
            for j in i_vals:
                root = bisection(lambda x: self.comb[i].iterLCOH(x) - self.solar[i].iterLCOH(j), -10, 30, 100)
                all_roots.append(root)
                #df_sol = df_sol.append({"fuelprice": root, "investment" : i / 10**7}, ignore_index = True)     
            ax[i].scatter(all_roots, i_vals / 10**7)
            ax[i].set_xlabel("Fuel Price (c/kWh)") 
            ax[i].set_ylabel("Investment(10 MM USD)")
            
        fig.tight_layout()
        fig.suptitle("Process Parity Solution Space", fontweight = "bold", fontsize = 14)
        
        self.reset()
        
        #return df_sol
    
    def pb_irr(self):
        
        def get_pb(index):
            
            # apply iter value
# =============================================================================
#             if iter_name == "FUELPRICE":
#                 self.comb[index].fuel_price = iter_value
#             elif iter_name == "INVESTMENT":
#                 self.solar[index].iterLCOH("INVESTMENT", iter_value)
#             else:
#                 print("No such iteration parameter")
# =============================================================================
            
            # Get the denominator of LCOH equation to calculate the numerator
            energy_yield = sum([self.solar[index].load_avg * 31536000 / 
                                (1 + self.solar[index].discount_rate[0]) ** i 
                                for i in range(self.solar[index].p_time[0])])
    
            # numerator of the LCOH equation and first term (total solar cost discounted to year0)
            value = list(-self.solar_current[index] * energy_yield / (3600*100))
        
            lifetime = self.solar[index].p_time[0]
            
            # create the bar values for the cash flow diagram using combustion OM
            for i in range(1, lifetime):
                value.append(self.comb[index].OM(i)[0]/(1+self.solar[index].discount_rate[0])**i)
                
            pb_year = lifetime

            # If total savings less than cost of solar system over lifetime
            if np.sum(value[1:]) < -value[0]:
                
                while (np.sum(value[1:]) < -value[0]) & (pb_year <= 49):
                    value.append(self.comb[index].OM(pb_year)[0]/(1+self.solar[index].discount_rate[0])**pb_year)
                    pb_year += 1
        
                if pb_year == 50:
                    print("No Payback Year")                
                
            else: 
                cash_flow = np.array(value[1:])
                pb_year = np.argmax(cash_flow.cumsum() > -value[0])
                
            return pb_year, value
        
            
        def plot_cf(index, value):  
            """ plot cash flow diagram"""
            t = np.arange(self.solar[index].year, self.solar[index].year + len(value), 1)
            
            fig, ax1 = plt.subplots(figsize=(12,6))
            ax1.set_ylim([-5 * 10**7 , 10**7])
        
        
            title = fig.suptitle("Cash Flow Diagram", fontweight = "bold", fontsize = 20)
            ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
        
            ax1.bar(t[:20], value[:20], color = ["#85bb65" if val >= 0 else "#D43E3E" for val in value], alpha = 0.8)
            ax1.set_xlabel("Year", fontsize = 15)
        
            ax1.set_ylabel('Annualized Cash Flow', color = "black", fontsize = 15)
            ax1.tick_params('y', colors = "black")
    
            fig.tight_layout()
        
            ax1.spines['top'].set_visible(False)
            ax1.spines['bottom'].set_visible(False)
            ax1.spines['right'].set_visible(False)
            ax1.spines['left'].set_visible(False)
            
            title.set_y(0.95)
            fig.subplots_adjust(top=0.85)
            
        no_plots = range(len(self.solar))
        
        for i in no_plots:
            payback, value = get_pb(i)
            #payback, t, value, iter_value = iter_pb(i,iter_name, 15)
            #print("The payback period is achieved within the lifetime of {} years when the {} is {}.".format(payback,iter_name,iter_value))
            plot_cf(i, value)
        
        self.reset()

if __name__ == "__main__":
    
    import pandas as pd
    test_obj = PrPar()

    state_abbr =[]
    comb_lcoh = []
    
    for i in test_obj.comb:
        comb_lcoh.append(i.em_costs)
        state_abbr.append(i.state_abbr)

    results = pd.DataFrame()
    results["STUSPS"] = state_abbr
    results["comblcoh"] = comb_lcoh
    
    results.to_csv("state_lcoh_em.csv")
