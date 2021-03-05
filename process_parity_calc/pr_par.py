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
    
    def __init__(self, m_obj, l_obj, new_obj = True, form = False):
        """ solar_tech, comb_tech should be appropriate factory objects"""

        self.m_obj = m_obj
        self.l_obj = l_obj
        
        solar_tech = ["PTCTES"]
        comb_tech = ["BOILER"]
        counties =["6037"]

        if new_obj:
            if form:
                self.s_form = form[0]
                self.c_form = form[1]
                
            else:
                print("\nCreating solar object")
                self.s_form = FormatMaker().create_format({"tech": solar_tech, "county": counties})
                    
                print("\nCreating combustion object")  
                self.c_form = FormatMaker().create_format({"tech": comb_tech, "county": counties})
                
        self.solar = [CLO.LCOHFactory().create_LCOH(i, self.m_obj, self.l_obj) for i in self.s_form]
        self.comb = [CLO.LCOHFactory().create_LCOH(i, self.m_obj, self.l_obj) for i in self.c_form]
        self.solar_current = list(map(lambda x: x.calculate_LCOH(), self.solar))
        self.comb_current = list(map(lambda x: x.calculate_LCOH(), self.comb))
            
            
    def reset(self):
        """ Reset the object to default state after running an analysis"""
        self.__init__(self.m_obj, self.l_obj, new_obj = False)
        
    def mc_analysis(self, index):
        """ Process Monte Carlo results"""
        
        # Grab Monte Carlo Dataframes from each LCOH object
        mc_solar = self.solar[index].simulate("MC")
        mc_comb = self.comb[index].simulate("MC")
        
        # Plot distributions of the LCOH variable
        sns.distplot(mc_solar["LCOH Value US c/kwh"], kde = True, axlabel = "LCOH Cents/KWH", label = self.solar[index].tech_type)
        sns.distplot(mc_comb["LCOH Value US c/kwh"], kde = True, axlabel = "LCOH Cents/KWH", label = self.comb[index].tech_type)
        plt.legend()
        
    def to_analysis(self, index):
        """Process Tornado Diagram Data"""
        
        # Read in the tornado diagram simulation dataframe
        to_solar = self.solar[index].simulate("TO").reset_index(drop = True)
        to_comb = self.comb[index].simulate("TO").reset_index(drop = True)

        def plot_to(df, techtype):
            """ Plot Tornado Diagrams"""
            # Grab default LCOH value
            val_LCOH = df["LCOH Value US c/kwh"][1]
            # Find % change from default LCOH value
            df["LCOH Value US c/kwh"] = (df["LCOH Value US c/kwh"] - val_LCOH)/val_LCOH * 100
            #round % changes
            df = df.round({"LCOH Value US c/kwh":2})
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
            fig , ax = plt.subplots(length,1, figsize = (10,6.66), dpi = 150)
            title = fig.suptitle(techtype + " Tornado Diagram", fontsize = 30, fontweight="bold", x = 0.58)
    
            for i in range(length):
                # Define masks for LCOH changes greater and less than tolerance
                tolerance = 0.005
                pmask = df.loc[no_vals*i+1:no_vals*i+no_vals, ["LCOH Value US c/kwh"]] > tolerance
                nmask = df.loc[no_vals*i+1:no_vals*i+no_vals:, ["LCOH Value US c/kwh"]] < -1 * tolerance
                # if any masks is empty, delete the axis/parameter since no sensitivity
                if (not any(pmask["LCOH Value US c/kwh"])) and (not any(nmask["LCOH Value US c/kwh"])):
                    fig.delaxes(ax[i])
                    continue
                sns.barplot(df[pmask]["LCOH Value US c/kwh"], ax = ax[i], color = "r", ci=None)
                sns.barplot(df[nmask]["LCOH Value US c/kwh"], ax = ax[i], color = "g", ci=None)
                ax[i].xaxis.set_visible(False)
                ax[i].set_xlim([-25, 25])
                [s.set_visible(False) for s in ax[i].spines.values()]

                ax[i].set_yticklabels(columns[old_ind[i]], fontsize = 15, fontweight = "bold")
                ax[i].yaxis.set_tick_params(length = 0)
                
            del copy
            # add the part of graph that you want
            ax[0].spines['top'].set_visible(True)
            ax[0].xaxis.set_visible(True)
            ax[0].xaxis.tick_top()
            ax[0].set_xlabel("\u0394LCOH%", size = 15)
            ax[0].xaxis.set_label_position('top')
            plt.tight_layout(pad = 0.5)
            title.set_y(1.0)
            fig.subplots_adjust(top=0.85)
            
        plot_to(to_solar, self.solar[index].tech_type)
        plot_to(to_comb, self.comb[index].tech_type)
        
        
    def pp_1D(self, iter_name):

        roots = []
        
        if iter_name.upper() == "INVESTMENT":
            
            for i in range(len(self.solar)):
                self.solar[i].iter_name = "INVESTMENT"
                root = bisection(lambda x: self.solar[i].iterLCOH(x) - self.comb_current[i], -1*10**8, 1*10**8, 200)
             
                if root == None:
                    print("No solution")
                    
                else: 
                    try:
                        root /= self.solar[i].model.sys_size

                    except AttributeError:
                        root /= self.solar[i].smodel.sys_size
                        
                    roots.append(root)
                    
        
        elif iter_name.upper() == "FUELPRICE":

            for i in range(len(self.comb)):
                self.comb[i].iter_name = "FUELPRICE"
                self.solar[i].iter_name = "FUELPRICE"
                root = bisection(lambda x: self.comb[i].iterLCOH(x) - self.solar[i].iterLCOH(x), -100, 100, 200)

                if root == None:
                    print("No solution")
                    
                else: 
                    roots.append(root)
        else:
            print("Not a valid iteration name.")
            return None
        
        return roots
        
    def pp_nD(self, no_val = 50):
        
        no_plots = len(self.solar)
        """Solution Space"""
        for i in range(no_plots):
            self.solar[i].iter_name = "BOTH"
        for i in range(no_plots):
            self.comb[i].iter_name = "FUELPRICE"  
        fig, ax = plt.subplots(no_plots, figsize = (10,15))

        # for each capital investment (i_vals) iterate to find associated fuel price
        for i in range(no_plots):
            i_vals = np.linspace(0 ,5*10**6, no_val)
            all_roots = []
            for j in i_vals:
                root = bisection(lambda x: self.comb[i].iterLCOH(x) - self.solar[i].iterLCOH((x,j)), -100, 100, 100)
                all_roots.append(root)
                #df_sol = df_sol.append({"fuelprice": root, "investment" : i / 10**7}, ignore_index = True)
            try:
                #Normalize to kwdc
                i_vals_norm = i_vals / self.solar[i].model.sys_size
            except AttributeError:
                i_vals_norm = i_vals / self.solar[i].smodel.sys_size
                
            ax[i].scatter(all_roots, i_vals_norm)
            ax[i].set_xlabel("Fuel Price (c/kWh)") 
            ax[i].set_ylabel("USD/kwp")
            
        fig.tight_layout()
        fig.suptitle("Process Parity Solution Space", fontweight = "bold", fontsize = 14)
        
    def fp_pb(self, itername = "FUELPRICE"):
        
        pb=[]
        if itername == "FUELPRICE":
            vals = np.linspace(1,20,20)

        def get_pb(index, itername, price):
            if itername == "FUELPRICE":
                self.comb[index].iter_name = itername
                self.solar[index].iter_name = itername
                
                ptime = [100]
                
                self.comb[index].p_time = ptime
                self.solar[index].p_time = ptime
                
                self.comb[index].iterLCOH(price)
                self.solar[index].iterLCOH(price)               
                self.comb[index].calculate_LCOH()
                self.solar[index].calculate_LCOH()
                
            i_solar = self.solar[index].year0[0]
            a_solar = self.solar[index].cashflow
            a_comb = self.comb[index].cashflow
            savings = [i-j for i,j in zip(a_solar,a_comb)]

            if sum(savings) + i_solar <=  0:
                
                year = 1
                
                while sum(savings[0:year]) + i_solar > 0:
                    year += 1

                pb_year = year-1 + abs((i_solar + sum(savings[0:year-1]))/(savings[year-1]))

                savings = -1 * np.array(savings)
                savings[0] += -1 * i_solar
                return pb_year
                
            else:
                savings = -1 * np.array(savings)
                savings[0] += -1 * i_solar               
                return False
        
        for i in range(len(self.solar)):
            pblist = []
            for j in vals:
                pblist.append(get_pb(i, "FUELPRICE", j))
            pb.append(pblist)
            
        return pb

    
    def pb(self):
        
        def get_pb(index):
            
            # max year to check for payback period - else no point
            ptime = [100]
            # set period of analysis calculations
            self.solar[index].p_time = ptime
            self.comb[index].p_time = ptime
            # calculate LCOH to get payback period variables
            self.solar[index].calculate_LCOH()
            self.comb[index].calculate_LCOH()
            
            i_solar = self.solar[index].year0[0]
            a_solar = self.solar[index].cashflow
            a_comb = self.comb[index].cashflow
            savings = [i-j for i,j in zip(a_solar,a_comb)]
            if sum(savings) + i_solar <=  0:
                
                year = 1
                
                while sum(savings[0:year]) + i_solar > 0:
                    year += 1

                pb_year = year-1 + abs((i_solar + sum(savings[0:year-1]))/(savings[year-1]))
                
                #add year 0 solar cost to savings in year 0
                savings = -1 * np.array(savings)
                savings[0] += -1 * i_solar
                return (pb_year, savings[0:year+1])
                
            else:
                savings = -1 * np.array(savings)
                savings[0] += -1 * i_solar               
                return (False, savings[0:26])
            
        def plot_cf(index, value):  
            """ plot cash flow diagram"""
            t = np.arange(self.solar[index].year, self.solar[index].year + len(value), 1)
            
            fig, ax1 = plt.subplots(figsize=(12,6))
            ax1.set_ylim([-10 * 10**6 , 0.8*10**6])
        
            title = fig.suptitle("1 MWAC PVRH Cash Flow - Dickson, Tennessee", fontweight = "bold", fontsize = 20)
            ax1.xaxis.set_major_locator(MaxNLocator(integer=True))
        
            ax1.bar(t, value, color = ["#85bb65" if val >= 0 else "#D43E3E" for val in value], alpha = 0.8)
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
        
        payback = []
        
        for i in no_plots:
            payback.append(get_pb(i)[0])

        return payback

            
    def irr(self):
        def get_irr(index):
            
            def irr_eq(dr):
                # set period of analysis calculations
                self.solar[index].discount_rate = dr
                self.comb[index].discount_rate = dr
                # calculate LCOH to get payback period variables
                self.solar[index].calculate_LCOH()
                self.comb[index].calculate_LCOH()
                
                i_solar = self.solar[index].year0[0]
                a_solar = self.solar[index].cashflow
                a_comb = self.comb[index].cashflow
                savings = [i-j for i,j in zip(a_solar,a_comb)]
                
                return sum(savings) + i_solar    
            
            irr = bisection(lambda x: irr_eq([x]), 0 ,40, 100)
            
            return irr
        
        no_plots = range(len(self.solar))        
        irr = []
        for i in no_plots:
            irr.append(get_irr(i))
        
        return irr


if __name__ == "__main__":

    from lcoh_config import ParamMethods as pm
    from model_config import ModelParams as mp    




