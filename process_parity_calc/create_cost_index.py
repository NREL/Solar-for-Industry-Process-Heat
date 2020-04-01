# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 14:52:58 2020

Creates a csv file of cost indexes based off the raw text file
obtained at https://www.chemengonline.com/pci-home

Please adjust years to reflect the years of data you downloaded.

Did cost indices by year - not months.  



@author: wxi
"""
import re
import pandas as pd
import os



def create_index(path_folder):
    """ 
    https://fred.stlouisfed.org/series/WPU061 - producer price index csv
    
    https://www.chemengonline.com/pci - chemical eng cost index - by year
    
    """
    
    try:
        if not type(path_folder) == str:

            raise AssertionError("Please enter a string.")

        if not os.path.exists(path_folder):

            raise AssertionError("Not a valid path/directory doesn't exist.")

    except AssertionError as e:

        print(e)
    
    path = path_folder
    
    cost_dict = {}

    index_list = ["CE INDEX", "Equipment", "Heat Exchangers and Tanks", 
                  "Process Machinery", "Pipe, valves and fittings", 
                  "Process Instruments", 
                  "Pumps and Compressors", "Electrical equipment", 
                  "Structural supports", "Construction Labor", "Buildings", 
                  "Engineering Supervision"]
    
    def remove_nonnumeric(string):
        dummy_var = float(re.sub(r'[^\d.]', '', string))
        return dummy_var
    
    # grab raw txt 
    file = open(os.path.join(path, "cost_index.txt"), "r")
    text = file.read()
    file.close()
    
    # modify initial year here
    data = text.split("1978")
    
    # Remove the initial few words
    data.pop(0)
    cost_dict['1978'] = data[0:12]
    del data[0:12]
    data = data[0]
    
    # Go through text and grab data points as a function of year
    for i in range(1979,2019):
        data = data.split(str(i))
        data.pop(0)
        cost_dict[str(i)] = data[0:12]
        del data[0:12]
        data=data[0]

    df = pd.DataFrame(cost_dict, index = index_list)

    cost_index = df.applymap(remove_nonnumeric)
    
    ppi = pd.read_csv(os.path.join(path,"WPU061.csv"))
    
    ppi = ppi.transpose()
    ppi.columns = ppi.iloc[0,:].apply(lambda x: ''.join(list(x[-4:])))
    ppi.drop(["DATE"], inplace = True)
    ppi = ppi.apply(pd.to_numeric)
    ppi = ppi.groupby(by = ppi.columns, axis = 1).mean()
    
    comb_index = pd.concat([cost_index, ppi], join='inner')
    comb_index = comb_index.rename(index = {"WPU061" : "PPI_CHEM"})
    
    comb_index.to_csv(os.path.join(path_folder, "cost_index_data.csv"))

if __name__ == "__main__":
    
    create_index(".\calculation_data")

    if os.path.exists(".\calculation_data\cost_index_data.csv"):

        print("File made.")

    else:
        print("Problem with file creation.")