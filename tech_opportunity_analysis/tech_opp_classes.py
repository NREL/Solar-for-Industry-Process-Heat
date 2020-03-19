
class boiler:

  def __init__(self, fuel_type, naics):

    self.efficiency = {'natural_gas': 0.83, 'fuel_oil': 0.77, 'coal': 0.6}

    self.hw = {311111: 0.2}


  def calc_process_energy(self, county_annual_energy):

    boiler_energy = self.effiency * county_annual_energy * self.hw

    return boiler_energy








# boilers_yay = boiler(natural_gas, 311111)

# boilers_yay.efficiency  0.83


# boilers_yay.calc_process_energy(1000)
