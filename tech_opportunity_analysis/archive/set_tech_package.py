
class tech_package:

    """

    """
    def __init__(self, solar_tech):

        names_list = ['dsg_lf', 'ptc_notes', 'ptc_tes', 'pv', 'swh']

        if solar_tech not in names_list:

            raise ValueError(
                '"{}" not in list of technology packages {}.'.format(
                    solar_tech, names_list)
                )

        else:

            self.solar_tech = solar_tech

        default_pkgparams = {
            'dsg_lf': {'industries': ['all'], 'temp_range':[0,90],
                       'end_uses': ['Conventional Boiler Use',
                                    'CHP and/or Cogeneration Processes',\
                                    'Process Heating']},
            'ptc_notes': {'industries': ['all'], 'temp_range':[],
                          'end_uses': ['Conventional Boiler Use',
                                       'CHP and/or Cogeneration Processes',
                                       'Process Heating']},
            'ptc_tes': {'industries': ['all'], 'temp_range':[],
                        'end_uses': ['Conventional Boiler Use',
                                     'CHP and/or Cogeneration Processes',
                                     'Process Heating']},
            'pv': {'industries': ['all'], 'temp_range':[],
                   'end_uses': ['Conventional Boiler Use',
                                'CHP and/or Cogeneration Processes',
                                'Process Heating']},
            'swh': {'industries': ['all'], 'temp_range':[],
                    'end_uses': ['Conventional Boiler Use',
                                 'CHP and/or Cogeneration Processes',
                                 'Process Heating']}
            }

        self.pkgparams = default_pkgparams[self.solar_tech]

    def get_load_params(self):
        """
        Return relevant industries, temperature range, and enduses for
        technology package
        """

        return self.pkgparams[self.solar_tech]

    def set_load_params(self, new_params={}):
        """
        Provide new parameters as dictionary with keys as industries,
        temp_range, end_uses; and values as lists.
        """

        for k, v in new_params.items():

            self.pkgparams[self.solar_tech][k] = v

    def regionalize_county_energy(mecs_energy_data):
        """
        Distill MECS-based energy estimates into their regional energy
        intensities (by NAICS, employment size class, end use, and fuel) and by
        establishment count
        """

        mecs_intensity = mecs_energy_data.groupby(
            ['MECS_Region','naics', 'Emp_Size', 'End_use', 'MECS_FT']
            ).MMBtu.sum()

        mecs_establishments = mecs_energy_data.groupby(
            ['MECS_Region','naics', 'Emp_Size','End_use', 'MECS_FT']
            ).est_count.mean()

        mecs_intensity = mecs_intensity.divide(mecs_establishments)

        return mecs_intensity, mecs_establishments

    def select_energy_data(self, county_energy_data):
        """
        Selects relevant county energy data based on tech package.
        """

        pkg_params = self.pkg_params[self.solar_tech]

        selected_data = county_energy_data[
            (county_energy_data.Temp_C.between(pkg_params.temp_range)) &
            (county_energy_data.End_use.isin(pkg_params.end_uses))
            ]

        if pkg_params['industries'] != 'all':

            selected_data = select_data[
                selected_data.naics.isin(pkg_params['industries'])
                ]

        ghgrp_data = select_data[selected_data.source_data=='ghgrp']

        mecs_data, mecs_establishments = regionalize_county_energy(
                selected_data[selected_data.source_data=='mecs_ipf']
                )

        final_data = pd.concat([ghgrp_data, mecs_data], axis=0,
                               ignore_index=False)

        return final_data, mecs_establishments











        return techpkg_energy







    # define a property for tech parameters
    def tech_load_params():
        doc = "The  property."
        def fget(self):
            return self._
        def fset(self, value):
            self._ = value
        def fdel(self):
            del self._
        return locals()
     = property(**())
