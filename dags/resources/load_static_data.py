state_dict = {
    # Mapping of first two digits of district id to the corresponding state
    '01': 'Schleswig-Holstein',
    '02': 'Hamburg',
    '03': 'Niedersachsen',
    '04': 'Bremen',
    '05': 'Nordrhein-Westfalen',
    '06': 'Hessen',
    '07': 'Rheinland-Pfalz',
    '08': 'Baden-Württemberg',
    '09': 'Bayern',
    '10': 'Saarland',
    '11': 'Berlin',
    '12': 'Brandenburg',
    '13': 'Mecklenburg-Vorpommern',
    '14': 'Sachsen',
    '15': 'Sachsen-Anhalt',
    '16': 'Thüringen'
}


def preprocess_district_data(filepath_in: str = 'static_data/04-kreise.xlsx',
                             filepath_out: str = 'districts.csv') -> str:
    import pandas as pd
    colnames = ['district_id', 'type', 'name', 'nuts3', 'area_sqare_km', 'pop_total',
                'pop_male', 'pop_female', 'pop_per_square_km']
    districts = pd.read_excel(filepath_in, sheet_name=1, skiprows=6, header=None,
                              names=colnames)
    districts = districts[districts['nuts3'].isna() == False]
    assert districts.isna().sum().sum() == 0
    districts = districts.astype({'pop_total': int,
                                'pop_male': int,
                                'pop_female': int,
                                'pop_per_square_km': int})
    districts['state'] = districts['district_id'].apply(lambda s: s[:2]).map(state_dict)
    # TODO: subset columns?
    districts.to_csv(filepath_out, index=False)
    return filepath_out
