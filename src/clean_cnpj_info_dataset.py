from itertools import chain
import json
import numpy as np
import pandas as pd

def decompose_main_activity(value):
    struct = json.loads(value.replace('\'', '"'))
    return pd.Series([*struct]). \
        rename_axis({'code': 'main_activity_code', 'text': 'main_activity'})

def decompose_secondary_activities(value):
    struct = json.loads(value.replace('\'', '"'))
    if bool(struct) and struct[0]['text'] != 'Não informada':
        new_attributes = [pd.Series(activity). \
            rename_axis({'code': 'secondary_activity_%i_code' % (index + 1),
                         'text': 'secondary_activity_%i' % (index + 1)})
            for index, activity in enumerate(struct)]
        return pd.concat(new_attributes)
    else:
        return pd.Series()



data = pd.read_csv('data/cnpj_info.xz',
                   dtype={'atividade_principal': np.str,
                          'atividades_secundarias': np.str,
                          'complemento': np.str,
                          'efr': np.str,
                          'email': np.str,
                          'message': np.str,
                          'motivo_situacao': np.str,
                          'situacao_especial': np.str})
data = data.drop_duplicates('cnpj')
data.rename(columns={
    'abertura': 'opening',
    'atividade_principal': 'main_activity',
    'atividades_secundarias': 'secondary_activities',
    'bairro': 'neighborhood',
    'cep': 'zip_code',
    'complemento': 'additional_address_details',
    'data_situacao_especial': 'special_situation_date',
    'data_situacao': 'situation_date',
    'efr': 'responsible_federative_entity',
    'fantasia': 'trade_name',
    'logradouro': 'address',
    'motivo_situacao': 'situation_reason',
    'municipio': 'city',
    'natureza_juridica': 'legal_nature',
    'nome': 'name',
    'numero': 'number',
    'situacao_especial': 'special_situation',
    'situacao': 'situation',
    'telefone': 'phone',
    'tipo': 'type',
    'uf': 'state',
    'ultima_atualizacao': 'last_updated',
}, inplace=True)

data['legal_nature'] = data['legal_nature'].astype('category')
data['message'] = data['message'].astype('category')
data['responsible_federative_entity'] = \
    data['responsible_federative_entity'].astype('category')
data['situation_reason'] = data['situation_reason'].astype('category')
data['situation'] = data['situation'].astype('category')
data['special_situation'] = data['special_situation'].astype('category')
data['status'] = data['status'].astype('category')
data['type'] = data['type'].astype('category')

data['main_activity'] = data['main_activity'].fillna('{}')
data['secondary_activities'] = data['secondary_activities'].fillna('{}')

data = pd.concat([
    data.drop(['main_activity', 'secondary_activities'], axis=1),
    data['main_activity'].apply(decompose_main_activity),
    data['secondary_activities'].apply(decompose_secondary_activities)],
    axis=1)

data.to_csv('data/companies.xz', compression='xz', encoding='utf-8', index=False)
