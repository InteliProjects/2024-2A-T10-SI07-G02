import pandas as pd
import unicodedata
import re

# Carregando os arquivos CSV
crd1_data = pd.read_csv('raw_data/raw_CRD1.csv')
municipios_data = pd.read_csv('raw_data/MUNICIPIOS.csv')

def normalize_text(text):
    text = text.upper()
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text

# Aplicar a padronização em todas as colunas, exceto AddressType e TypeOfAddress
columns_to_normalize = crd1_data.columns.difference(['AddressType', 'TypeOfAddress'])
crd1_data[columns_to_normalize] = crd1_data[columns_to_normalize].applymap(lambda x: normalize_text(str(x)) if isinstance(x, str) else x)

# Certificar que TypeOfAddress está em CAPS LOCK
crd1_data['TypeOfAddress'] = crd1_data['TypeOfAddress'].str.upper()

# Apagar completamente a coluna AddressName antes de aplicar a regra
crd1_data['AddressName'] = ""

def set_address(row):
    if row['AddressType'].strip().upper() == 'BO_SHIPTO':
        if row['ParentKey'].startswith('C'):
            return 'PAGAR A'
        elif row['ParentKey'].startswith('F'):
            return 'COBRANÇA'
    elif row['AddressType'].strip().upper() == 'BO_BILLTO':
        if row['ParentKey'].startswith('C'):
            return 'COBRANÇA'
        elif row['ParentKey'].startswith('F'):
            return 'PAGAR A'
    return ''

# Aplicar as regras para definir o AddressName e substituir totalmente a coluna
crd1_data['AddressName'] = crd1_data.apply(set_address, axis=1)

# Enumeração de endereços repetidos
def enumerate_addresses(df):
    df['AddressName'] = df.groupby(['ParentKey', 'AddressType'])['AddressName'].transform(lambda x: x + (x.duplicated(keep=False).cumsum()).astype(str).replace('0', ''))
    return df

crd1_data = enumerate_addresses(crd1_data)

# Substituir valores em TypeOfAddress
type_of_address_mapping = {
    'AV': 'AVENIDA',
    'PC': 'PRAÇA',
    'TR': 'TRECHO'
}

crd1_data['TypeOfAddress'] = crd1_data['TypeOfAddress'].replace(type_of_address_mapping)

# Expansão dos tipos de endereço em AddressType
addrtype_mapping = {
    'Av': 'AVENIDA',
    'Pc': 'PRAÇA',
    'Rod': 'RODOVIA',
    'Al': 'ALAMEDA',
    'Lgo': 'LARGO'
}

def expand_addresstype(row):
    addrtype = row['AddressType']
    if pd.isnull(addrtype) and pd.notnull(row['Street']):
        for key, value in addrtype_mapping.items():
            if row['Street'].startswith(key):
                row['AddressType'] = value
                row['Street'] = row['Street'][len(key):].strip()
                break
    elif addrtype in addrtype_mapping:
        row['AddressType'] = addrtype_mapping[addrtype]
    return row

crd1_data = crd1_data.apply(expand_addresstype, axis=1)

# Processamento da coluna Street e StreetNo
def process_street(row):
    street = row['Street']
    if pd.notnull(street):
        street_parts = street.split()
        if street_parts[-1].isdigit() and pd.isnull(row['StreetNo']):
            row['StreetNo'] = street_parts.pop()
            row['Street'] = " ".join(street_parts)
    return row

crd1_data = crd1_data.apply(process_street, axis=1)

# Mapeamento para Block e City
block_city_mapping = {
    'Jd': 'JARDIM',
    'Vl': 'VILA',
    'Distr': 'DISTRITO',
    'Cid': 'CIDADE',
    'Set': 'SETOR',
    'Pq': 'PARQUE'
}

crd1_data['Block'] = crd1_data['Block'].replace(block_city_mapping, regex=True)
crd1_data['City'] = crd1_data['City'].replace(block_city_mapping, regex=True)

# Separar registros com campos vazios em Block e City
missing_block_city = crd1_data[(crd1_data['Block'].isnull()) | (crd1_data['City'].isnull())]

# Mapeamento de County com base em MUNICIPIOS - apenas para códigos numéricos
municipios_dict = dict(zip(municipios_data['Chave autom.'], municipios_data['Nome do município']))

def map_county(value):
    if pd.isnull(value):
        return value
    try:
        # Substituir pelo nome do município
        county_name = municipios_dict[int(value)]
        # Normalizar o texto (caps lock, sem acento, sem pontuação)
        return normalize_text(county_name)
    except (ValueError, KeyError):
        return value

crd1_data['County'] = crd1_data['County'].apply(map_county)

# Padronização de ZipCode
def format_zipcode(zipcode):
    if pd.isnull(zipcode):
        return None
    zipcode = str(zipcode)  # Converte para string
    zipcode = re.sub(r'\D', '', zipcode)  # Remove caracteres não numéricos
    if len(zipcode) == 8:
        return zipcode
    elif len(zipcode) == 7:
        return '0' + zipcode
    else:
        return None

crd1_data['ZipCode'] = crd1_data['ZipCode'].apply(format_zipcode)

# Separar registros com CEP inválidos
invalid_zipcodes = crd1_data[crd1_data['ZipCode'].isnull()]

# Salvar dados processados
crd1_data.to_csv('processed_data/processed_CRD1.csv', index=False)

# Salvar dados para verificação manual
missing_block_city.to_csv('processed_data/missing_block_city_CRD1.csv', index=False)
invalid_zipcodes.to_csv('processed_data/invalid_zipcodes_CRD1.csv', index=False)