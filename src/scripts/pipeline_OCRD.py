import pandas as pd
import unicodedata
import re

# Carregamento do arquivo CSV
file_path = 'raw_data/raw_OCRD.csv'
ocrd_data = pd.read_csv(file_path)

# Funções auxiliares
def remove_special_characters(text):
    nfkd_form = unicodedata.normalize('NFKD', text)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def clean_phone_number(phone):
    if isinstance(phone, str):
        # Remove todos os não dígitos
        phone_clean = re.sub(r'\D', '', phone)
        if len(phone_clean) >= 8:
            # Armazene os dois primeiros dígitos como DDD se o telefone tiver mais de 8 dígitos
            ddd = phone_clean[:2]
            if phone_clean.startswith("0800"):
                return phone_clean, ddd
            else:
                main_part = phone_clean[-8:]
                return f"{ddd}-{main_part[:4]}-{main_part[4:]}", ddd
        return phone_clean, None
    return phone, None

def process_ocrd_data(df):
    df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)  # Converter para CAPS LOCK
    df = df.applymap(lambda x: remove_special_characters(x) if isinstance(x, str) else x)  # Remover acentos e caracteres especiais
    df = df.replace({'ç': 'c'}, regex=True)  # Substituir 'ç' por 'c'
    
    # Remover pontuação de todas as colunas
    df = df.replace(r'[^\w\s]', '', regex=True)
    
    # Substituir valores de CardType sem alterar o caso das letras
    df['CardType'] = df['CardType'].replace({
        'FORNECEDOR': 'cSupplier',
        'CLIENTE': 'cCustomer',
        'LEAD': 'cLid'
    })
    
    # Processar Phone 1
    phone1_info = df['Phone1'].apply(lambda x: clean_phone_number(str(x)) if pd.notnull(x) else (x, None))
    df['Phone1'], df['Phone2'] = zip(*phone1_info)  # Armazenar DDD na coluna Phone 2
    
    # Identificar colunas para manutenção manual
    manual_fix = df[(df['Phone1'].str.contains(r'[a-zA-Z]', na=False)) | (df['Phone2'].str.contains(r'[a-zA-Z]', na=False))]

    return df, manual_fix

# Processando os dados
processed_data, manual_fix_data = process_ocrd_data(ocrd_data)

# Salvar os dados processados e os dados que precisam de correção manual em arquivos CSV
processed_csv_path = 'processed_data/processed_OCRD.csv'
manual_fix_csv_path = 'processed_data/manual_fix_OCRD.csv'

processed_data.to_csv(processed_csv_path, index=False)
manual_fix_data.to_csv(manual_fix_csv_path, index=False)
