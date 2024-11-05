import pandas as pd

# Carregar o arquivo CSV
crd7_data = pd.read_csv('raw_data/raw_CRD7.csv')

# Garantir que todas as colunas TaxId0 até TaxId8 estejam no dataframe
required_columns = ['ParentKey', 'TaxId0', 'TaxId1', 'TaxId2', 'TaxId3', 'TaxId4', 'TaxId5', 'TaxId6', 'TaxId7', 'TaxId8']

# Adicionar colunas ausentes com valores NaN
for col in required_columns:
    if col not in crd7_data.columns:
        crd7_data[col] = None

def generate_unique_lines(group):
    # Criação das 3 linhas para cada ParentKey
    return pd.DataFrame({
        'ParentKey': [group['ParentKey'].iloc[0]] * 3,
        'LineNum': [0, 1, 2],
        'Address': ['', 'COBRANCA', 'ENTREGA'],
        'TaxId0': [group['TaxId0'].iloc[0]] * 3,
        'TaxId1': [group['TaxId1'].iloc[0]] * 3,
        'TaxId2': [group['TaxId2'].iloc[0]] * 3,
        'TaxId3': [group['TaxId3'].iloc[0]] * 3,
        'TaxId4': [group['TaxId4'].iloc[0]] * 3,
        'TaxId5': [group['TaxId5'].iloc[0]] * 3,
        'TaxId6': [group['TaxId6'].iloc[0]] * 3,
        'TaxId7': [group['TaxId7'].iloc[0]] * 3,
        'TaxId8': [group['TaxId8'].iloc[0]] * 3
    })

# Agrupar por ParentKey e gerar linhas únicas
crd7_expanded = crd7_data.groupby('ParentKey').apply(generate_unique_lines).reset_index(drop=True)

# Salvar o arquivo final como CSV
crd7_expanded.to_csv('processed_data/processed_CRD7.csv', index=False)
