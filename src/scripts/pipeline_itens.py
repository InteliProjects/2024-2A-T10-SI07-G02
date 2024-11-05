import pandas as pd

# Carrega o arquivo CSV de itens
raw_itens_path = 'raw_data/raw_itens.csv'
raw_itens = pd.read_csv(raw_itens_path)

# Dicionários de mapeamento para as colunas de texto para numéricas
item_class_mapping = {
    'Serviço': 1,
    'Material': 2
}

material_type_mapping = {
    'Mercadoria para revenda': 0,
    'Materia-prima': 1,
    'Embalagem': 2,
    'Mercadoria em processo': 3,
    'Produtos acabados': 4,
    'Subproduto': 5,
    'Produto Intermediário': 6,
    'Material de Uso e Consumo': 7,
    'Ativo imobilizado': 8,
    'Serviços': 9,
    'Outros insumos': 10,
    'Outras': 99
}

product_source_mapping = {
    'Nacional, exceto as indicadas nos códigos 3, 4, 5 e 8': 0,
    'Estrangeira - Importação direta, exceto a indicada no código 6': 1,
    'Estrangeira - Adquirida no mercado interno, exceto a indicada no código 7': 2,
    'Nacional, mercadoria ou bem com Conteúdo de Importação superior a 40% e inferior ou igual a 70%': 3,
    'Nacional, cuja produção tenha sido feita em conformidade com os processos produtivos básicos': 4,
    'Nacional, mercadoria ou bem com Conteúdo de Importação inferior ou igual a 40%': 5,
    'Estrangeira - Importação direta, sem similar nacional, constante em lista de Resolução CAMEX e gás natural': 6,
    'Estrangeira - Adquirida no mercado interno, sem similar nacional, constante lista CAMEX e gás natural': 7,
    'Nacional, mercadoria ou bem com Conteúdo de Importação superior a 70%': 8
}

columns_to_convert = ['PurchaseItem', 'SalesItem', 'InventoryItem', 
                      'ManageSerialNumbers', 'ManageBatchNumbers', 'IsPhantom']

for col in columns_to_convert:
    raw_itens[col] = raw_itens[col].replace({'Sim': 'Y', 'Não': 'N'})

# Aplica as conversões para as colunas relevantes
raw_itens['ItemClass'] = raw_itens['ItemClass'].map(item_class_mapping)
raw_itens['MaterialType'] = raw_itens['MaterialType'].map(material_type_mapping)
raw_itens['ProductSource'] = raw_itens['ProductSource'].map(product_source_mapping)

# Converte ItemName para maiúsculas
raw_itens['ItemName'] = raw_itens['ItemName'].str.upper()

# Reordenar as colunas
ordered_columns = [
    'ItemCode', 'ItemName', 'ForeignName', 'ItemsGroupCode', 'ItemType', 'PurchaseItem', 
    'SalesItem', 'InventoryItem', 'ShipType', 'ManageSerialNumbers', 'ManageBatchNumbers', 
    'IsPhantom', 'ItemClass', 'MaterialType', 'NCMCode', 'ProductSource', 
    'PurchaseUnit', 'PurchaseItemsPerUnit', 'SalesUnit', 'SalesItemsPerUnit', 
    'InventoryUOM', 'MinInventory', 'MaxInventory', 'PlanningSystem', 
    'ProcurementMethod', 'OrderMultiple', 'MinOrderQuantity', 'LeadTime', 'User_Text'
]

# Reordena as colunas
raw_itens = raw_itens[ordered_columns]

# Salva o arquivo processado
processed_itens_path = 'processed_data/processed_itens.csv'
raw_itens.to_csv(processed_itens_path, index=False)