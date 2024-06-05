import pandas as pd
import os
import datetime
import warnings
import numpy as np

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# Mapeamento pasta e importação do arquivo
origem = r'C://projetos//bb_prima//processar//'
destino = r'C://projetos//bb_prima//processado//'

if not os.path.exists(origem):
    os.makedirs(origem)
if not os.path.exists(destino):
    os.makedirs(destino)

dt_atual = datetime.datetime.now()
arquivos = os.listdir(origem)
auto_ep = []
demais_p = []

for arquivo in arquivos:
    lista_arq = os.path.join(origem, arquivo)
    df = pd.read_excel(lista_arq)

    if 'AUTO_EP' in arquivo:
        auto_ep.append(df)
        nm_auto_ep = arquivo
    elif 'TB_REGUA_1PARCELA-AUTO' in arquivo:
        demais_p.append(df)
        nm_demais_p = arquivo

df_auto_ep = pd.concat(auto_ep, ignore_index=True)
df_demais_p = pd.concat(demais_p, ignore_index=True)

# Inicio tratar base
df_demais_p['CPF'] = df_demais_p['CPF'].astype(str)
df_demais_p['APOLICE'] = df_demais_p['APOLICE'].astype(str)
df_demais_p['VALOR'] = df_demais_p['VALOR'].astype(str)
df_demais_p['LINHA_DIGITAVEL'] = df_demais_p['LINHA_DIGITAVEL'].astype(str).replace('nan', '')
df_demais_p['ATRASO'] = (dt_atual - df_demais_p['VENCIMENTO']).dt.days
df_demais_p['VENCIMENTO'] = df_demais_p['VENCIMENTO'].dt.strftime('%#d/%#m/%Y')
df_demais_p['PRODUTO'] = 'AUTO'


def tratar_celular(cel):
    try:
        return int(cel)
    except (ValueError, TypeError):
        return 0


df_demais_p['CEL SEGURADO2'] = df_demais_p['CEL SEGURADO'].apply(tratar_celular).fillna(0).astype('Int64').astype(str)
df_demais_p['CEL SEGURADO2'] = df_demais_p['CEL SEGURADO2'].replace('0', '')
df_demais_p['NUM_CARACT'] = df_demais_p.apply(lambda x: len(x['CEL SEGURADO2']), axis=1)
# Fim tratar base

#Mapear colunas
mapear_boleto = {'CPF': 'ID',
                 'CEL SEGURADO2': 'TELEFONE',
                 'NOME SEGURADO': 'NOME',
                 'APOLICE': 'ident_proposta_apolice',
                 'VALOR': 'VALOR',
                 'VENCIMENTO': 'DATA VENCIMENTO',
                 'LINHA_DIGITAVEL': 'CODIGO',
                 'PRODUTO': 'PRODUTO'
                 }

mapear_debito = {'CPF': 'ID',
                 'CEL SEGURADO2': 'TELEFONE',
                 'NOME SEGURADO': 'NOME',
                 'APOLICE': 'ident_proposta_apolice',
                 'VALOR': 'VALOR',
                 'VENCIMENTO': 'DATA VENCIMENTO',
                 'PRODUTO': 'PRODUTO'
                 }

##### INICIO FILTRO
# BOLETO A VENCER
ba_a_vencer = df_demais_p[(df_demais_p['NUM_CARACT'] == 13)
                          & (df_demais_p['FORMA_DE_PAGAMENTO'] == 'BA')
                          & (df_demais_p['ATRASO'] == -2)
                          & (df_demais_p['LINHA_DIGITAVEL'] != '')
                          ]

# BOLETO VENCIDO
ba_vencido = df_demais_p[(df_demais_p['NUM_CARACT'] == 13)
                         & (df_demais_p['FORMA_DE_PAGAMENTO'] == 'BA')
                         & (df_demais_p['ATRASO'] == 3)
                         & (df_demais_p['LINHA_DIGITAVEL'] != '')
                         ]
# DEBITO A VENCER
db_a_vencer = df_demais_p[(df_demais_p['NUM_CARACT'] == 13)
                          & ((df_demais_p['FORMA_DE_PAGAMENTO'] == 'DB') | (df_demais_p['FORMA_DE_PAGAMENTO'] == 'DC'))
                          & (df_demais_p['ATRASO'] == -2)
                          ]

# DEBITO VENCIDO
db_vencido = df_demais_p[(df_demais_p['NUM_CARACT'] == 13)
                         & ((df_demais_p['FORMA_DE_PAGAMENTO'] == 'DB') | (df_demais_p['FORMA_DE_PAGAMENTO'] == 'DC'))
                         & (df_demais_p['ATRASO'] == 3)
                         ]
##### FIM FILTRO

df_higienizado = pd.concat([ba_a_vencer, ba_vencido, db_a_vencer, db_vencido], axis=0)
df_higienizado.to_excel(f'{destino}BASE_HIGIENIZADA_{nm_demais_p.split("_")[3]}', index=False)

##### INICIO EXPORTAÇÃO
# BOLETO A VENCER
ba_a_vencer = ba_a_vencer.rename(columns=mapear_boleto)[list(mapear_boleto.values())]
ba_a_vencer.to_excel(f'{destino}1_parcela_Auto_BA_A_VENCER_{nm_demais_p.split("_")[3]}', index=False)

# BOLETO VENCIDO
ba_vencido = ba_vencido.rename(columns=mapear_boleto)[list(mapear_boleto.values())]
ba_vencido.to_excel(f'{destino}1_parcela_Auto_BA_VENCIDO_{nm_demais_p.split("_")[3]}', index=False)

# DEBITO A VENCER
db_a_vencer = db_a_vencer.rename(columns=mapear_debito)[list(mapear_debito.values())]
db_a_vencer.to_excel(f'{destino}1_parcela_Auto_DC_DB_A_VENCER_{nm_demais_p.split("_")[3]}', index=False)

# DEBITO VENCIDO
db_vencido = db_vencido.rename(columns=mapear_debito)[list(mapear_debito.values())]
db_vencido.to_excel(f'{destino}1_parcela_Auto_DC_DB_VENCIDO_{nm_demais_p.split("_")[3]}', index=False)
##### FIM EXPORTAÇÃO
print('Extração FINALIZADA!')
