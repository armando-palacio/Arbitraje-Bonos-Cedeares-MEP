import requests
from bs4 import BeautifulSoup
import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('colheader_justify', 'center')
import numpy as np
import yfinance as yf
import argparse

parser = argparse.ArgumentParser(description='Pasamos la ruta del archivo ratios.json')
parser.add_argument('-p', '--path', type=str, help='Ruta del archivo')
args = parser.parse_args()
if args.path:
    path = args.path
else:
    path = 'C:/Users/Teleco/OneDrive/Proyectos/Arbitraje-Bonos-Cedeares-MEP/ratios.json'

def eliminar_caracteres(cadena):
    caracteres_a_eliminar = ['\n', '\r', ' ', '%', '.']
    tabla_de_reemplazo = str.maketrans('', '', ''.join(caracteres_a_eliminar))
    return cadena.translate(tabla_de_reemplazo)

def clasify_number(num):
    if num//10**9>0:
        return str(round(num/10**9, 1))+'B'
    if num//10**6>0:
        return str(round(num/10**6, 1))+'M'
    if num//10**3>0:
        return str(round(num/10**3, 1))+'k'
    else:
        return str(round(num,1))
def clasify_numbers(numbers):
    return np.vectorize(clasify_number)(numbers)

def get_stock_price(symbols):
    def get_sck(s):
        Tk = yf.Ticker(s)
        try:
            p = Tk.history('1d')['Close'].iloc[-1]
        except:
            return np.nan
        return p

    if isinstance(symbols, (list, tuple, np.ndarray)):
        return np.array([get_sck(s) for s in symbols])
    return get_sck(symbols)

def get_table(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise TimeoutError('La página está caida')

    if 'bonos' in url:
        c=-3 # ['Símbolo', 'ÚltimoOperado', 'VariaciónDiaria', 'CantidadCompra', 'PrecioCompra', 'PrecioVenta', 'CantidadVenta', 'Máximo', 'Mínimo', 'ÚltimoCierre', 'MontoOperado']
    elif 'cedears' in url:
        c=-1 # ['Símbolo', 'ÚltimoOperado', 'VariaciónDiaria', 'CantidadCompra', 'PrecioCompra', 'PrecioVenta', 'CantidadVenta', 'Apertura', 'Mínimo', 'Máximo', 'ÚltimoCierre', 'MontoOperado']
    elif 'estados-unidos' in url:
        c=2 # ['Símbolo', 'ÚltimoOperado']
    else:
        raise ValueError('no se reconoce la url proporcionada')

    page = BeautifulSoup(response.content, 'html.parser')
    html_table = page.find(id = 'cotizaciones')
    columnas = [i.text for i in html_table.find('thead').find('tr').find_all('td')[:c]]

    assets = html_table.find('tbody').find_all('tr')

    assets_df = pd.DataFrame(columns=columnas)

    for asset in assets:
        asset = asset.find_all('td')[:c] # valores correspondientes a las columnas ['Símbolo', 'ÚltimoOperado', ...]
        symbol = eliminar_caracteres(asset[0].find('b').text)
        name = eliminar_caracteres(asset[0].find('span').text)
        asset = pd.Series([symbol] + [eliminar_caracteres(td.text).replace(',','.') for td in asset[1:]], columnas)
        asset[asset=='-']=np.nan
        if c!=-3: asset['name'] = name.lower()
        assets_df = assets_df._append(asset, ignore_index=True)

    assets_df['ÚltimoOperado'] = assets_df['ÚltimoOperado'].astype(float)
    if c == 2:
        return assets_df

    assets_df['VariaciónDiaria'] = assets_df['VariaciónDiaria'].astype(float)
    assets_df['CantidadCompra'] = assets_df['CantidadCompra'].astype(float)
    assets_df['PrecioCompra'] = assets_df['PrecioCompra'].astype(float)
    assets_df['PrecioVenta'] = assets_df['PrecioVenta'].astype(float)
    assets_df['CantidadVenta'] = assets_df['CantidadCompra'].astype(float)
    assets_df['Máximo'] = assets_df['Máximo'].astype(float)
    assets_df['Mínimo'] = assets_df['Mínimo'].astype(float)
    assets_df['ÚltimoCierre']= assets_df['ÚltimoCierre'].astype(float)
    assets_df['MontoOperado'] = assets_df['MontoOperado'].astype(float)

    if c == -1:
        assets_df['Apertura'] = assets_df['Apertura'].astype(float)
    return assets_df

bonos = get_table('https://iol.invertironline.com/mercado/cotizaciones/argentina/bonos/soberanos-en-dólares')

# separamos los bonos en pesos y los bonos en dólares
bonos_ARS = bonos[~(bonos['Símbolo'].str.endswith('D') | bonos['Símbolo'].str.endswith('C'))]
bonos_MEP = bonos[bonos['Símbolo'].str.endswith('D')]

# filtramos todos los bonos que tienen cotización en pesos y en dólares a la vez
bonos_ARS = bonos_ARS[(bonos_ARS['Símbolo']+'D').isin(bonos_MEP['Símbolo'])].sort_values(by='Símbolo')
bonos_MEP = bonos_MEP[bonos_MEP['Símbolo'].isin(bonos_ARS['Símbolo']+'D')].sort_values(by='Símbolo')

cols = ['Símbolo', 'USD_MEP', 'Volumen[ARS]', 'Volumen[USD]']
bonos = pd.DataFrame(columns=cols)
bonos['Símbolo'] = bonos_ARS['Símbolo'].values
bonos['USD_MEP'] = (bonos_ARS['ÚltimoOperado'].values/bonos_MEP['ÚltimoOperado'].values).round(2)
bonos['Volumen[ARS]'] = clasify_numbers(bonos_ARS['MontoOperado'].values)
bonos['Volumen[USD]'] = clasify_numbers(bonos_MEP['MontoOperado'].values)
bonos = bonos.sort_values(by='USD_MEP')


cedears = get_table('https://iol.invertironline.com/mercado/cotizaciones/argentina/cedears/todos')

# separamos los bonos en pesos, filtrando por el nombre de la compañía.
cedears_ARS = cedears.drop_duplicates(subset='name')
filt = {'ADGO':'ARGO','AKOB':0,'AOCAD':0,'ARKKETF':0,'AUY':0,'BBV':'BBVA','BNG':'BG','BRKB':0,'CS':0,'DISN':'DIS','GOGLD':0,'MAD':0,'NOKA':'NOK','OGZD':0,'PKS':'PKX','SI':0,'TEFO':'TEF','TEN':'TS','TWTR':0,'TXR':'TX','XROX':'XRX'}

# eliminamos los cedears que no aparecen en yahoo_finance
eliminar = list(filter(lambda clave: filt[clave] == 0, filt.keys()))
cedears_ARS = cedears_ARS[~cedears_ARS['Símbolo'].isin( eliminar )]

cedears_MEP = cedears[(cedears['name'].isin(cedears_ARS['name']) & cedears['Símbolo'].str.endswith('D') & ~cedears['Símbolo'].isin(cedears_ARS['Símbolo']))]
cedears_ARS = cedears_ARS[cedears_ARS['name'].isin(cedears_MEP['name'])]

cedears_ARS = cedears_ARS.sort_values(by='Símbolo')
cedears_MEP = cedears_MEP.sort_values(by='Símbolo', key=lambda x: x.map({v: i for i, v in enumerate((cedears_ARS['Símbolo']+'D').values)}))

cols = ['Símbolo', 'Volumen[ARS]', 'Precio[ARS]', 'Precio[USD]', 'Volumen[USD]', 'USD_MEP', 'Cedear[USD]', 'Acción[USD]','ratio[Ced/Acc-1]%','Compañía']
cedears = pd.DataFrame(columns=cols)

cedears['Símbolo'] = cedears_ARS['Símbolo'].values
cedears['Precio[ARS]'] = cedears_ARS['ÚltimoOperado'].values
cedears['Volumen[ARS]'] = clasify_numbers(cedears_ARS['MontoOperado'].values)
cedears['Precio[USD]'] = cedears_MEP['ÚltimoOperado'].values
cedears['Volumen[USD]'] = clasify_numbers(cedears_MEP['MontoOperado'].values)
cedears['USD_MEP'] = (cedears_ARS['ÚltimoOperado'].values/cedears_MEP['ÚltimoOperado'].values).round(1)

# modificamos los símbolos para que coincidan con yahoo_finance
modificar = list(filter(lambda clave: filt[clave] != 0, filt.keys()))
new_ARS = list(filter(lambda valor: valor != 0, filt.values()))
cedears['Símbolo'] = cedears['Símbolo'].replace(modificar, new_ARS)

ratio = pd.read_json(path, orient='index').sort_index(); ratio.columns = ['ratio']
cedears['Cedear[USD]'] = (ratio['ratio'].values * cedears['Precio[USD]'].values).round(2)
cedears['Acción[USD]'] = get_stock_price(cedears['Símbolo'].values).round(2) # Obtiene los valores de las
cedears['ratio[Ced/Acc-1]%'] = (100 * (cedears['Cedear[USD]'].values/cedears['Acción[USD]'].values - 1)).round(1)
cedears['Compañía'] = cedears_ARS['name'].values
cedears = cedears.sort_values(by='USD_MEP')

print(bonos)
print(cedears)
