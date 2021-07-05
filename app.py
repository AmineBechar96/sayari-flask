from flask import Flask
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient


def fun(f, i2):
    if i2 == 'b':
        return f / 10
    elif i2 == 'c':
        return f / 1000
    else:
        g = f / 1000
        if (g > 1):
            return g
        else:
            return f

app = Flask(__name__)
@app.route('/')
def hello_world():
    ten_days = datetime.today() - timedelta(10)

    # # Connection à la base de donnés Mongo

    # In[166]:

    client = pymongo.MongoClient(
        "mongodb+srv://amine:testtest@cluster0.qlxh6.mongodb.net/test2?retryWrites=true&w=majority")
    db = client.test2
    collection = db['Ouedkniss-today']
    collection_sell = db['Sell']
    collection_entropot = db['Entropot']

    # # Lire la base de donnés comme DataFrame

    # In[167]:

    file = pd.DataFrame(list(collection.find({}, {'_id': False})))

    # # Lire les fichiers sell (voiture vendu) et entrpot (tout les voitures)

    # In[168]:

    sell = pd.DataFrame(list(collection_sell.find({}, {'_id': False})))
    sell.set_index('id', inplace=True)
    entopot = pd.DataFrame(list(collection_entropot.find({}, {'_id': False})))
    entopot.set_index('id', inplace=True)

    # # Définir l'index

    # In[169]:

    file = file[~file.loc[:, 'id'].isna()]
    file.loc[:, 'id'] = file.id.astype('int')
    file.set_index('id', inplace=True)
    file[~file.model.isna()]

    # # Définir la date et les voitures affichés dans les 10 derniers jours

    # In[170]:

    file.date_annonce = file.date_annonce.str.split().str.get(0)
    file.date_annonce = pd.to_datetime(file.date_annonce, dayfirst=True)
    file = file[file.date_annonce > ten_days]

    # In[171]:

    sell2 = sell.reset_index().merge(file.reset_index().drop_duplicates(), on=['id'],
                                     how='left', indicator=True)
    sell2 = sell2[sell2._merge == 'left_only'].iloc[:, 0:20]
    sell2.set_index('id', inplace=True)
    sell2.columns = sell.columns

    # # Split Model en trois column

    # In[172]:

    file.model = file.model.str.split().str.get(0)
    file.proDate = file.proDate.str.split().str.get(-1)
    file.brand = file.brand.str.split().str.get(1)
    file.notes = file.notes.str.split(n=3).str[2:-1].str.join(sep=' ')
    file.loc[:, 'proDate'] = file.loc[:, 'proDate'].astype('int')

    # # Split Moteur en trois column

    # In[173]:

    file.loc[:, 'ch'] = file.moteur.str.split().str.get(-1)
    file.loc[:, 'tdi'] = file.moteur.str.split().str.get(1)
    file.loc[:, 'litre'] = file.moteur.str.split().str.get(0)
    file.loc[:, 'notes_moteur'] = file.moteur.str.split(n=3).str[2:-1].str.join(sep=' ')
    file.drop(columns='moteur', inplace=True)

    # # Traiter le litrage du moteur exp 1.6

    # In[174]:

    file.loc[:, 'litre'] = file.loc[:, 'litre'].str.replace('[^\w]', '.', regex=True)

    file.loc[:, 'litre'] = file.loc[:, 'litre'].str.extract(r'(\d\W\d)').iloc[:, 0]
    file.loc[:, 'litre'] = file.litre.astype('float')

    # # Traiter le prix

    # In[175]:

    file.loc[:, 'price'] = file.loc[:, 'price'].str.extract(r'(\d\d\d*)').iloc[:, 0]
    file = file[~file.price.isna()]
    file.loc[:, 'price'] = file.loc[:, 'price'].astype('int')
    file.loc[(file.price > 600) & (file.litre < 1.6), 'price'] = (
                file[(file.price > 600) & (file.litre < 1.6)].price / 10).astype('int')

    # # Traiter le ch

    # In[176]:

    file.loc[:, 'ch'] = file.loc[:, 'ch'].str.extract(r'(\d\d\d*)').iloc[:, 0]
    file.loc[:, 'ch'] = file.loc[:, 'ch'].fillna(170)
    file.loc[:, 'ch'] = file.loc[:, 'ch'].astype('int')

    # # Traiter les Wilayas
    file.loc[:, 'location'] = file.loc[:, 'location'].str.replace('Ain temouchent', 'AinTemouchent')
    file.loc[:, 'location'] = file.loc[:, 'location'].str.replace('Ain defla', 'AinDefla')
    file.loc[:, 'location'] = file.loc[:, 'location'].str.replace('El taref', 'ElTaref')
    file.loc[:, 'location'] = file.loc[:, 'location'].str.replace('El oued', 'ElOued')
    file.loc[:, 'location'] = file.loc[:, 'location'].str.replace('El bayadh', 'ElBayadh')
    file.loc[:, 'location'] = file.loc[:, 'location'].str.strip().str.get(0)

    # # Traiter le kilométrage
    file.loc[:, 'kilometrage'] = file.loc[:, 'kilometrage'].str.replace('  km', '')
    file.loc[:, 'kilometrage'] = file.loc[:, 'kilometrage'].astype('int')
    file.loc[:, 'kilometrage'] = file.loc[:, 'kilometrage'].apply(fun, i2='a')
    file.loc[((file.loc[:, 'proDate'] > 2010) & (file.loc[:, 'kilometrage'] > 600)), 'kilometrage'] = file.loc[
        ((file.loc[:, 'proDate'] > 2010) & (file.loc[:, 'kilometrage'] > 600)), 'kilometrage'].apply(fun, i2='b')
    file.loc[((file.loc[:, 'proDate'] > 2020) & (file.loc[:, 'kilometrage'] < 1000)), 'kilometrage'] = file.loc[
        ((file.loc[:, 'proDate'] > 2020) & (file.loc[:, 'kilometrage'] < 1000)), 'kilometrage'].apply(fun, i2="c")

    # In[179]:

    entopot.date_annonce = pd.to_datetime(entopot.date_annonce, unit='ms')
    df_all = entopot.loc[entopot.date_annonce > ten_days].merge(file.reset_index(), on=['id'],
                                                                how='left', indicator=True)

    # In[180]:

    sell = df_all[df_all.loc[:, '_merge'] == 'left_only'].iloc[:, 0:20]
    sell.set_index('id', inplace=True)
    sell.columns = entopot.columns

    # In[181]:

    df_all2 = entopot.reset_index().merge(file.reset_index().drop_duplicates(), on=['id'],
                                          how='right', indicator=True)

    # In[182]:

    new_cars = df_all2[df_all2.loc[:, '_merge'] == 'right_only'].iloc[:, np.r_[0, 20:39]]
    new_cars.set_index('id', inplace=True)
    new_cars.columns = entopot.columns

    # In[183]:

    entropot = pd.concat([entopot, new_cars])
    entropot.reset_index(inplace=True)
    entropot.drop_duplicates('id', inplace=True)
    collection_entropot.drop()
    data_dict = entropot.drop_duplicates('id').to_dict("records")
    collection_entropot.insert_many(data_dict)

    # In[184]:

    selll = pd.concat([sell, sell2])
    selll.reset_index(inplace=True)
    collection_sell.drop()
    data_dict = selll.drop_duplicates('id').to_dict("records")
    collection_sell.insert_many(data_dict)

    # In[191]:

    return 'Hello World!'




if __name__ == '__main__':
    app.run()
