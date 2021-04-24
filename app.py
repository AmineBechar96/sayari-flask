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
    # # Extraire la Date des 10 derniers jours

    ten_days = datetime.today() - timedelta(10)

    # # Connection à la base de donnés Mongo

    client = pymongo.MongoClient(
        "mongodb+srv://amine:testtest@cluster0.qlxh6.mongodb.net/test2?retryWrites=true&w=majority")
    db = client.test2
    collection = db['Ouedkniss-today']
    collection_sell = db['Ouedkniss-today1']
    collection_entropot = db['Ouedkniss-today2']

    # # Lire la base de donnés comme DataFrame

    file = pd.DataFrame(list(collection.find()))
    file.drop('_id', axis=1, inplace=True)

    # # Lire les fichiers sell (voiture vendu) et entrpot (tout les voitures)

    sell = pd.read_json("sell.json")
    sell.set_index('id', inplace=True)
    entopot = pd.read_json("entropot.json")
    entopot.set_index('id', inplace=True)

    # # Définir l'index

    file = file[~file.loc[:, 'id'].isna()]
    file.loc[:, 'id'] = file.id.astype('int')
    file.set_index('id', inplace=True)

    # # Définir la date et les voitures affichés dans les 10 derniers jours

    file.date_annonce = file.date_annonce.str.split().str.get(0)
    file.date_annonce = pd.to_datetime(file.date_annonce, dayfirst=True)
    file = file[file.date_annonce > ten_days]

    sell2 = sell.reset_index().merge(file.reset_index().drop_duplicates(), on=['id'],
                                     how='left', indicator=True)
    sell2 = sell2[sell2._merge == 'left_only'].iloc[:, 0:18]
    sell2.set_index('id', inplace=True)
    sell2.columns = sell.columns

    # # Split Model en trois column

    file.model = file.model.str.split().str.get(0)
    file.proDate = file.proDate.str.split().str.get(-1)
    file.brand = file.brand.str.split().str.get(1)
    file.notes = file.notes.str.split(n=3).str[2:-1].str.join(sep=' ')
    file.loc[:, 'proDate'] = file.loc[:, 'proDate'].astype('int')

    # # Split Moteur en trois column

    file.loc[:, 'ch'] = file.moteur.str.split().str.get(-1)
    file.loc[:, 'tdi'] = file.moteur.str.split().str.get(1)
    file.loc[:, 'litre'] = file.moteur.str.split().str.get(0)
    file.loc[:, 'notes_moteur'] = file.moteur.str.split(n=3).str[2:-1].str.join(sep=' ')
    file.drop(columns='moteur', inplace=True)

    # # Traiter le litrage du moteur exp 1.6

    for ch in ['\\', '´', '`', '*', '_', '{', '}', '[', ']', '(', ')', '>', '#', '+', '-', '.', '!', '$', '\'']:
        file.loc[:, 'litre'] = file.loc[:, 'litre'].str.replace(ch, '.', regex=False)

    file.loc[:, 'litre'] = file.loc[:, 'litre'].str.extract(r'(\d\W\d)').iloc[:, 0]
    file.loc[:, 'litre'] = file.litre.astype('float')

    # # Traiter le prix

    file.loc[:, 'price'] = file.loc[:, 'price'].str.extract(r'(\d\d\d*)').iloc[:, 0]
    file = file[~file.price.isna()]
    file.loc[:, 'price'] = file.loc[:, 'price'].astype('int')
    file.loc[(file.price > 600) & (file.litre < 1.6), 'price'] = (
            file[(file.price > 600) & (file.litre < 1.6)].price / 10).astype('int')

    # # Traiter le ch

    file.loc[:, 'ch'] = file.loc[:, 'ch'].str.extract(r'(\d\d\d*)').iloc[:, 0]
    file.loc[:, 'ch'] = file.loc[:, 'ch'].fillna(170)
    file.loc[:, 'ch'] = file.loc[:, 'ch'].astype('int')

    # # Traiter le kilométrage

    file.loc[:, 'kilometrage'] = file.loc[:, 'kilometrage'].str.replace('  km', '')
    file.loc[:, 'kilometrage'] = file.loc[:, 'kilometrage'].astype('int')
    file.loc[:, 'kilometrage'] = file.loc[:, 'kilometrage'].apply(fun, i2='a')
    file.loc[((file.loc[:, 'proDate'] > 2010) & (file.loc[:, 'kilometrage'] > 600)), 'kilometrage'] = file.loc[
        ((file.loc[:, 'proDate'] > 2010) & (file.loc[:, 'kilometrage'] > 600)), 'kilometrage'].apply(fun, i2='b')
    file.loc[((file.loc[:, 'proDate'] > 2020) & (file.loc[:, 'kilometrage'] < 1000)), 'kilometrage'] = file.loc[
        ((file.loc[:, 'proDate'] > 2020) & (file.loc[:, 'kilometrage'] < 1000)), 'kilometrage'].apply(fun, i2="c")

    df_all = entopot.merge(file.reset_index().drop_duplicates(), on=['id'],
                           how='left', indicator=True)

    sell = df_all[df_all.loc[:, '_merge'] == 'left_only'].iloc[:, 0:18]
    sell.set_index('id', inplace=True)
    sell.columns = entopot.columns

    df_all2 = entopot.reset_index().merge(file.reset_index().drop_duplicates(), on=['id'],
                                          how='right', indicator=True)

    new_cars = df_all[df_all.loc[:, '_merge'] == 'right_only'].iloc[:, np.r_[0, 18:35]]
    new_cars.set_index('id', inplace=True)
    new_cars.columns = entopot.columns

    entropot = pd.concat([entopot, new_cars])
    entropot.reset_index(inplace=True)
    entropot.drop_duplicates('id', inplace=True)
    # entropot.to_json("entropot.json")
    data_dict = entropot.to_dict("records")
    # Insert collection
    collection_entropot.insert_many(data_dict)

    selll = pd.concat([sell, sell2])
    selll.reset_index(inplace=True)
    selll.drop_duplicates('id', inplace=True)  # .to_json("sell.json")
    data_dict = selll.to_dict("records")
    # Insert collection
    collection_sell.insert_many(data_dict)
    return 'Hello World!'




if __name__ == '__main__':
    app.run()
