#!/usr/bin/env python
# coding: utf-8

# Εισαγωγή των απαραίτητων βιβλιοθηκών
import requests
import pandas as pd
import re
import numpy as np
import json
from datetime import datetime
today = datetime.today().strftime('_%-d_%-m_%Y')

# Ρυθμίσεις για την εμφάνιση δεδομένων στο pandas
pd.set_option('display.max_rows', 2500)     # Ορισμός μέγιστου αριθμού γραμμών που θα εμφανίζονται
pd.set_option('display.max_columns', 500)   # Ορισμός μέγιστου αριθμού στηλών που θα εμφανίζονται

# Ορισμός των headers για το HTTP αίτημα
headers = {
    'sec-ch-ua-platform': '"macOS"',
    'Referer': 'https://e-katanalotis.gov.gr/',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'sec-ch-ua-mobile': '?0',
}

params = {
    'cid': '1730332800000',
}

response = requests.get(
    'https://warply.s3.amazonaws.com/applications/ed840ad545884deeb6c6b699176797ed/basket-retailers/prices.json',
    params=params,
    headers=headers,
)

# Ανάγνωση της απόκρισης ως JSON
data = response.json()

# Ανάκτηση της λίστας των προϊόντων από τα δεδομένα
prod = data['context']['MAPP_PRODUCTS']['result']['products']

# Ανάκτηση της λίστας των εμπόρων από τα δεδομένα
merchants = data['context']['MAPP_PRODUCTS']['result']['merchants']

# Συνάρτηση για την εύρεση του ονόματος του εμπόρου με βάση το uuid
def get_merchants(uuid:int):
    merchant = 'not found'  # Αρχικοποίηση μεταβλητής
    for item in merchants:
        if item['merchant_uuid']==uuid:
            merchant = item['name']
            break
    return merchant

# Ανάκτηση της λίστας των προμηθευτών από τα δεδομένα
suppliers = data['context']['MAPP_PRODUCTS']['result']['suppliers']

# Συνάρτηση για την εύρεση του ονόματος του προμηθευτή με βάση το uuid
def get_supplier(uuid:int):
    supplier = 'not found'  # Αρχικοποίηση μεταβλητής
    for item in suppliers:
        if item['id']==uuid:
            supplier = item['name']
            break
    return supplier

# Ανάκτηση της λίστας των κατηγοριών από τα δεδομένα
categories = data['context']['MAPP_PRODUCTS']['result']['categories']

# Εμφάνιση της 11ης κατηγορίας (για έλεγχο)
categories[10]

# Συνάρτηση για την ανάπτυξη των κατηγοριών και υποκατηγοριών σε λίστα
def category_long(category:dict):
    results=[]
    result={}
    result['name']=category['name']
    result['uuid']=category['uuid']
    results.append(result)
    for item in category['sub_categories']:
        result={}
        result['name']=item['name']
        result['uuid']=item['uuid']
        results.append(result)
        for sub_sub in item['sub_sub_categories']:
            result={}
            result['name']=sub_sub['name']
            result['uuid']=sub_sub['uuid']
            results.append(result)
    return results

# Συνάρτηση για την εύρεση του ονόματος της κατηγορίας με βάση το uuid
def find_category(uuid:int):
    category = 'Not found'
    for item in category_results:
        if item['uuid']==uuid:
            category = item['name']
    return category

# Δημιουργία λίστας με όλα τα αποτελέσματα κατηγοριών
category_results=[]
for item in categories:
    category_results = category_results+category_long(item)

# Συνάρτηση για τη δημιουργία DataFrame από τις κατηγορίες
def category_df(record:dict):
    results = []
    if record['sub_categories']:
        subcategories = record['sub_categories']
        if isinstance(subcategories, list) and len(subcategories)>0:
            for i, item in enumerate(subcategories):
                if item['sub_sub_categories'] and isinstance(item['sub_sub_categories'], list) and len(item['sub_sub_categories'])>0:
                    for n, sub_sub_item in enumerate(item['sub_sub_categories']):
                        result = {}
                        result['name'] = record['name']
                        result['uuid'] = record['uuid']
                        result[f"sub_name"]=item['name']
                        result[f"sub_uuid"]=item['uuid']
                        result[f"sub_sub_name"]=sub_sub_item['name']
                        result[f"sub_sub_uuid"]=sub_sub_item['uuid']
                        results.append(result)
    return results

# Παράδειγμα εκτέλεσης της συνάρτησης (σχολιασμένο)
# record(categories[1])

# Συγκέντρωση όλων των αποτελεσμάτων σε μία λίστα
results = []
for item in categories:
    results=results+category_df(item)

# Δημιουργία DataFrame από τα αποτελέσματα των κατηγοριών
cat = pd.DataFrame(results)



# Συνάρτηση για την επεξεργασία των προϊόντων και τη δημιουργία λίστας λεξικών με τα δεδομένα
def product(record:dict):
    results=[]
    fields = json.loads(record['extra_fields'])  # Μετατροπή των 'extra_fields' από JSON string σε dict
    for item in record['prices']:
        result ={}
        result['product_id']=record['barcode']
        result['name'] = record['name']
        result['date'] = fields['date']
        result['unit'] = fields['unit']
        if isinstance(record['category'], list):
            categories = ', '.join([find_category(i) for i in record['category']])
            result['category_name'] = categories
        result['category_codes'] = record['category']
        result['monimi_meiosi'] = record['monimi_meiosi']
        result['promo'] = record['promo']
        result['supplier_name'] = get_supplier(record['supplier'])
        result['supplier_code'] = record['supplier']
        result['merchant'] = get_merchants(int(item['merchant_uuid']))
        result['price'] = item['price']
        results.append(result)
    return results

# Συγκέντρωση όλων των αποτελεσμάτων προϊόντων σε μία λίστα
results_products = []
for item in prod:
    results_products= results_products + product(item)

# Δημιουργία DataFrame από τα αποτελέσματα των προϊόντων
df = pd.DataFrame(results_products)
df.to_csv(f"data{today}.csv", index=False)

response_3 = requests.get(
    'https://warply.s3.amazonaws.com/applications/ed840ad545884deeb6c6b699176797ed/basket-retailers/freshbasket.json?v=1730710976905',
    headers={
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9,el;q=0.8',
        'Connection': 'keep-alive',
        'Origin': 'https://e-katanalotis.gov.gr',
        'Referer': 'https://e-katanalotis.gov.gr/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }
)

data = response_3.json()
retailers = data['retailers'].keys()

from_date_object = datetime.strptime(data['from'], '%d-%m-%Y')
to_date_object = datetime.strptime(data['to'], '%d-%m-%Y')
filename = f"fresh_basket_{str(from_date_object.day)}_{str(from_date_object.month)}_to_{str(to_date_object.day)}_{str(to_date_object.month)}_{str(to_date_object.year)}.csv"

fresh_basket=[]
for retail in retailers:
    for i in data['retailers'][retail]['basket']:
        i['retailer'] = retail
        i['from'] = data['from']
        i['to'] = data['to']
        fresh_basket.append(i)

pd.DataFrame(fresh_basket).to_csv(filename, index=False)