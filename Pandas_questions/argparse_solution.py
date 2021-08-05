import argparse
import pandas as pd 

ap = argparse.ArgumentParser()
ap.add_argument("--min-date", type= str, required=True)
ap.add_argument("--max-date", type= str, required=True)
ap.add_argument("--top", type=int,required=True)
args = vars(ap.parse_args())

df1 = pd.read_csv("product.txt")
df1.columns = ['id_p','name_p','brand_p']
df2 = pd.read_csv('sales.txt')
df3 = pd.read_csv('store.txt')

df_new = df2.merge(df3, left_on='store', right_on='id')
df_new = df_new.merge(df1,left_on='product', right_on= 'id_p')
min_date= args['min_date']
max_date= args['max_date']
top_rows = args['top']

if top_rows<3:
    top_rows = 3


df_groupby = df_new.groupby('date',as_index=False).max()
df_filtered = df_groupby[(df_groupby['date']>=min_date) & (df_groupby['date']<=max_date)]
df_top_seller_product = df_filtered[['name_p','quantity']]
df_top_seller_product.columns=['name','quantity']
df_top_seller_store = df_filtered[['name','quantity']]
df_top_seller_brand = df_filtered[['brand_p','quantity']]
df_top_seller_brand.columns=['brand','quantity']
df_top_seller_city = df_filtered[['city', 'quantity']]

print('\n')
print('-- top seller product --')
print(df_top_seller_product.head(top_rows))
print('\n')
print('-- top seller store --')
print(df_top_seller_store.head(top_rows))
print('\n')
print('-- top seller brand --')
print(df_top_seller_brand.head(top_rows))
print('\n')
print('-- top seller city --')
print(df_top_seller_city.head(top_rows))