import pandas as pd

#data cleaning operations
data = pd.read_csv("apartments.csv")

cols = ['id','pets_allowed','category','title','body','amenities','currency','fee','has_photo','price_display','address','cityname','latitude','longitude','source','time']
data.drop(cols,axis=1,inplace=True)
data = data.dropna()
data.info()
data.to_csv('apartmentsclean.csv')
