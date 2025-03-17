from data_ingest import API_injest

import folium

m = folium.Map(location=(45.5236, -122.6750))

m.save("map.html")

""" generator = API_injest.GetAllData();

for data in generator:
    print("ok")

 """