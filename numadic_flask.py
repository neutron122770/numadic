from flask import Flask, request, make_response
app = Flask(__name__)
@app.route('/hello/', methods=['GET', 'POST'])
def welcome():
    lat = float(request.args.get("lat"))
    lon = float(request.args.get("lon"))
    
    import argparse
    import pandas as pd
    import zipfile
    import numpy as np
    import io


    # read Trip-Info.csv
    df = pd.read_csv("C:/Users/Rakesh/Downloads/test3.csv")

    #get number of trips by vehicle and transporter
    df_grouped = df.groupby(['vehicle_number','transporter_name'])['trip_id'].nunique().reset_index()

    #define haversine
    from math import radians, cos, sin, asin, sqrt
    def haversine(lon1, lat1, lon2, lat2):
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        r = 6371 # Radius of earth in kilometers. Use 3956 for miles
        return c * r


    db = pd.DataFrame()
    with zipfile.ZipFile('C:/Users/Rakesh/Downloads/test_zip.zip') as zip:
        for i in df.vehicle_number.unique():  
            with zip.open('test%s.csv'%(i)) as myZip: #assumption:zipped csv files are stored in some way where vehicle number is present in file name
                df1 = pd.read_csv(myZip)  

                df2 = df1.groupby(['lic_plate_no']).agg({'tis' : [np.min, np. max] , 'osf' : np.sum, 'spd' : np.mean}).reset_index() #calculate KPIs for one vechile number
                df2.columns = ['lic_plate_no','min_ts','max_ts','no_osf','avg_speed']

                df3 = pd.merge(df1, df2, how = 'inner', left_on= ['lic_plate_no','tis'], right_on=['lic_plate_no','min_ts'])  #get start lat lon by joining with min starttime for a vehicle
                df3 = df3[['lic_plate_no','lat','lon','max_ts','no_osf',  'avg_speed']]
                df3.columns = ['lic_plate_no','lat1','lon1', 'max_ts','no_osf','avg_speed']
                df4 = pd.merge(df1, df3, how = 'inner', left_on= ['lic_plate_no','tis'], right_on=['lic_plate_no','max_ts'])  #get end lat lom by joining with max starttime for a vehicle

                df4['distance'] = haversine(df4.lon, df4.lat, df4.lon1, df4.lat1) # calculate distance between start and end points for a vehicle
                df4 = df4[['lic_plate_no','distance','no_osf',  'avg_speed']]
                df5 = df4.merge(df_grouped, left_on='lic_plate_no', right_on='vehicle_number') # join with vehicle details file
                df5.columns = ['lic_plate_no','distance','no_osf',  'avg_speed','vehicle_number', 'transporter_name', 'no_trips']
                df5 = df5[['lic_plate_no','distance','no_trips',   'avg_speed', 'transporter_name','no_osf']] #get required columns from join 
                db = db.append(df5) #append all individual vehicle number KPIs
 
    #adding excel to buffer
    buffer = io.BytesIO()
    db.to_excel(buffer)
    #make response object
    resp = make_response(buffer.getvalue())
    resp.headers["Content-Disposition"] = "attachment; filename=export.xlsx"
    resp.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    #resp is desired excel
    return resp
 

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)