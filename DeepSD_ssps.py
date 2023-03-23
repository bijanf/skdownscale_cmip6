# parameters
train_slice = slice('1985', '2014')  # train time range
holdout_slice = slice('2070', '2099')  # prediction time range
#train_slice = slice('1995', '2014')
#holdout_slice = slice('1980', '1999')


# bounding box of downscaling region
lat0=38.80
lat1=39.80
lon0=67.4
lon1=70.8
var="tasmin"
lon_slice = slice(lon0, lon1) #90
lat_slice = slice(lat0, lat1) #60

# chunk shape for dask execution (time must be contiguous, ie -1)
chunks = {'lat': 10, 'lon': 10, 'time': -1}


if __name__ == '__main__':
    

    # load the Dask CLuster
    from dask.distributed import Client

    client = Client()
    print('client--------------------- ',client)
    from skdownscale.pointwise_models import BcsdTemperature, BcsdPrecipitation
    from skdownscale.pointwise_models import PointWiseDownscaler
    from dask.diagnostics import ProgressBar

    from skdownscale.pointwise_models import AnalogRegression, PureAnalog




    import xarray as xr

    fnames = [f'../outputs/chelsa_CA_{year}.nc'
              for year in range(int(train_slice.start), int(train_slice.stop) + 1)]
    # open the data and cleanup a bit of metadata
    obs = xr.open_mfdataset(fnames, engine='netcdf4', concat_dim='time',combine='nested')
    obs_subset = obs[var].sel(time=train_slice, lon=lon_slice, lat=lat_slice).resample(time='1d').mean().load(scheduler='threads').chunk(chunks)

    print("Finished reading the observations-------------------------------")

    import intake_esm
    import intake
    import pandas as pd


    ssp="ssp585"
    # collect the data from api

    url = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
    col = intake.open_esm_datastore(url)
    cat = col.search(
        activity_id =["CMIP","ScenarioMIP"], # search for historical and hist-nat (detection and attribution)
        variable_id=var,              # search for precipitation
        table_id="day",               # monthly values
        experiment_id =[ssp,"historical"],
        #experiment_id ="historical",
        member_id ="r1i1p1f1"
    )




    #CMIP.MPI-M.MPI-ESM1-2-HR.historical.day.gn
    ffhist = ["CMIP."+cat.df.iloc[x].institution_id+"."+cat.df.iloc[x].source_id+".historical." + cat.df.iloc[x].table_id+"."+cat.df.iloc[x].grid_label  for x in range(cat.df.shape[0])]
    hhssp  = ["ScenarioMIP."+cat.df.iloc[x].institution_id+"."+cat.df.iloc[x].source_id+"."+ssp+"."+cat.df.iloc[x].table_id+"."+cat.df.iloc[x].grid_label  for x in range(cat.df.shape[0])]
    print(ffhist)
    print()
    print(hhssp)

#    ff = ["CMIP.NOAA-GFDL.GFDL-CM4.historical.day.gr2","CMIP.NOAA-GFDL.GFDL-CM4.historical.day.gr1",
#          "CMIP.NASA-GISS.GISS-E2-1-G.historical.day.gn","CMIP.AWI.AWI-CM-1-1-MR.historical.day.gn",
#          "CMIP.BCC.BCC-ESM1.historical.day.gn","CMIP.SNU.SAM0-UNICON.historical.day.gn",
#          "CMIP.CCCma.CanESM5.historical.day.gn","CMIP.INM.INM-CM4-8.historical.day.gr1",
#          "CMIP.MRI.MRI-ESM2-0.historical.day.gn","CMIP.INM.INM-CM5-0.historical.day.gr1",
#          "CMIP.HAMMOZ-Consortium.MPI-ESM-1-2-HAM.historical.day.gn","CMIP.MPI-M.MPI-ESM1-2-LR.historical.day.gn",
#          "CMIP.MPI-M.MPI-ESM1-2-HR.historical.day.gn","CMIP.NOAA-GFDL.GFDL-ESM4.historical.day.gr1",
#          "CMIP.NUIST.NESM3.historical.day.gn","CMIP.NCC.NorESM2-LM.historical.day.gn",
#          "CMIP.CAS.FGOALS-g3.historical.day.gn","CMIP.MIROC.MIROC6.historical.day.gn",
#          "CMIP.CAS.FGOALS-f3-L.historical.day.gr","CMIP.CSIRO-ARCCSS.ACCESS-CM2.historical.day.gn",
#          "CMIP.NCC.NorESM2-MM.historical.day.gn","CMIP.CSIRO.ACCESS-ESM1-5.historical.day.gn",
#          "CMIP.KIOST.KIOST-ESM.historical.day.gr1","CMIP.AWI.AWI-ESM-1-1-LR.historical.day.gn",
#          "CMIP.EC-Earth-Consortium.EC-Earth3-Veg-LR.historical.day.gr","CMIP.EC-Earth-Consortium.EC-Earth3.historical.day.gr"]
    kk = 23 #4,5,6,8,9,10,18,21,22
    for mod_hist, mod_ssp in zip(ffhist[kk:],hhssp[kk:]):
        print('kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk                         ',kk)
        
        #if kk == 2:
        #    kk+=1
        #    continue
        
        print("start "+mod_hist," ",mod_ssp)
        try:
            ds_model_hist = cat[mod_hist].to_dask().squeeze(drop=True).drop(['height',
                                                                            'lat_bnds',
                                                                            'lon_bnds',
                                                                            'time_bnds'])
        except ValueError:
            
            ds_model_hist = cat[mod_hist].to_dask().squeeze(drop=True).drop(['lat_bnds',
                                                                       'lon_bnds',
                                                                       'time_bnds'])


        try:
            ds_model_ssp = cat[mod_ssp].to_dask().squeeze(drop=True).drop(['height',
                                                                           'lat_bnds',
                                                                           'lon_bnds',
                                                                           'time_bnds'])
        except ValueError:
            ds_model_ssp = cat[mod_ssp].to_dask().squeeze(drop=True).drop(['lat_bnds',
                                                                       'lon_bnds',
                                                                       'time_bnds'])
          
          

#        ds_model = cat[mod].to_dask().squeeze(drop=True).drop(['lat_bnds',
#                                                               'lon_bnds',
#                                                               'time_bnds'])
        


#            ds_model = cat[mod].to_dask().squeeze(drop=True)    
        print("finished reading the ds_model---------------")
        print()
        print()
        
              
    ###
    ## regional subsets, ready for downscaling
        train_subset = ds_model_hist[var].sel(time=train_slice).interp_like(obs_subset.isel(time=0,
                                                                                       drop=True), method='linear')
    
        try:
            train_subset['time'] = pd.to_datetime(train_subset.indexes['time'])
        except TypeError:
            train_subset['time'] = train_subset.indexes['time'].to_datetimeindex()
    
        print("finished train_subset--------------------------")
    
        train_subset = train_subset.resample(time='1d').mean().load(scheduler='threads').chunk(chunks)
        train_subset = train_subset.dropna(dim="time", how="any")
        obs_subset_new = obs_subset.sel(time=train_subset.time)
        ######################################################
        ########################################################
        holdout_subset = ds_model_ssp[var].sel(time=holdout_slice).interp_like(obs_subset.isel(time=0,
                                                                                           drop=True), method='linear')
        try:
            holdout_subset['time'] = pd.to_datetime(holdout_subset.indexes['time'])
        except TypeError:
            holdout_subset['time'] = holdout_subset.indexes['time'].to_datetimeindex()
        holdout_subset = holdout_subset.resample(time='1d').mean().load(scheduler='threads').chunk(chunks)
        

        print("finished test_subset--------------------------")
        print()
        
        print('start tarining '+mod_ssp)
        print()
        
        ##########################################################
        #               Train the model
        ##########################################################
        model = PointWiseDownscaler(BcsdTemperature(return_anoms=False))
   #    model
        train_subset = train_subset.fillna(-300)
        obs_subset = obs_subset.fillna(-300)
        model.fit(train_subset, obs_subset_new)  
        print('start predicting '+mod_ssp)
        print()
        
        ##########################################################
        #               peredict
        ##########################################################
        holdout_subset = holdout_subset.dropna(dim="time", how="any")

        print(holdout_subset.shape)
        print('-------------------')
        print()
        print()
        predicted = model.predict(holdout_subset).load()
        
            
        ##########################################################
        #               Save the results
        ##########################################################
        predicted.to_netcdf('predicted_'+mod_ssp+'_'+str(int(holdout_slice.start))+'_'+str(int(holdout_slice.stop))+'.nc')
        Client.restart(client)
#        client = Client()
        kk +=1 
