# parameters
#train_slice = slice('1980', '1999')  # train time range
#holdout_slice = slice('1995', '2014')  # prediction time range
train_slice = slice('1995', '2014')
holdout_slice = slice('1980', '1999')


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
        activity_id ="CMIP", # search for historical and hist-nat (detection and attribution)
        variable_id=var,              # search for precipitation
        table_id="day",               # monthly values
        #experiment_id =[ssp,"historical"]
        experiment_id ="historical",
        member_id ="r1i1p1f1"
    )




    #CMIP.MPI-M.MPI-ESM1-2-HR.historical.day.gn
    ff = [cat.df.iloc[x].activity_id+"."+cat.df.iloc[x].institution_id+"."+cat.df.iloc[x].source_id+"."+cat.df.iloc[x].experiment_id+"." + cat.df.iloc[x].table_id+"."+cat.df.iloc[x].grid_label
          for x in range(cat.df.shape[0])]
    kk = 21
    for mod in ff[21:]:
        if kk == 2:
            kk+=1
            continue
        
        print("start "+mod)
        try:
            ds_model = cat[mod].to_dask().squeeze(drop=True).drop(['height',
                                                                   'lat_bnds',
                                                                   'lon_bnds',
                                                                   'time_bnds'])
        except ValueError:
            ds_model = cat[mod].to_dask().squeeze(drop=True).drop(['lat_bnds',
                                                                   'lon_bnds',
                                                                   'time_bnds'])
#            ds_model = cat[mod].to_dask().squeeze(drop=True)    
        print("finished reading the ds_model---------------")
        print()
        print()
        
              
    ###
    ## regional subsets, ready for downscaling
        train_subset = ds_model[var].sel(time=train_slice).interp_like(obs_subset.isel(time=0,
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
        holdout_subset = ds_model[var].sel(time=holdout_slice).interp_like(obs_subset.isel(time=0,
                                                                                           drop=True), method='linear')
        try:
            holdout_subset['time'] = pd.to_datetime(holdout_subset.indexes['time'])
        except TypeError:
            holdout_subset['time'] = holdout_subset.indexes['time'].to_datetimeindex()
        holdout_subset = holdout_subset.resample(time='1d').mean().load(scheduler='threads').chunk(chunks)
        

        print("finished test_subset--------------------------")
        print()
        
        print('start tarining '+mod)
        print()
        
        ##########################################################
        #               Train the model
        ##########################################################
        model = PointWiseDownscaler(BcsdTemperature(return_anoms=False))
   #    model
        train_subset = train_subset.fillna(-300)
        obs_subset = obs_subset.fillna(-300)
        model.fit(train_subset, obs_subset_new)  
        print('start predicting '+mod)
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
        predicted.to_netcdf('predicted_'+mod+'_'+str(int(holdout_slice.start))+'_'+str(int(holdout_slice.stop))+'.nc')
        Client.restart(client)
#        client = Client()
        kk +=1 
