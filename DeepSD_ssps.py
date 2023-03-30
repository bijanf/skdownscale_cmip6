# parameters
train_slice = slice('1985', '2014')  # train time range
holdout_slice = slice('2015', '2044')  # prediction time range
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


    ssp="ssp126"
    
    # collect the data from api

    url = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
    col = intake.open_esm_datastore(url)
    cat = col.search(
        activity_id =["CMIP","ScenarioMIP"], # search for historical and hist-nat (detection and attribution)
        variable_id=var,              # search for precipitation
        table_id="day",               # monthly values
#        experiment_id =[ssp,"historical"],
        experiment_id =ssp,  
        #experiment_id ="historical",
        member_id ="r1i1p1f1"
    )




    #CMIP.MPI-M.MPI-ESM1-2-HR.historical.day.gn
    ffhist = ["CMIP."+cat.df.iloc[x].institution_id+"."+cat.df.iloc[x].source_id+".historical." + cat.df.iloc[x].table_id+"."+cat.df.iloc[x].grid_label  for x in range(cat.df.shape[0])]
    hhssp  = ["ScenarioMIP."+cat.df.iloc[x].institution_id+"."+cat.df.iloc[x].source_id+"."+ssp+"."+cat.df.iloc[x].table_id+"."+cat.df.iloc[x].grid_label  for x in range(cat.df.shape[0])]


    if ssp == "ssp126":
        hhssp=["ScenarioMIP.AWI.AWI-CM-1-1-MR.ssp126.day.gn",
               "ScenarioMIP.CAS.FGOALS-g3.ssp126.day.gn",
               "ScenarioMIP.CCCma.CanESM5.ssp126.day.gn",
               "ScenarioMIP.CMCC.CMCC-ESM2.ssp126.day.gn",
               "ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp126.day.gr",
               "ScenarioMIP.EC-Earth-Consortium.EC-Earth3-Veg-LR.ssp126.day.gr",
               "ScenarioMIP.INM.INM-CM4-8.ssp126.day.gr1",
               "ScenarioMIP.INM.INM-CM5-0.ssp126.day.gr1",
               "ScenarioMIP.KIOST.KIOST-ESM.ssp126.day.gr1",
               "ScenarioMIP.MIROC.MIROC6.ssp126.day.gn",
               "ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp126.day.gn",
               "ScenarioMIP.MRI.MRI-ESM2-0.ssp126.day.gn",
               "ScenarioMIP.NCC.NorESM2-MM.ssp126.day.gn",
               "ScenarioMIP.NOAA-GFDL.GFDL-ESM4.ssp126.day.gr1",
               "ScenarioMIP.NUIST.NESM3.ssp126.day.gn"]
        
        ffhist= [ ff.replace("ScenarioMIP","CMIP") for ff in  hhssp ]
        ffhist=[ ff.replace("ssp126","historical") for ff in  ffhist]


        

    if ssp == "ssp245":
        hssp=["ScenarioMIP.AS-RCEC.TaiESM1.ssp245.day.gn",
              "ScenarioMIP.AWI.AWI-CM-1-1-MR.ssp245.day.gn",
              "ScenarioMIP.CAS.FGOALS-g3.ssp245.day.gn",
              "ScenarioMIP.CCCma.CanESM5.ssp245.day.gn",
              "ScenarioMIP.CMCC.CMCC-ESM2.ssp245.day.gn",
              "ScenarioMIP.CSIRO-ARCCSS.ACCESS-CM2.ssp245.day.gn",
              "ScenarioMIP.EC-Earth-Consortium.EC-Earth3-CC.ssp245.day.gr",
              "ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp245.day.gr",
              "ScenarioMIP.EC-Earth-Consortium.EC-Earth3-Veg-LR.ssp245.day.gr",
              "ScenarioMIP.INM.INM-CM4-8.ssp245.day.gr1",
              "ScenarioMIP.INM.INM-CM5-0.ssp245.day.gr1",
              "ScenarioMIP.KIOST.KIOST-ESM.ssp245.day.gr1",
              "ScenarioMIP.MIROC.MIROC6.ssp245.day.gn",
              "ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp245.day.gn",
              "ScenarioMIP.MRI.MRI-ESM2-0.ssp245.day.gn",
              "ScenarioMIP.NCC.NorESM2-LM.ssp245.day.gn",
              "ScenarioMIP.NCC.NorESM2-MM.ssp245.day.gn",
              "ScenarioMIP.NOAA-GFDL.GFDL-CM4.ssp245.day.gr1",
              "ScenarioMIP.NOAA-GFDL.GFDL-CM4.ssp245.day.gr2",
              "ScenarioMIP.NOAA-GFDL.GFDL-ESM4.ssp245.day.gr1",
              "ScenarioMIP.NUIST.NESM3.ssp245.day.gn"]
        
        ffhist=[ff.replace("ScenarioMIP","CMIP") for ff in  hhssp ]
        ffhist=[ff.replace("ssp245","historical") for ff in  ffhist]
      


    if ssp == "ssp370":
        hhssp=["ScenarioMIP.AWI.AWI-CM-1-1-MR.ssp370.day.gn",
               "ScenarioMIP.CAS.FGOALS-g3.ssp370.day.gn",
               "ScenarioMIP.CCCma.CanESM5.ssp370.day.gn",
               "ScenarioMIP.CMCC.CMCC-ESM2.ssp370.day.gn",
               "ScenarioMIP.EC-Earth-Consortium.EC-Earth3-AerChem.ssp370.day.gr",
               "ScenarioMIP.EC-Earth-Consortium.EC-Earth3.ssp370.day.gr_2",
               "ScenarioMIP.EC-Earth-Consortium.EC-Earth3-Veg-LR.ssp370.day.gr",
               "ScenarioMIP.INM.INM-CM4-8.ssp370.day.gr1",
               "ScenarioMIP.INM.INM-CM5-0.ssp370.day.gr1",
               "ScenarioMIP.MIROC.MIROC6.ssp370.day.gn",
               "ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp370.day.gn",
               "ScenarioMIP.MRI.MRI-ESM2-0.ssp370.day.gn",
               "ScenarioMIP.NCC.NorESM2-LM.ssp370.day.gn",
               "ScenarioMIP.NCC.NorESM2-MM.ssp370.day.gn",
               "ScenarioMIP.NOAA-GFDL.GFDL-ESM4.ssp370.day.gr1"]
        ffhist= [ ff.replace("ScenarioMIP","CMIP") for ff in  hhssp ]
        ffhist=[ ff.replace("ssp370","historical") for ff in  ffhist]
        

    if ssp == "ssp585":
        hhssp = ["ScenarioMIP.AWI.AWI-CM-1-1-MR.ssp585.day.gn",
                 "ScenarioMIP.CAS.FGOALS-g3.ssp585.day.gn",
                 "ScenarioMIP.CCCma.CanESM5.ssp585.day.gn",
                 "ScenarioMIP.CSIRO.ACCESS-ESM1-5.ssp585.day.gn",
                 "ScenarioMIP.CSIRO-ARCCSS.ACCESS-CM2.ssp585.day.gn",
                 "ScenarioMIP.EC-Earth-Consortium.EC-Earth3-Veg-LR.ssp585.day.gr",
                 "ScenarioMIP.INM.INM-CM4-8.ssp585.day.gr1",
                 "ScenarioMIP.INM.INM-CM5-0.ssp585.day.gr1",
                 "ScenarioMIP.KIOST.KIOST-ESM.ssp585.day.gr1",
                 "ScenarioMIP.MIROC.MIROC6.ssp585.day.gn",
                 "ScenarioMIP.MPI-M.MPI-ESM1-2-LR.ssp585.day.gn",
                 "ScenarioMIP.MRI.MRI-ESM2-0.ssp585.day.gn",
                 "ScenarioMIP.NCC.NorESM2-LM.ssp585.day.gn",
                 "ScenarioMIP.NCC.NorESM2-MM.ssp585.day.gn",
                 "ScenarioMIP.NOAA-GFDL.GFDL-CM4.ssp585.day.gr1",
                 "ScenarioMIP.NOAA-GFDL.GFDL-CM4.ssp585.day.gr2",
                 "ScenarioMIP.NOAA-GFDL.GFDL-ESM4.ssp585.day.gr1",
                 "ScenarioMIP.NUIST.NESM3.ssp585.day.gn"]
        ffhist= [ ff.replace("ScenarioMIP","CMIP") for ff in  hhssp ]
        ffhist=[ ff.replace("ssp585","historical") for ff in  ffhist]
        
    print(ffhist)
    print()
    print(hhssp)
    
    # again load cat for both hist and ssp:
    del cat
    
    cat = col.search(
        activity_id =["CMIP","ScenarioMIP"], # search for historical and hist-nat (detection and attribution)
        variable_id=var,              # search for precipitation
        table_id="day",               # monthly values
        experiment_id =[ssp,"historical"],
        member_id ="r1i1p1f1"
    )
    
    
    kk= 0
    for mod_hist, mod_ssp in zip(ffhist[kk:],hhssp[kk:]):
        print('kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk                         ',kk)
        
        #if kk in [1]:
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
        print("--------------------START time conversion--------------------------------------")
        try:
            holdout_subset['time'] = pd.to_datetime(holdout_subset.indexes['time'])
        except TypeError:
            holdout_subset['time'] = holdout_subset.indexes['time'].to_datetimeindex()
        holdout_subset = holdout_subset.resample(time='1d').mean().load(scheduler='threads').chunk(chunks)
        

        print("finished test_subset--------------------------")
        print()



        
        
        print('start tarining ---------------------------------------->>>>>>>>>>>>>>>>>>>>><'+mod_ssp)
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
###        client = Client()
        kk +=1 
