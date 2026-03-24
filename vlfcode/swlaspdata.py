import pandas as pd
import urllib.parse as parse
import sys

goes_satellite = 19

# Datasets/variables
datasets = {
    "xray-flux": f"dap/noaa_goes{goes_satellite}_xrs_1m", 
    "particle-flux": f"dap/goesp_part_flux_P5M", # proton and electron
    "magnetometer": f"dap/goesp_mag_p1m",
    "planetary-kp": f"dap2/iswa_noaa_kp_P3H",  #dap2/potsdam_kp iswa_noaa_kp_P3H.
}
# TODO: define satellite number to download from
# sat_nbr
def get_data(dataset_id, tstart, tend=None):
    '''
    dataset_id : dataset of variable
    tstart, tend: interval times in format "YYYY-MM-DDTHH:mm:SS". tend can be None to grab the latests
    
    available datasets IDs: 
        - xray-flux        : solar xray fluz
        - particle-flux    : solar proton and electron 
        - magnetometer     : GOES magnetometer
        - planetary-kp     : Dst Index/ Kp
    
    Returns: DataFrame
    '''
    baseurl = f"https://lasp.colorado.edu/space-weather-portal/latis/{dataset_id}.csv?"

    tstart_enc = parse.quote(tstart)
    
    # Manually URL encode the '>' symbol as '%3E'
    query = f"time%3E={tstart_enc}"
    
    if tend:
        tend_enc = parse.quote(tend)
        # Manually URL encode the '<' symbol as '%3C'
        query += f"&time%3C{tend_enc}"
        
    full_url = baseurl + query
    # print(f"Downloading data from {full_url}")
    try: 
        df = pd.read_csv(full_url, storage_options={'User-Agent': 'Mozilla/5.0'})
        return df
    except Exception as e:
        print(f"Error downloading {dataset_id}: {e}")     
        return None

if __name__ == "__main__":
    # test demo
    
    t1 = "2026-01-01T08:00:00"
    t2 = "2026-01-01T16:00:00"
    print("Demo: Downloading data form LASP")
    print("Time Range: ", t1, "to", t2)
    # https://lasp.colorado.edu/space-weather-portal/latis/dap2/iswa_noaa_kp_P3H.csv?time>=2024-03-01T00:00:00&time<2024-03-02T00:00:00
    for item in datasets.items():
        print(item)
        df = get_data(item[1], t1, t2)
        print(df.head())
        print("\n---")
    
    print("\n\nAll done.")