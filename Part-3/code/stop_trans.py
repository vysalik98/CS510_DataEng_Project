import pandas as pd

def stop_val(df):
    """
    Data Validation of Stop Events Data
    Args:
        df (pandas dataframe): pd dataframe with stopevents data
    """
    assertion1 = "Route id exists and is a positive number"
    assert("route_id" in df.columns and df["route_id"].all() >= 0), f"{assertion1}: NOT MET"
    f"{assertion1} : MET"
    assertion2 = "The service key represents the day of the week, and it can take one of three possible values: S,W,U"
    assert df["service_key"].isin(['M', 'S', 'W', 'U']).all(), f"{assertion2}: NOT MET"
    f"{assertion2} : MET"
    assertion3 = "Trip id a non negative, 9-digit integer"
    assert (
    df["trip_id"].all() > 0 & df["trip_id"].astype(int).between(99999999, 10000000000).all()
    ), f"{assertion3}: NOT MET"
    f"{assertion3} : MET"
    assertion4 = "Direction exists and is either 0 or 1"
    assert(df["direction"].all() in [0, 1]), f"{assertion4}: NOT MET"
    f"{assertion4} : MET"
    assertion5 = "Vehicle ID is non-null and positive number"
    assert(df["vehicle_id"].all() > 0), f"{assertion5}: NOT MET"
    f"{assertion5} : MET"
    return df

def stop_trans(df):
    """
    Data Transformation of Stop Events Data
    Args:
        df (pandas dataframe): pd dataframe with stopevents data
    """
    df['service_key'] = df['service_key'].apply(lambda x: 'Saturday' if x == 'S' else("Sunday" if x == 'U' else 'Weekday'))
    df['direction'] = df['direction'].apply(lambda x: 'Out' if x == '0' else("Back" if x == '1' else 'Unknown'))
    return df