#!/usr/bin/env python

"""
Developer : Vysali Kallepalli
Script : data_transformations.py
Date : 05/12/2023
Purpose : Data Validations and Transformations
"""

import pandas as pd
from datetime import datetime, timedelta


def data_validate1(df):
    """
    Data Validation of Raw Trimet Data
    Args:
        df (pandas dataframe): pd dataframe with raw json data
    """
    print("Performing Assertions on Raw Data... ")
    assertion1 = "The vehicle_id for every bus is a positive number"
    # id = df1["VEHICLE_ID"].astype(int)
    assert (df["VEHICLE_ID"].all()) > 0, f"{assertion1} NOT MET"
    print(f"\t{assertion1} MET")

    assertion2 = "For all records, there exists an EVENT_NO_TRIP which is a positive nine-digit number"
    assert df["EVENT_NO_TRIP"].between(99999999, 10000000000).all(), print(
        f"{assertion2} NOT MET"
    )
    print(f"\t{assertion2} MET")

    assertion3 = "For all records, there exists an EVENT_NO_STOP which is a positive nine-digit number"
    assert df["EVENT_NO_STOP"].between(99999999, 10000000000).all(), print(
        f"{assertion3} NOT MET"
    )
    print(f"\t{assertion3} MET")

    assertion4 = "The ACT_TIME column exists and represent seconds elapsed from midnight to the next day"
    act_time = df["ACT_TIME"].astype(int)
    total_seconds = (24 * 60 * 60) + (60 * 60 * 4)
    if "ACT_TIME" in df.columns:
        assert act_time.between(0, total_seconds).all(), print(f"{assertion4} NOT MET")
        print(f"\t{assertion4}  MET")

    assertion5 = "OPD_DATE format is either DD-MMM-YY or DDMMMYY:00:00:00"
    opd_format = df["OPD_DATE"]
    pattern1 = r"\d{2}-[A-Za-z]{3}-\d{2}"
    pattern2 = r"\d{2}[A-Za-z]{3}\d{4}:\d{2}:\d{2}:\d{2}"
    if (
        opd_format.str.contains(pattern1).any()
        or opd_format.str.contains(pattern2).any()
    ):
        print(f"\t{assertion5} MET")
    else:
        print(f"\t{assertion5} NOT MET")


def data_transform(df):
    """Transformation of Raw Trimet Data

    Args:
        df (pandas dataframe): pd dataframe with raw json data

    Returns:
        pandas Dataframe: pd Dataframe with transformed data
    """
    print("Performing Data Transformations...")
    if df["OPD_DATE"].str.contains(r"\d{2}-[A-Za-z]{3}-\d{2}").any():
        filtered_df = df.copy()
        filtered_df.rename(
            columns={
                "EVENT_NO_TRIP": "trip_id",
                "OPD_DATE": "tstamp",
                "VELOCITY": "longitude",
                "DIRECTION": "latitude",
                "RADIO_QUALITY": "gps_satellites",
                "GPS_LONGITUDE": "gps_hdop",
            },
            inplace=True,
        )
        filtered_df.columns = filtered_df.columns.str.lower()
    else:
        filtered_df = df.copy()
        filtered_df.rename(
            columns={
                "EVENT_NO_TRIP": "trip_id",
                "OPD_DATE": "tstamp",
                "GPS_LONGITUDE": "longitude",
                "GPS_LATITUDE": "latitude",
            },
            inplace=True,
        )
        filtered_df.columns = filtered_df.columns.str.lower()

    filtered_df["tstamp"] = filtered_df["tstamp"].apply(
        lambda value: pd.to_datetime(value, format="%d-%b-%y", errors="coerce")
        if len(value) <= 11
        else pd.to_datetime(value, format="%d%b%Y:%H:%M:%S", errors="coerce")
    )
    filtered_df["act_time"] = pd.to_numeric(filtered_df["act_time"], errors="coerce")
    filtered_df["tstamp"] = filtered_df.apply(
        lambda row: row["tstamp"] + timedelta(seconds=row["act_time"])
        if pd.notnull(row["tstamp"])
        else "",
        axis=1,
    )
    filtered_df = filtered_df.sort_values(["trip_id", "tstamp"])
    filtered_df["dmeters"] = filtered_df.groupby(["trip_id"])["meters"].diff()
    filtered_df["dtimestamp"] = filtered_df.groupby(["trip_id"])["tstamp"].diff()
    filtered_df["speed"] = filtered_df.apply(
        lambda row: round(row["dmeters"] / row["dtimestamp"].total_seconds(), 2), axis=1
    )
    filtered_df["speed"] = filtered_df.groupby(["trip_id"])["speed"].fillna(
        method="bfill"
    )
    filtered_df["service_key"] = filtered_df["tstamp"].dt.dayofweek.apply(
        lambda day: "Weekday" if day < 5 else ("Saturday" if day == 5 else "Sunday")
    )
    return filtered_df


def data_validate2(filtered_df):
    """
     Data Validation of Transformed Trimet Data
    Args:
        df (pandas dataframe): pd dataframe with transformed trimet data
    """
    print("Performing Assertions on Transformed Data...")
    assertion6 = "In all records, the speed value is a non-negative number"
    if filtered_df["speed"].dtype == object:
        filtered_df["speed"] = filtered_df["speed"].astype(float)
        assert filtered_df["speed"].all() >= 0, print(f"{assertion6}: NOT MET")
    print(f"\t{assertion6}: MET")

    assertion7 = "The speed of a TriMet bus should not exceed 100 miles per hour"
    if filtered_df["speed"].dtype == object:
        new_df = filtered_df["speed"].astype(float)
        speed_mph = new_df["speed"] * 2.23694
        speed_limit = 100
        assert not (speed_mph > speed_limit).any(), f"{assertion7}: NOT MET"
    print(f"\t{assertion7}: MET")

    assertion8 = "If a latitude coordinate is present for every trip, then a corresponding longitude coordinate should also be present"
    assert (
        filtered_df["latitude"].notnull() == filtered_df["longitude"].notnull()
    ).all(), f"{assertion8}: NOT MET"
    print(f"\t{assertion8}: MET")

    assertion9 = "gps_satellites exists and is a positive number"
    assert (
        "gps_satellites" in filtered_df.columns
        and filtered_df["gps_satellites"].all() >= 0
    ), f"{assertion9}: NOT MET"
    print(f"\t{assertion9}: MET")

    assertion10 = "gps_hdop exists and is a positive number"
    assert (
        "gps_hdop" in filtered_df.columns and filtered_df["gps_hdop"].all() >= 0
    ), f"{assertion10}: NOT MET"
    print(f"\t{assertion10}: MET")
