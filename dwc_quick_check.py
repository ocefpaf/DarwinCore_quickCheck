"""
0. Loads three CSV files (`event_bd.csv`, `occurrence_bd.csv`, and `emof_bd.csv`) into pandas
DataFrames.
1. Checks for the presence of required columns;
2. Verifies data completeness by looking for null values in critical fields;
3. Validates the geographic coordinates (latitude and longitude) to ensure they are within valid
   ranges;
4. Checks for depth information, making sure that `minimumDepthInMeters` is not greater than
   `maximumDepthInMeters` and that depth values are numeric;
5. Checks with the World Register of Marine Species (WoRMS) API to validate scientific names,
   identifying any unaccepted or unfound taxa.
6. Then attempts to merge these DataFrames.
   a. First, `df_event` is merged with `df_occurrence` on the `eventID` column, with a validation
      to ensure it's a one-to-many relationship. The event file should have a unique list of
      `eventID` which match to `eventID` in the occurrence file.
   b. Next, the resulting DataFrame (`df_event_occur`) is merged with df_emof on occurrenceID,
      again with a one-to-many validation. The event and occurrence data files should have unique
      `occurrenceID` values which map to the `occurrenceID` in the emof file.
7. Finally, it checks if the number of rows in the final merged DataFrame (df_event_occur_emof)
   is equal to the number of rows in the df_emof DataFrame, indicating a successful arrangement
   of event files. Since we are merging all of the data together, there should be the same number
   of rows in the final dataset as there are in the emof file, but with more columns from the
   event and occurrence files.

"""

import functools

import stamina

import pandas as pd
import janitor  # noqa: F401
import requests
from pathlib import Path

required_occurrence_columns = [
    "occurrenceID",
    "scientificName",
    "eventDate",
    "decimalLatitude",
    "decimalLongitude",
    "basisOfRecord",
    "occurrenceStatus",
]

required_emof_columns = [
    "eventID",
    "occurrenceID",
    "measurementValue",
    "measurementType",
    "measurementUnit",
]

required_event_columns = [
    "eventID",
    "eventDate",
    "decimalLatitude",
    "decimalLongitude",
    "countryCode",
    "geodeticDatum",
]


required_columns = (
    required_occurrence_columns + required_emof_columns + required_event_columns
)


def check_merge_tables(df_event, df_occurrence, df_emof):
    """Using `one_to_many` to check if merge keys are unique in left dataset."""
    df = None
    try:
        msg = "✅ Merge tables passed!"
        df_event_occur = df_event.merge(
            df_occurrence, on="eventID", validate="one_to_many"
        )
        if df_event_occur.empty:
            msg = f"❌ Merge tables failed.\nCould not merge {set(df_event['eventID'])}\non\n{set(df_occurrence['eventID'])}."
            return df, msg
        df = df_event_occur.merge(
            df_emof, on="occurrenceID", validate="one_to_many", suffixes=(None, "r_")
        )
        if df.empty:
            msg = f"❌ Merge tables failed.\nCould not merge {set(df_occurrence['occurrenceID'])}\non\n{set(df_emof['occurrenceID'])}."
            return df, msg
    except pd.errors.MergeError as err:
        msg = f"❌ Merge tables failed. \n{err}"
    return df, msg


def check_required_columns(df, columns):
    missing_cols = list(set(columns).difference(df.columns))
    res = True
    msg = "✅ Passed required columns!"
    if missing_cols:
        msg = f"❌ Failed! Missing required DwC {missing_cols} columns."
        res = False
    return res, msg


def check_null_values(df, columns):
    missing = df.columns[df.isna().any()].to_list()
    res = True
    msg = "✅ Passed null values check!"
    if missing:
        msg = f"⚠️  Columns {missing} have missing values."
        res = False
    return res, msg


def check_latitude(df):
    res = True
    msg = "✅ Passed `decimalLatitude` bounds!"
    if "decimalLatitude" not in df.columns:
        return False, "⚠️  Cannot find `decimalLatitude` column."
    invalid_lat = df[
        pd.to_numeric(df["decimalLatitude"], errors="coerce").isna()
        | (df["decimalLatitude"] <= -90)
        | (df["decimalLatitude"] >= 90)
    ]
    if not invalid_lat.empty:
        msg = f"❌ Invalid `decimalLatitude` values detected. {invalid_lat.index.tolist()}"
        res = False
    return res, msg


def check_longitude(df):
    res = True
    msg = "✅ Passed `decimalLongitude` bounds!"
    if "decimalLongitude" not in df.columns:
        return False, "⚠️  Cannot find `decimalLongitude` column."
    invalid_lon = df[
        pd.to_numeric(df["decimalLongitude"], errors="coerce").isna()
        | (df["decimalLongitude"] <= -180)
        | (df["decimalLongitude"] >= 180)
    ]
    if not invalid_lon.empty:
        msg = f" Invalid decimalLongitude values detected. {invalid_lon.index.tolist()}"
        res = False
    return res, msg


def check_depth_consistency(df):
    res = True
    msg = "✅ Passed depth consistency test!"
    if ("minimumDepthInMeters", "maximumDepthInMeters") not in df.columns:
        msg = (
            "⚠️  No depth information found (minimumDepthInMeters/maximumDepthInMeters)."
        )
        # We return here b/c we cannot run the tests below without these columns.
        return False, msg

    min_depth = pd.to_numeric(df["minimumDepthInMeters"], errors="coerce")
    msg_min = ""
    if not min_depth.isna().empty:
        msg_min = (
            f"⚠️  Non-numeric values in minimumDepthInMeters {min_depth.index.tolist()}"
        )
        res = False

    max_depth = pd.to_numeric(df["minimumDepthInMeters"], errors="coerce")
    msg_max = ""
    if not max_depth.isna().empty:
        msg_max = (
            f"⚠️  Non-numeric values in minimumDepthInMeters {max_depth.index.tolist()}"
        )
        res = False

    # Check logic: Min should not be greater than Max
    illogical = all(min_depth >= max_depth)

    if not illogical.empty:
        msg = f"❌ minimumDepthInMeters is greater than maximumDepthInMeters {illogical.tolist()}"
        res = False
    msg = f"{msg_min}\n{msg_max}\n{msg}"
    return res, msg


def check_scientific_names(df):
    if "scientificName" not in df.columns:
        return [None, "⚠️  Missing the `scientificName` column!"]

    names = list(set(df["scientificName"]))

    results = [check_scientific_name(name) for name in names]
    return [msg for res, msg in results if not res]


@functools.lru_cache(maxsize=128)
def check_scientific_name(name):
    response = _check_scientific_name(name)

    # Bail early to avoid unnecessary retries.
    if response.status_code == 204 or response.status_code == 400:
        return False, f"⚠️  {response.status_code=} for {name=}."

    if response.status_code == 200:
        results = response.json()
    else:
        msg = f"⚠️  WoRMS API Error: {response.status_code} for {name=}"
        return False, msg

    # The API returns a list of lists (one list per name queried).
    is_unique = f"Found 1 match for {name=}"
    if len(results) > 1:
        is_unique = "⚠️  Found more than 1 match for {name=}, selecting the first one"

    # Take first match.
    result = results[0]
    if result["status"] != "accepted":
        msg = f"{is_unique}\n⚠️  Taxon {result['status']=}. Accepted name: {result['valid_name']=}, {result['url']=}"
        return False, msg
    return True, f"{is_unique}."


@stamina.retry(on=requests.exceptions.HTTPError, attempts=3)
def _check_scientific_name(name):
    url = f"http://www.marinespecies.org/rest/AphiaRecordsByName/{name}?like=true&marine_only=true"
    return requests.get(url, timeout=60)


if __name__ == "__main__":
    # TODO: Web version add 3 boxes for each upload.
    # test_dir = Path("tests/data/bad_data")
    test_dir = Path("tests/data/good_data")
    # test_dir = Path("tests/data/encoding_issues")

    kw = {"encoding_errors": "ignore"}
    clean_names = {
        "axis": "columns",
        "strip_underscores": True,
        "case_type": "preserve",
        "remove_special": True,
    }

    df_event = pd.read_csv(test_dir.joinpath("event_bd.csv"), **kw).clean_names(
        **clean_names
    )
    df_occurrence = pd.read_csv(
        test_dir.joinpath("occurrence_bd.csv"), **kw
    ).clean_names(**clean_names)
    df_emof = pd.read_csv(test_dir.joinpath("emof_bd.csv"), **kw).clean_names(
        **clean_names
    )

    df, msg = check_merge_tables(
        df_event=df_event, df_occurrence=df_occurrence, df_emof=df_emof
    )
    print(msg)

    if df is not None:
        res, msg = check_required_columns(df, columns=required_columns)
        print(msg)

        res, msg = check_null_values(df, columns=required_columns)
        print(msg)

        res, msg = check_latitude(df)
        print(msg)

        res, msg = check_longitude(df)
        print(msg)

        res, msg = check_depth_consistency(df)
        print(msg)

        results = check_scientific_names(df)
        [print(msg) for msg in results]
