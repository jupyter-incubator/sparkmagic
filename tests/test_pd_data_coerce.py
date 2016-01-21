from pandas.util.testing import assert_frame_equal
import pandas as pd

from remotespark.utils.utils import coerce_pandas_df_to_numeric_datetime


def test_no_coercing():
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': u'12'},
               {u'buildingID': 1, u'date': u'random', u'temp_diff': u'0adsf'}]
    desired_df = pd.DataFrame(records)

    df = pd.DataFrame(records)
    coerce_pandas_df_to_numeric_datetime(df)

    assert_frame_equal(desired_df, df)


def test_date_coercing():
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': u'12'},
               {u'buildingID': 1, u'date': u'6/1/13', u'temp_diff': u'0adsf'}]
    desired_df = pd.DataFrame(records)
    desired_df["date"] = pd.to_datetime(desired_df["date"])

    df = pd.DataFrame(records)
    coerce_pandas_df_to_numeric_datetime(df)

    assert_frame_equal(desired_df, df)


def test_date_coercing_none_values():
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': u'12'},
               {u'buildingID': 1, u'date': None, u'temp_diff': u'0adsf'}]
    desired_df = pd.DataFrame(records)
    desired_df["date"] = pd.to_datetime(desired_df["date"])

    df = pd.DataFrame(records)
    coerce_pandas_df_to_numeric_datetime(df)

    assert_frame_equal(desired_df, df)


def test_date_none_values_and_no_coercing():
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': u'12'},
               {u'buildingID': 1, u'date': None, u'temp_diff': u'0adsf'},
               {u'buildingID': 1, u'date': u'adsf', u'temp_diff': u'0adsf'}]
    desired_df = pd.DataFrame(records)

    df = pd.DataFrame(records)
    coerce_pandas_df_to_numeric_datetime(df)

    assert_frame_equal(desired_df, df)


def test_numeric_coercing():
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': u'12'},
               {u'buildingID': 1, u'date': u'adsf', u'temp_diff': u'0'}]
    desired_df = pd.DataFrame(records)
    desired_df["temp_diff"] = pd.to_numeric(desired_df["temp_diff"])

    df = pd.DataFrame(records)
    coerce_pandas_df_to_numeric_datetime(df)

    assert_frame_equal(desired_df, df)


def test_numeric_coercing_none_values():
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': u'12'},
               {u'buildingID': 1, u'date': u'asdf', u'temp_diff': None}]
    desired_df = pd.DataFrame(records)
    desired_df["temp_diff"] = pd.to_numeric(desired_df["temp_diff"])

    df = pd.DataFrame(records)
    coerce_pandas_df_to_numeric_datetime(df)

    assert_frame_equal(desired_df, df)


def test_numeric_none_values_and_no_coercing():
    records = [{u'buildingID': 0, u'date': u'6/1/13', u'temp_diff': u'12'},
               {u'buildingID': 1, u'date': u'asdf', u'temp_diff': None},
               {u'buildingID': 1, u'date': u'adsf', u'temp_diff': u'0asdf'}]
    desired_df = pd.DataFrame(records)

    df = pd.DataFrame(records)
    coerce_pandas_df_to_numeric_datetime(df)

    assert_frame_equal(desired_df, df)
