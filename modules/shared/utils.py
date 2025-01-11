import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import itertools
import numpy as np
import pandas as pd
from mailersend import emails
from prefect import task
from prefect.variables import Variable

from modules.database.utils import query_table

@task
def send_email(from_email: str, to_emails: list[str], subject: str, body: str):
    mailersend_credentials : dict = Variable.get("mailersend_credentials")
    mailer = emails.NewEmail(mailersend_credentials.get("api_key"))
    mail_body = {}
    mail_from = {
        "email": from_email
    }
    recipients = [ { "email": email } for email in to_emails ]
    mailer.set_mail_from(mail_from, mail_body)
    mailer.set_mail_to(recipients, mail_body)
    mailer.set_subject(subject, mail_body)
    mailer.set_html_content(body, mail_body)
    mailer.send(mail_body)

def list_union(left: list, right: list) -> list:
    left_series = pd.Series(left)
    right_series = pd.Series(right)
    output = left_series.drop_duplicates().to_list()
    output.extend(right_series.loc[~right_series.isin(left_series)].drop_duplicates().to_list())
    return output

def list_except(lst: list, lst_except: list) -> list:
    lst_series = pd.Series(lst)
    lst_except_series = pd.Series(lst_except)
    return lst_series.loc[~lst_series.isin(lst_except_series.to_list())].to_list()

def left_join(df_left: pd.DataFrame, df_right: pd.DataFrame, on: list[str]) -> pd.DataFrame:
     
    # 1. Check for columns in df.
    existing_columns = pd.Series(df_left.columns)
    existing_columns = existing_columns.loc[existing_columns.isin(df_right.columns)]
    existing_columns = existing_columns.loc[~existing_columns.isin(on)].to_list()

    # 2. Merge the DataFrames.
    left_suffix = "_x"
    right_suffix = "_y"
    df_out = df_left.merge(df_right, how="left", on=on, suffixes=[left_suffix, right_suffix])

    # 3. Handle the overwriting of non-null values.
    for column in existing_columns:
        df_out[column] = df_out.apply(lambda x: x[column + left_suffix] if pd.isnull(x[column + right_suffix]) else x[column + right_suffix], axis=1)
        df_out.drop([column + right_suffix, column + left_suffix], axis=1, inplace=True)
    
    return df_out
     
def match_currencies(df: pd.DataFrame, matching_hierarchy_columns: list[str] = ["name","altname","bname"], only_matches: bool = False, include_match_columns: bool = True):
    
    # 1. Check for columns in df.
    matching_columns = set(matching_hierarchy_columns).intersection(df.columns)
    if len(matching_columns) == 0:
        raise Exception("No matching column(s) in the input DataFrame. Must include at least one of the following: {matching_hierarchy_columns}".format(matching_hierarchy_columns=str.join(", ", matching_hierarchy_columns)))

    # 2. Get current Currency data.
    key_columns_series = ["timestamp", "currency"]
    currency = query_table("SELECT {currency_columns} FROM \"Currency\"".format(currency_columns=str.join(",",list_union(key_columns_series, matching_hierarchy_columns))))
    key_columns_series = pd.Series(["timestamp", "currency"])
    matching_hierarchy_column_series = pd.Series(matching_hierarchy_columns)
    currency_columns = key_columns_series.to_list()
    currency_columns.extend(matching_hierarchy_column_series.loc[~matching_hierarchy_column_series.isin(key_columns_series)].drop_duplicates().to_list())
    currency = currency[list_union(list_except(key_columns_series, ["timestamp"]), matching_hierarchy_columns)]

    # 3. Try and match the input dataframe to a currency for each item in the matching hierarchy.
    df_out = df.copy()
    for column in matching_hierarchy_columns[::-1]:
        if column in df.columns:
            columns_i = ["currency", column]
            currency_i = currency[columns_i].copy()
            currency_i["match_column"] = column
            currency_i = currency_i.loc[currency_i[column].isin(df_out[column].to_list())]
            df_out = left_join(df_out, currency_i, on=[column])

    # 4. Format the result with a new column showing which column was matched to and the status of the match.
    df_out = df_out.replace({ np.nan : None })
    df_out["match_status"] = df_out["currency"].apply(lambda x: "MATCH" if x != None else "NO_MATCH")
    df_out["currency"] = df_out["currency"].astype('Int64')

    # 5. Only include matches if specified.
    df_out =  df_out.loc[df_out["match_status"] == "MATCH"] if only_matches else df_out

    # 6. Include match columns if specified.
    if not include_match_columns:
        df_out.drop(columns=["match_status", "match_column"], axis=1, inplace=True)
        df_out.reset_index(inplace=True)
    
    return df_out

def match_currency_pairs(df: pd.DataFrame, pair_column: str = "pair", matching_hierarchy_columns: list[str] = ["name","altname","bname"], only_matches: bool = False, include_match_columns: bool = True):
    
    # 1. Check for columns in df.
    if not pair_column in set(df.columns):
        raise Exception(f"The input DataFrame must contain the input pair_column with name: {pair_column}")

    # 1. Get current Currency data.
    key_columns_series = ["timestamp", "currency"]
    currency = query_table("SELECT {currency_columns} FROM \"Currency\"".format(currency_columns=str.join(",",list_union(key_columns_series, matching_hierarchy_columns))))
    key_columns_series = pd.Series(["timestamp", "currency"])
    matching_hierarchy_column_series = pd.Series(matching_hierarchy_columns)
    currency_columns = key_columns_series.to_list()
    currency_columns.extend(matching_hierarchy_column_series.loc[~matching_hierarchy_column_series.isin(key_columns_series)].drop_duplicates().to_list())
    currency = currency[list_union(list_except(key_columns_series, ["timestamp"]), matching_hierarchy_columns)]

    # 2. Try and match the input dataframe to a currency for each item in the matching hierarchy.
    df_out = df.copy()
    for column in matching_hierarchy_columns[::-1]:
        columns_i = ["currency", column]
        currency_i = currency.loc[~currency[column].isnull()][columns_i]
        combinations_i = list(itertools.combinations(currency_i.to_dict("records"), 2))
        combinations_i.extend([(x[1], x[0]) for x in combinations_i])
        currency_pairs_i = pd.DataFrame([
            {
                "from_currency" : x[0].get("currency"), 
                f"from_{column}" : x[0].get(column), 
                "to_currency" : x[1].get("currency"), 
                f"to_{column}" : x[1].get(column),
                "match_column" : column
            } for x in combinations_i ])
        if len(currency_pairs_i) == 0:
            continue
        currency_pairs_i["pair"] = currency_pairs_i[f"from_{column}"] + currency_pairs_i[f"to_{column}"]
        currency_pairs_i = currency_pairs_i.loc[currency_pairs_i[pair_column].isin(df_out[pair_column].to_list())]
        df_out = left_join(df_out, currency_pairs_i, on=[pair_column])
        df_out.drop(columns=[f"from_{column}", f"to_{column}"], axis=1, inplace=True)

    # 3. Format the result with a new column showing which column was matched to and the status of the match.
    df_out = df_out.replace({ np.nan : None })
    df_out["match_status"] = df_out["match_column"].apply(lambda x: "MATCH" if x != None else "NO_MATCH")
    df_out["from_currency"] = df_out["from_currency"].astype('Int64')
    df_out["to_currency"] = df_out["to_currency"].astype('Int64')

    # 4. Only include matches if specified.
    df_out =  df_out.loc[df_out["match_status"] == "MATCH"] if only_matches else df_out

    # 5. Include match columns if specified.
    if not include_match_columns:
        df_out.drop(columns=["match_status", "match_column"], axis=1, inplace=True)
        df_out.reset_index(drop=True, inplace=True)
    
    return df_out