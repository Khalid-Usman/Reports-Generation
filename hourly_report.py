import os
import sys
import glob
import argparse
import warnings
import numpy as np
import pandas as pd
from tqdm import tqdm
from typing import List
from os.path import join

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

STATUSES = {
    'Declined': 'Declined',
    'Rejected': 'Rejected',
    'Returned': 'Returned',
    'Posted': 'Posted',
    'CR_PARTIC_NAME': 'CR_PARTIC_NAME'
}

SUB_STATUSES = {
    'Duplication': ' Duplication',
    'No response from Beneficiary': ' No Response',
    'Amount is invalid or missing': ' Amount Invalid',
    'Already returned original SCT': ' Already Returned',
    'Account number is invalid or missing': ' Account Invalid',
    'Account currency is invalid or missing': ' Currency Invalid',
    'Payment is a duplicate of another payment': ' Duplicate Payment',
    'Reason has not been specified by end customer': ' Reason Not Specified',
    'Creditor account number invalid or missing': ' Creditor Account Invalid',
    'Specific transaction/message amount is greater than allowed maximum': ' Amount Exceed',
    'Transaction forbidden on this type of account (formerly No Agreement)': ' Transaction Forbidden',
    'Account specified is blocked, prohibiting posting of transactions against it': ' Account Blocked',
    'Account number specified has been closed on the bank of accounts books': ' Account Specified Closed',
    'Cancellation requested following technical problems resulting in an erroneous transaction': ' Technical Problems',
    'Specification of the debtor’s name and/or address needed for regulatory requirements is insufficient or '
    'missing': ' Missing Debtor Name Or Address',
    'Success': ' Success',
    'Invalid value': ' Invalid Value',
    'Lack of funds': ' Lack of Funds',
    'Wrong rejection motive': ' Wrong motive',
    'Invalid value date': ' Invalid value date',
    'Payment is not found': ' Payment not Found',
    'Document was rejected by timeout': ' Timeout',
    'On-us dynamic QR code payment': ' DQRC Payment',
    'Cancellation requested by the Debtor': ' Cancelled by Debtor',
    'Partial return requested by customer': ' Partial return by Customer',
    'Reason has not been specified by agent': ' Reason not specified by Agent',
    'Return for original payment is already registered': ' Return already Registered',
    'Original credit transfer never received': ' Original credit transfer never received',
    'Amount of funds available to cover specified message amount is insufficient': ' Amount Insufficient',
    'Transaction type not supported/authorized on this account': ' Transaction not supported on this account',
    'Invalid value customer identification (such as CNIC, NTN, POC, etc.)': ' Invalid Customer Identification',
    'Specification of the debtors account or unique identification needed for reasons of regulatory requirements is'
    ' insufficient or missing': ' Regulatory requirements are insufficient'
}


def check_file_exists(path: str, name: str):
    """
    This function will check the path of csv, if it does not exist then raise an error
    :param path: path of file contains cnic
    :param name: name of file contains cnic
    :return:
    """
    if not os.path.exists(path):
        raise FileNotFoundError("{name} does not exist!".format(name=name))


def highlight(s):
    res = s.str.split('%').str[0].astype(float) > 97
    return ['background-color: red' if v else 'background-color: green' for v in res]


def save_report(df_report: pd.DataFrame):
    """
    This function will generate reports (csv and excel) and save into the output directory
    :param df_report: The dataframe used to generate report
    :return:
    """
    df_report.to_csv(os.path.join(args.target_path, '3_hours.csv'), index=False)
    writer = pd.ExcelWriter(os.path.join(args.target_path, '3_hours.xlsx'))
    df_report.to_excel(writer, index=False)
    writer._save()


def post_processing(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    This function will join two dataframes based on some common key i.e. PARTICIPANT_NAME
    :param df1: The debitor dataframe
    :param df2: The creditor datafram
    :return: The dataframe after joining debitor and creditor dataframes and some post-processing
    """
    df_merged = df1.merge(df2, how='outer', on='PARTICIPANT_NAME').fillna(0)
    df_merged.loc['Total'] = df_merged.sum(numeric_only=True, axis=0)
    df_merged[df_merged.eq(0)] = np.nan
    df_merged.iloc[-1, 0] = 'GRAND TOTAL'
    avg_success = df_merged['Success'].str.replace('%', '').astype(float).mean()
    df_merged.iloc[-1, -1] = "{:.2f}%".format(avg_success)
    return df_merged


def filter_file(file_name: str):
    """
    This function will read csv file and filter it out
    :param file_name: Name of file
    :return:
    """
    pivot_df = pd.read_excel(file_name)
    pivot_df = pivot_df.loc[:, ["ENDTOENDID", "DB_PARTIC_NAME", "CR_PARTIC_NAME", "TR_STATUS_NAME",
                                "BANK_OP_CODE", "REJECT_MOTIVE_DESCRITION"]]
    # pivot_df = pivot_df[(pivot_df['BANK_OP_CODE'] == "CTWA") | (pivot_df['BANK_OP_CODE'] == "CTAA") |
    #                    (pivot_df['BANK_OP_CODE'] == "CTAW") | (pivot_df['BANK_OP_CODE'] == "CTWW")]
    pivot_df = pivot_df[(pivot_df['BANK_OP_CODE'] != "CSDC") & (pivot_df['BANK_OP_CODE'] != "PMCT")]
    return pivot_df


def extract_creditor_details(df_cred: pd.DataFrame):
    """
    This function will read datafrane and extract creditor information
    :param df_cred: The dataframe
    :return: The sub-dataframe containing only creditor data
    """
    df_cred = pd.pivot_table(df_cred, columns=["TR_STATUS_NAME", "REJECT_MOTIVE_DESCRITION"], index="CR_PARTIC_NAME",
                             values="ENDTOENDID", aggfunc='count').reset_index()
    df_cred = df_cred.fillna(0)
    cols = df_cred.columns.values
    new_cols = []
    for index, item in enumerate(cols):
        temp_col_name = ""
        if item[0] in STATUSES:
            temp_col_name += STATUSES[item[0]]
        sub_status = item[1].rsplit('.', 1)[0]
        sub_status = sub_status.replace("'", "")
        if sub_status in SUB_STATUSES:
            temp_col_name += SUB_STATUSES[sub_status]
        new_cols.append(temp_col_name)

    df_cred.columns = new_cols
    col_posted = df_cred.pop("Posted")
    df_cred.insert(1, col_posted.name, col_posted)
    df_cred['Grand Total'] = df_cred.sum(numeric_only=True, axis=1)
    df_cred['Success'] = (df_cred['Posted'] / df_cred['Grand Total']) * 100
    df_cred['Success'] = df_cred['Success'].apply(lambda x: '{0:.2f}%'.format(x))
    df_cred.rename(columns={'CR_PARTIC_NAME': 'PARTICIPANT_NAME', 'Posted': 'Received Posted'}, inplace=True)
    return df_cred


def extract_debitor_details(df_deb: pd.DataFrame):
    """
    This function will read the full data and extract debitor information
    :param df_deb: The dataframe
    :return: The dataframe containing only debitor data
    """
    df_deb = pd.pivot_table(df_deb, columns="TR_STATUS_NAME", index="DB_PARTIC_NAME", values="ENDTOENDID",
                            aggfunc='count').reset_index()
    df_deb = df_deb.loc[:, ["DB_PARTIC_NAME", "Posted"]]
    df_deb = df_deb.fillna(0)
    df_deb['Posted'] = df_deb['Posted'].astype(int)
    df_deb.rename(columns={'DB_PARTIC_NAME': 'PARTICIPANT_NAME', 'Posted': 'Sent Posted'}, inplace=True)
    return df_deb


def merge_files(files: List) -> pd.DataFrame:
    """
    This function will read all csv files in the source folder and stack them vertically
    :param files: List of all input files
    :return: It will return a dataframe
    """
    data = pd.DataFrame()
    for i in tqdm(range(0, len(files)), desc="Merging files ..."):
        file_path = join(args.source_path, files[i])
        check_file_exists(file_path, "Sheet Not Found!")
        temp_df = filter_file(file_name=file_path)
        data = pd.concat([data, temp_df], ignore_index=True)
        data = data.fillna("")
    return data


def parse_args(sys_args: List[str]) -> argparse.Namespace:
    """
    This function parses the command line arguments.
    Parameters
    :param sys_args: Command line arguments
    :returns argparse namespace
    """
    parser = argparse.ArgumentParser(description='Execute data_analyses for showing data')
    parser.add_argument('--target_path', '-t', required=True, type=str, help='path to a target directory where you'
                                                                             ' want to store output files')
    parser.add_argument('--source_path', '-s', required=True, type=str, help='path of input directory that contains'
                                                                             ' csv files')
    return parser.parse_args(sys_args)


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])
    assert len(args.source_path) > 0 and os.path.exists(args.source_path), "source path not found!"
    assert len(args.target_path) > 0 and os.path.exists(args.target_path), "target path not found!"
    all_files = glob.glob(os.path.join(args.source_path, "*.xlsx"))
    assert len(all_files) > 0, "Source folder do not contain any input file"

    df = merge_files(files=all_files)
    print("We are performing complex calculations for you. Please wait ...")
    df_debit = extract_debitor_details(df_deb=df)
    df_credit = extract_creditor_details(df_cred=df)

    df = post_processing(df1=df_debit, df2=df_credit)
    save_report(df_report=df)
    print("Congratulations! Your 3-hour Report is Ready!")