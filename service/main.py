import os
import pandas as pd
import json
import re
from PIL import Image
from textractor import Textractor
from textractor.data.constants import TextractFeatures, Direction, DirectionalFinderType
from textractor.visualizers.entitylist import EntityList

extractor = Textractor(region_name="ap-south-1")

QUERY_TOTAL_PAYMENT_DUE = "What is the Total Payment Due"
QUERY_PAYMENT_DUE_DATE = "What is the Payment Due date"
QUERY_CREDIT_LIMIT = "What is the Credit Limit"
QUERY_AVAILABLE_CREDIT_LIMIT = "Whats is the Available credit limit"
QUERY_MINIMUM_PAYMENT_DUE = "What is the Minimum Payment due"
QUERY_STATEMENT_GENERATION_DATE = "What is the Statement Generation Date"
QUERY_CREDIT_CARD_NUMBER = "What is the Credit Card Number"

def extractDataFromPdf(file_source: str):

    print("Extracting data from PDF")

    queries = [
        QUERY_TOTAL_PAYMENT_DUE,
        QUERY_PAYMENT_DUE_DATE,
        QUERY_CREDIT_LIMIT,
        QUERY_AVAILABLE_CREDIT_LIMIT,
        QUERY_MINIMUM_PAYMENT_DUE,
        QUERY_STATEMENT_GENERATION_DATE,
        QUERY_CREDIT_CARD_NUMBER
    ]

    queries_to_variable_map = {
        QUERY_TOTAL_PAYMENT_DUE: "total_payment_due",
        QUERY_PAYMENT_DUE_DATE: "payment_due_date",
        QUERY_CREDIT_LIMIT: "credit_limit",
        QUERY_AVAILABLE_CREDIT_LIMIT: "available_credit_limit",
        QUERY_MINIMUM_PAYMENT_DUE: "minimum_payment_due",
        QUERY_STATEMENT_GENERATION_DATE: "statement_generation_date",
        QUERY_CREDIT_CARD_NUMBER: "card_number_last_four"
    }

    document = extractor.start_document_analysis(file_source, features=[TextractFeatures.TABLES, TextractFeatures.QUERIES],
                                                queries=queries, save_image=True)
    tableList = EntityList(document.tables)
    print(document.queries)
    statement_response = {}
    for i in document.queries:
        query_data = str(i)

        for query in queries:
            if query in query_data:
                query_result = query_data.split(query)[1].split(" ")[1]
                statement_response[queries_to_variable_map[query]] =  query_result


    merged = []
    for table in tableList:
        df = table.to_pandas()
        if df.iloc[0, 0].strip() == "DATE" or df.iloc[1, 0].strip() == "DATE":
            merged.append(df.iloc[1:])

    mergedDf = pd.concat(merged) 
    mergedDf.columns = ["date", "txn_details", "merchant_type", "amount"]
    Crs = mergedDf[mergedDf.iloc[:, 3].str.contains('Cr|Credit', case=False)]
    Crs['amount'] = Crs['amount'].apply(lambda x: re.sub(r'[^\d.]', '', x))
    credit_response = json.loads(Crs.to_json(orient='records'))

    Drs = mergedDf[mergedDf.iloc[:, 3].str.contains('Dr|Debit', case=False)]
    Drs['amount'] = Drs['amount'].apply(lambda x: re.sub(r'[^\d.]', '', x))
    debit_response = json.loads(Drs.to_json(orient='records'))

    return {"STATEMENT" : statement_response, "TRANSACTION" : {"CREDIT" : credit_response, "DEBIT" : debit_response}}
 




data = extractDataFromPdf("s3://jar-artefacts-preprod/email-preprod/ACFrOgAF_P-w1p6hIjcE0DcPCx-8-cR3GipBfIFVjLsLUacMATCQaM-Mtg_y7S-4sqJXJf8NN6Ntfk-itLTi8nJUTdSv0n3sRsDpDoD3TB_7HCXNJo2jH9ObCa1HYSHQT6AdQCKJd41jkKXQpHw1LJBByRyex12trdxm5P6LGw==.pdf")

print(data)