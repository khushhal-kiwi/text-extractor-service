import os
import pandas as pd
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

def extractDataFromPdf(file_source: str):

    queries = [
        QUERY_TOTAL_PAYMENT_DUE,
        QUERY_PAYMENT_DUE_DATE,
        QUERY_CREDIT_LIMIT,
        QUERY_AVAILABLE_CREDIT_LIMIT,
        QUERY_MINIMUM_PAYMENT_DUE,
        QUERY_STATEMENT_GENERATION_DATE
    ]

    queries_to_variable_map = {
        QUERY_TOTAL_PAYMENT_DUE: "total_payment_due",
        QUERY_PAYMENT_DUE_DATE: "payment_due_date",
        QUERY_CREDIT_LIMIT: "credit_limit",
        QUERY_AVAILABLE_CREDIT_LIMIT: "available_credit_limit",
        QUERY_MINIMUM_PAYMENT_DUE: "minimum_payment_due",
        QUERY_STATEMENT_GENERATION_DATE: "statement_generation_date"
    }

    document = extractor.start_document_analysis(file_source, features=[TextractFeatures.TABLES, TextractFeatures.QUERIES],
                                                queries=queries, save_image=True)
    tableList = EntityList(document.tables)
    # print(document.queries)
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
    Crs = mergedDf[mergedDf.iloc[:, 3].str.contains('Cr|Credit', case=False)]
    print("CRS")
    print(Crs.head())

    Drs = mergedDf[mergedDf.iloc[:, 3].str.contains('Dr|Debit', case=False)]
    print("DRS")
    print(Drs.head())

    return {"STATEMENT" : statement_response}
 

