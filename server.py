from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel

from service.PdfExtractService import extractData


app = FastAPI()


class ParsePdfRequestBody(BaseModel):
    encodedPdfData: str
    pdfPassword: str
    bankName: str


@app.post("/pdf/parse")
def read_root(parsePdfRequest: ParsePdfRequestBody):
    data = extractData(parsePdfRequest)
    return data



