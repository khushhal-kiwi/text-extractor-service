from pydantic import BaseModel


class ParsePdfRequestBody(BaseModel):
    encodedPdfData: str
    pdfPassword: str
    informationType: str
        

