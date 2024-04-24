from .main import extractDataFromPdf
from util.PdfExtractHelper import extractPdfFromEncodedData


def extractData(parsePdfRequest):
    extractPdfFromEncodedData(parsePdfRequest.encodedPdfData, parsePdfRequest.pdfPassword)
    filePath = "s3://jar-artefacts-preprod/email-preprod/attachement.pdf"
    return extractDataFromPdf(filePath)
