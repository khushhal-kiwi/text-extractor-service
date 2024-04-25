from typing import Union

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import asyncio
import uvicorn

from service.PdfExtractService import extractData, extractDataFromEmailQueue


app = FastAPI()


class ParsePdfRequestBody(BaseModel):
    encodedPdfData: str
    pdfPassword: str
    bankName: str


@app.post("/pdf/parse")
def read_root(parsePdfRequest: ParsePdfRequestBody):
    data = extractData(parsePdfRequest)
    return data


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(poll_queue())

async def poll_queue():
    extractDataFromEmailQueue()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)


