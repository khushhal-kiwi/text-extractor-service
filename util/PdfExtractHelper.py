import base64
import io
import PyPDF2
import boto3
import hashlib

def extractPdfFromEncodedData(base64_encoded_pdf, password, message_id):
    print("Decoding pdf from encoded data")
    decoded_pdf = base64.urlsafe_b64decode(base64_encoded_pdf)

    pdf_reader = PyPDF2.PdfReader(io.BytesIO(decoded_pdf))
    try:
        pdf_reader.decrypt(password)
    except NotImplementedError:
        pass

    pdf_writer = PyPDF2.PdfWriter()

    for page_num in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[page_num]
        try:
            page.decrypt(password)
        except Exception:
            pass
        pdf_writer.add_page(page)


    output_pdf = io.BytesIO()
    pdf_writer.write(output_pdf)
    output_pdf.seek(0)

    md5_checksum = hashlib.md5(output_pdf.read()).digest()

    output_pdf.seek(0)

    bucket_name = "jar-artefacts-preprod"
    s3_key = "email-preprod/" + message_id + ".pdf"

    s3_client = boto3.client('s3')

    try:
        # Upload the PDF file to S3 with Content-MD5 checksum
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=output_pdf, ContentMD5=base64.b64encode(md5_checksum).decode('utf-8'))
    except Exception as e:
        print("Error:", e)