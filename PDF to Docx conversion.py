import PyPDF2

# open the PDF file
with open('clcoding.pdf','rb') as pdf_file:
    pdf_reader = PyPDF2.PdfFileReader(pdf_file)

    # open a new word file
    with open('clcoding.docx','w') as docx_file:
        for page in range (pdf_reader.numPages):
            text = pdf_reader.getPage(page).extractText()
            docx_file.write(text)

            