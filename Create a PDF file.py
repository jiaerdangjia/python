from reportlab.pdfgen import canvas

# create a new PDF file
pdf_file = canvas.Canvas("example1.pdf")

# add text to the PDF file 
pdf_file.drawString(72,720,"Helllo,World!")

# save the PDF file
pdf_file.save()