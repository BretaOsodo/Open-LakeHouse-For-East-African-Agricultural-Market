FROM python:3.14-slim

#set the working directory
WORKDIR /app

#copy requirements first
copy requirements.txt .

#install dependencies
RUN pip install --no-cache-dir -r requirements.txt

#copy project files and folder
COPY ingestion/ ./ingestion/
COPY testing/ ./testing/
COPY Validation/ ./Validation

#Run application
CMD ["python","ingestion/s3_ingestion.py"]
