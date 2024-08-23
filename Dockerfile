FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /Brazil_Order_ayomide

#install dependencies
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY entrypoint.sh /Brazil_Order_ayomide/entrypoint.sh
RUN chmod +x /Brazil_Order_ayomide/entrypoint.sh

#copy the data directory to the container
COPY data /Brazil_Order_ayomide/data

EXPOSE 8080
#copy the entire src directory, which includes the main files
COPY src /Brazil_Order_ayomide/src
#copy the entire tests directory, which includes the test files
#COPY tests /Brazil_Order_ayomide/tests
#copy the run_tests.py file to the container
#COPY run_tests.py /Brazil_Order_ayomide/run_tests.py

# #Install DuckDB
# RUN apt-get update && apt-get install -y wget
# RUN apt-get update && apt-get install -y unzip
# RUN wget https://github.com/duckdb/duckdb/releases/download/v0.8.1/duckdb_cli-linux-amd64.zip
# RUN unzip duckdb_cli-linux-amd64.zip
# RUN chmod +x duckdb