FROM python:3.10
WORKDIR /app
COPY . /app
RUN pip install requests kafka-python
CMD ["python", "read_stations_status.py"]
