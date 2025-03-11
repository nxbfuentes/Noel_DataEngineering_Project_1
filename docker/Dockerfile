FROM python:3.9

WORKDIR /app

COPY /app .

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "-m", "etl_project.pipelines.opensky_flights"]
