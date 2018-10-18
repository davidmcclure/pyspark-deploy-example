
FROM dclure/spark

ADD code/requirements.txt /etc
RUN pip install -r /etc/requirements.txt

ADD code /code
WORKDIR /code
