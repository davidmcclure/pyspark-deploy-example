
FROM dclure/spark

ADD code/requirements.txt /etc
RUN pip install -r /etc/requirements.txt

RUN python -m textblob.download_corpora

ADD code /code
WORKDIR /code
