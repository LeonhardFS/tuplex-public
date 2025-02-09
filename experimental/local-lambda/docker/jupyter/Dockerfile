FROM jupyter/minimal-notebook

COPY ./requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY ./minio_test.py /home/jovyan/work/minio_test.py