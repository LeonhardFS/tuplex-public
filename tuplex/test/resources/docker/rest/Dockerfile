FROM python:3.11

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

EXPOSE 8090

# Debug setting
# ENV FLASK_APP=app/main.py
# ENV FLASK_DEBUG=TRUE
# CMD ["python", "-m", "flask", "run", "--host=0.0.0.0", "--port=8090"]

# Production setting
CMD ["gunicorn", "--chdir", "/code/app", "--bind", "0.0.0.0:8090", "--log-level", "info", "main:app"]