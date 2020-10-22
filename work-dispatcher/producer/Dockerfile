FROM tiangolo/meinheld-gunicorn-flask:python3.7

COPY ./app /app
RUN pip3 install -r /app/requirements.txt

ENV PORT 8080
EXPOSE 8080

