FROM python:3.7-slim

RUN mkdir /data
WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

WORKDIR /data
#ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
#CMD []
CMD [ "python", "/usr/src/app/elastician/tools.py" ]