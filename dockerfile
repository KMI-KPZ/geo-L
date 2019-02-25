FROM python:3

RUN git clone https://github.com/DServSys/geoLIMES

WORKDIR /geoLIMES

RUN pip install  --no-cache-dir -r requirements.txt

EXPOSE 8888

ENTRYPOINT ["python3", "server.py", "-d"]
