FROM python:3.11

RUN apt-get update && \
    apt-get install -y openjdk-21-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /workspace

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD ["jupyter","lab","--ip=0.0.0.0","--port=8080","--no-browser","--allow-root","--ServerApp.token=","--ServerApp.password="]