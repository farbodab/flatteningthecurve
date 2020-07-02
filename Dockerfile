# FROM registry.access.redhat.com/ubi8/python-38
FROM python:3.8
EXPOSE 8080
ENV CONFIG_DIR=/opt/app-root/app \
    install_dir=/usr/local/bin \
    url=https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz



WORKDIR ${CONFIG_DIR}

USER root
COPY . ${CONFIG_DIR}

RUN apt-get update && apt-get install -yq \
    chromium \
    xvfb \
    xsel \
    unzip \
    libgconf2 \
    libncurses5 \
    libxml2 \
    xclip

RUN wget -q "https://chromedriver.storage.googleapis.com/2.35/chromedriver_linux64.zip" -O /tmp/chromedriver.zip \
    && unzip /tmp/chromedriver.zip -d /usr/bin/ \
    && rm /tmp/chromedriver.zip

RUN pip install --upgrade pip && \
    pip install -r ${CONFIG_DIR}/requirements.txt

USER 1001
CMD ["gunicorn","-b 0.0.0.0:8080","--workers=12","server:app"]
