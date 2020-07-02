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
    firefox-esr=52.6.0esr-1~deb9u1 \
    chromium=62.0.3202.89-1~deb9u1 \
    git-core=1:2.11.0-3+deb9u2 \
    xvfb=2:1.19.2-1+deb9u2 \
    xsel=1.2.0-2+b1 \
    unzip=6.0-21 \
    python-pytest=3.0.6-1 \
    libgconf2-4=3.2.6-4+b1 \
    libncurses5=6.0+20161126-1+deb9u2 \
    libxml2-dev=2.9.4+dfsg1-2.2+deb9u2 \
    libxslt-dev \
    libz-dev \
    xclip=0.12+svn84-4+b1

RUN wget -q "https://chromedriver.storage.googleapis.com/2.35/chromedriver_linux64.zip" -O /tmp/chromedriver.zip \
    && unzip /tmp/chromedriver.zip -d /usr/bin/ \
    && rm /tmp/chromedriver.zip

RUN pip install --upgrade pip && \
    pip install -r ${CONFIG_DIR}/requirements.txt

USER 1001
CMD ["gunicorn","-b 0.0.0.0:8080","--workers=12","server:app"]
