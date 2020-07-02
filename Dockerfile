FROM registry.access.redhat.com/ubi8/python-38
EXPOSE 8080
ENV CONFIG_DIR=/opt/app-root/app \
    install_dir=/usr/local/bin \
    url=https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz



WORKDIR ${CONFIG_DIR}

USER root
COPY . ${CONFIG_DIR}
RUN version=$(wget -qO- https://chromedriver.storage.googleapis.com/LATEST_RELEASE) && \
    url=https://chromedriver.storage.googleapis.com/$version/chromedriver_linux64.zip && \
    curl -s -L "$url" | unzip && \
    unzip chromedriver_linux64.zip && \
    chmod +x chromedriver && \
    mv chromedriver "$install_dir" && \
    dnf install zip glibc fontconfig && \
    mkdir open_date && \
    mkdir 211_data && \
    wget https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2 && \
    tar -xvf phantomjs-2.1.1-linux-x86_64.tar.bz2 && \
    mv phantomjs-2.1.1-linux-x86_64/bin/phantomjs ${install_dir} && \
    pip install --upgrade pip && \
    pip install -r ${CONFIG_DIR}/requirements.txt

USER 1001
CMD ["gunicorn","-b 0.0.0.0:8080","--workers=12","server:app"]
