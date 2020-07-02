FROM registry.access.redhat.com/ubi8/python-38
EXPOSE 8080
ENV CONFIG_DIR=/opt/app-root/app \
    install_dir=/usr/local/bin \
    url=https://github.com/mozilla/geckodriver/releases/download/v0.26.0/geckodriver-v0.26.0-linux64.tar.gz



WORKDIR ${CONFIG_DIR}

USER root
COPY . ${CONFIG_DIR}
RUN cat << EOF > /etc/yum.repos.d/google-chrome.repo [google-chrome] \n name=google-chrome \n baseurl=http://dl.google.com/linux/chrome/rpm/stable/x86_64 \n enabled=1 \n gpgcheck=1 \n gpgkey=https://dl.google.com/linux/linux_signing_key.pub \n EOF

RUN dnf install google-chrome-stable glibc fontconfig && \
    mkdir open_date && \
    mkdir 211_data && \
    version=$(wget -qO- https://chromedriver.storage.googleapis.com/LATEST_RELEASE) && \
    url=https://chromedriver.storage.googleapis.com/$version/chromedriver_linux64.zip && \
    curl -o chromedriver_linux64.zip "$url" && \
    unzip chromedriver_linux64.zip && \
    chmod +x chromedriver && \
    mv chromedriver "$install_dir" && \
    pip install --upgrade pip && \
    pip install -r ${CONFIG_DIR}/requirements.txt

USER 1001
CMD ["gunicorn","-b 0.0.0.0:8080","--workers=12","server:app"]
