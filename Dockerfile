FROM registry.access.redhat.com/ubi8/python-38
EXPOSE 8080
ENV CONFIG_DIR=/opt/app-root/app
WORKDIR ${CONFIG_DIR}

USER root
COPY . ${CONFIG_DIR}
RUN pip install --upgrade pip && \
    pip install -r ${CONFIG_DIR}/requirements.txt

USER 1001
CMD ["gunicorn","-b 0.0.0.0:8080","--workers=12","server:app"]
