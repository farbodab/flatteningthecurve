FROM registry.access.redhat.com/ubi8/python-38
ENV KAGGLE_CONFIG_DIR=/opt/app-root/app
WORKDIR ${KAGGLE_CONFIG_DIR}

USER root
RUN pip install --upgrade pip && \
    pip install -r ${KAGGLE_CONFIG_DIR}/requirements.txt && \
    chmod 600 ${KAGGLE_CONFIG_DIR}/app.sh
USER 1001
EXPOSE 8080
CMD ["gunicorn", "server:app"]
