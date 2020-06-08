FROM registry.access.redhat.com/ubi8/python-38
ENV KAGGLE_CONFIG_DIR=/opt/app-root/app
WORKDIR ${KAGGLE_CONFIG_DIR}

USER root
RUN mkdir -p ${KAGGLE_CONFIG_DIR}/.kaggle
COPY . ${KAGGLE_CONFIG_DIR}
COPY kaggle.json ${KAGGLE_CONFIG_DIR}/.kaggle/kaggle.json
RUN pip install --upgrade pip && \
    pip install -r ${KAGGLE_CONFIG_DIR}/requirements.txt && \
    chmod 600 ${KAGGLE_CONFIG_DIR}/.kaggle/kaggle.json
    #chmod 600 /opt/app-root/app/kaggle.json
USER 1001
EXPOSE 8080
CMD ["gunicorn", "server:app"]


