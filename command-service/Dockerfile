FROM --platform=linux/amd64 python:3.12-alpine
COPY --from=ubuntu /usr/local/bin /usr/local/bin

RUN apk update && apk add curl jq && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && chmod +x kubectl && mv kubectl /usr/local/bin/

RUN cd /tmp && curl -OL https://get.helm.sh/helm-v3.13.2-linux-amd64.tar.gz \
    && tar -zxvf helm-v3.13.2-linux-amd64.tar.gz \
    && mv linux-amd64/helm /usr/local/bin/helm

# RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
#     && mv kubectl /usr/local/bin/kubectl && chmod +x /usr/local/bin/kubectl

# RUN apk add openssl==3.3.0-r2 libcrypto3==3.3.0-r2 || \
RUN apk add openssl libcrypto3
RUN apk upgrade --no-cache
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY helm-charts ./helm-charts
WORKDIR /app/src
CMD [ "uvicorn", "routes:app", "--host", "0.0.0.0" ]