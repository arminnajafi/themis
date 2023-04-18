FROM --platform=linux/amd64 public.ecr.aws/bitnami/python:3.11

COPY dist /usr/themis
RUN pip install /usr/themis/themis-*-py3-none-any.whl

ENTRYPOINT ["abs"]