FROM tensorflow/tensorflow:latest

COPY . ./

RUN pip install transformers
RUN pip install tensorflow
RUN pip install boto3
RUN pip install pySqsListener
RUN pip install python-decouple

ENV CUDA_VISIBLE_DEVICES="-1"
# This *may* cause a peformance issue...
ENV PYTHONUNBUFFERED="1"

ENTRYPOINT ["python", "app.py"]