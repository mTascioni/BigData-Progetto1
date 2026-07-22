FROM bitnamilegacy/spark:3.5.6

RUN pip3 install --no-cache-dir pandas numpy pyarrow scikit-learn

USER root
RUN echo "spark:x:1001:0:spark:/home/spark:/bin/bash" >> /etc/passwd
USER 1001
