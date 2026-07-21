FROM bitnamilegacy/spark:3.5.6

# pandas/numpy/pyarrow/scikit-learn: servono agli executor per il pandas_udf
# dell'Isolation Forest nel job di detection (streaming/detection_job.py).
# L'immagine bitnami base ha solo pyspark. Nessun utente root richiesto:
# l'utente di default (uid 1001, gid 0) ha gia' i permessi per installare
# in user-site (verificato prima di scrivere questo Dockerfile).
RUN pip3 install --no-cache-dir pandas numpy pyarrow scikit-learn

# L'immagine base non ha una entry /etc/passwd per uid 1001 (funziona per i
# processi avviati dal suo entrypoint, che imposta le cose diversamente, ma
# NON per un `spark-submit` lanciato a mano via `docker exec`): senza,
# Hadoop UserGroupInformation fallisce con
# "LoginException: NullPointerException: invalid null input: name" perche'
# il login OS-level (UnixLoginModule) non trova un nome utente per l'uid.
USER root
RUN echo "spark:x:1001:0:spark:/home/spark:/bin/bash" >> /etc/passwd
USER 1001
