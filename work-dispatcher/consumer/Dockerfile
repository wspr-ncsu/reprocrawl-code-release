FROM python:3.7-stretch

RUN useradd -u 1000 -d /work kpw
COPY requirements.txt /work/
RUN pip3 install -r /work/requirements.txt

USER kpw
WORKDIR /work
COPY kpw.py /work/
COPY barrier/ /work/barrier/

# Run our custom RQ worker as the entry point
ENTRYPOINT ["/usr/local/bin/dumb-init", "--", "python3", "kpw.py"]

# Additional arguments: list of queue names
CMD ["default"]
