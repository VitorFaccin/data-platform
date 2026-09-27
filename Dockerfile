# The image compose actually runs: official Airflow + this repo's runtime dependencies,
# resolved once at BUILD time. Installing at container boot was rejected (README,
# decision 12): it re-resolves the dependency tree on every start — slow, and the set
# that boots today can differ from the one that booted yesterday. A baked image is
# immutable: what CI built is what runs.
# The Python version is pinned in the tag, not inherited: the bare 3.3.1 tag ships
# Python 3.13 today, and CI (python-version + constraints file) must test the SAME
# interpreter the image runs. Change all three together.
FROM apache/airflow:3.3.1-python3.13

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
