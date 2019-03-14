FROM    python:3.7

# By default one of Airflow's dependencies installs a GPL dependency (unidecode). To avoid this dependency set SLUGIFY_USES_TEXT_UNIDECODE=yes in your environment when you install or upgrade Airflow. To force installing the GPL version set AIRFLOW_GPL_UNIDECODE
ENV     SLUGIFY_USES_TEXT_UNIDECODE=yes

# Never prompts the user for choices on installation/configuration of packages
ENV     DEBIAN_FRONTEND noninteractive
ENV     TERM linux

# Define en_US.
ENV     LANGUAGE    en_US.UTF-8
ENV     LANG        en_US.UTF-8
ENV     LC_ALL      en_US.UTF-8
ENV     LC_CTYPE    en_US.UTF-8
ENV     LC_MESSAGES en_US.UTF-8
ENV     LC_ALL      en_US.UTF-8

ENV     AIRFLOW_HOME /root

RUN         set -ex \
            mkdir -p ${AIRFLOW_HOME}/logs \
                     ${AIRFLOW_HOME}/dags \
                     ${AIRFLOW_HOME}/plugins \
        &&  buildDeps=' \
                build-essential \
                libblas-dev \
                libffi-dev \
                libkrb5-dev \
                liblapack-dev \
                libpq-dev \
                libsasl2-dev \
                libssl-dev \
                libxml2-dev \
                libxslt1-dev \
                python3-dev \
                python3-pip \
                zlib1g-dev \
                default-libmysqlclient-dev \
            ' \
        &&  apt-get update -yqq \
        &&  apt-get install -yqq --no-install-recommends \
                $buildDeps \
                apt-utils \
                curl \
                git \
                locales \
                netcat \
                mysql-client \
                gettext-base \
        &&  sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
        &&  locale-gen \
        &&  update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
        &&  pip install --no-cache-dir 'apache-airflow[hive,s3]==1.10.2' 'tenacity==4.12.0' \
        &&  apt-get remove --purge -yqq $buildDeps libpq-dev \
        &&  apt-get clean \
        &&  rm -rf \
                /var/lib/apt/lists/* \
                /tmp/* \
                /var/tmp/* \
                /usr/share/man \
                /usr/share/doc \
                /usr/share/doc-base \
        &&  airflow initdb

EXPOSE     8080 5555 8793

WORKDIR    ${AIRFLOW_HOME}
CMD ["airflow"]

