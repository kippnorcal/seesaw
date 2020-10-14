FROM python:3.7
WORKDIR /code

# install firefox
RUN apt-get update
RUN apt-get install -y firefox-esr
# install geckodriver
RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.24.0/geckodriver-v0.24.0-linux64.tar.gz 
RUN tar -xvzf geckodriver*
RUN chmod +x geckodriver
RUN mv geckodriver /usr/local/bin/

# SQL dependencies
RUN wget https://packages.microsoft.com/debian/9/prod/pool/main/m/msodbcsql17/msodbcsql17_17.5.2.1-1_amd64.deb
RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y unixodbc unixodbc-dev
RUN yes | dpkg -i msodbcsql17_17.5.2.1-1_amd64.deb

# Project dependencies
COPY Pipfile .
RUN pip install pipenv
RUN pipenv install --skip-lock
COPY ./ .
ENV MOZ_HEADLESS=1
CMD ["pipenv", "run", "python", "main.py"]