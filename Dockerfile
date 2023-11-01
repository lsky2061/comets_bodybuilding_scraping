FROM apache/airflow:2.7.1

# Copy requirements.txt file
COPY requirements.txt /

# Install Python packages from requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

USER root

RUN apt-get -y install libnss3\
      libnspr4\                                    
        libatk1.0-0\                                 
         libatk-bridge2.0-0\                          
         libcups2\                                    
         libdbus-1-3\                                 
         libdrm2\                                     
         libxkbcommon0\                               
         libatspi2.0-0\                               
         libxcomposite1\                              
         libxdamage1\
         libxfixes3\
         libxrandr2\
         libgbm1\
         libasound2\
         libx11-xcb1\
         libxcursor1\  
         libgtk-3-0\ 
         libcairo-gobject2\ 
         libgdk-pixbuf-2.0-0\ 
         libdbus-glib-1-2

USER airflow

# RUN pip install playwright

RUN playwright install