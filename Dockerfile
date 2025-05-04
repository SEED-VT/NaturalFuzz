FROM amazoncorretto:8u372-al2

ENV REPO_LINK=https://github.com/SEED-VT/NaturalFuzz.git
ENV REPO_NAME=NaturalFuzz

RUN yum -y update
RUN yum install -y unzip
RUN yum install -y zip
RUN yum install -y gzip
RUN curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs 
RUN chmod +x cs 
RUN ./cs setup -y

ENV PATH="$PATH:/root/.local/share/coursier/bin"

RUN yum install -y git
RUN yum install -y python3
RUN python3 -m pip install pandas seaborn matplotlib

RUN rpm --import https://yum.corretto.aws/corretto.key
RUN curl -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
RUN yum install -y java-11-amazon-corretto-devel
RUN ./cs fetch org.scala-lang:scala-compiler:2.12.2

RUN git clone $REPO_LINK
RUN mkdir -p $REPO_NAME/graphs
RUN curl -L -o tpcds_noheader_nocommas.zip https://github.com/SEED-VT/NaturalFuzz/releases/download/v1.0.0/tpcds_noheader_nocommas.zip
RUN yum install -y unzip
RUN unzip tpcds_noheader_nocommas.zip
RUN mv tpcds_noheader_nocommas $REPO_NAME/data

RUN cd $REPO_NAME && sbt assembly
