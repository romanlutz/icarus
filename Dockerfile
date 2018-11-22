FROM continuumio/miniconda3
ADD ./requirements.txt /
RUN conda create -y -n py3k anaconda python=3.5.6 pip=10.0.1 cryptography=2.3 numpy=1.10.4 scipy=1.1.0 matplotlib=3.0.0 networkx=2.1
RUN chmod 777 /opt/conda/bin/activate
RUN echo "source activate py3k" > ~/.bashrc
ENV PATH /opt/conda/envs/py3k/bin:$PATH
RUN /opt/conda/bin/activate py3k
RUN pip install -r requirements.txt
ADD . /
