Currently only for one-cache scenarios! Changes are coming...
This is a fork of the original icarus project!

# Icarus ICN caching simulator
Icarus is a Python-based discrete-event simulator for evaluating caching
performance in Information Centric Networks (ICN).

Icarus is not bound to any specific ICN architecture. Its design allows users
to implement and evalute new caching policies or caching and routing strategy
with few lines of code.

This document explains how to configure and run the simulator.

## Download and installation

### Prerequisites
Before using the simulator, you need to install all required dependencies. 

#### Docker
To make things easier, I've put together a Dockerfile that should pull in all dependencies in a working state and with compatible versions. To use that, install Docker for your operating system, make sure the Docker daemon is started and run the following from the directory containing the Dockerfile
```
docker build . -t icarus:latest
docker run -it icarus /bin/bash
```
With the prompt inside the Docker container, run:
```
(py3k) root@c8a467c91f34:/# python icarus.py -r results config.py
```

The old setup instructions have been removed because this repository was migrated to python3, thus invalidating the instructions.

#### Dev
You can also get the development branch from the Github repository using Git. Just open a shell, `cd` to the directory where you want to download the simulator and type:

    $ git clone https://github.com/romanlutz/icarus.git

## Usage

### Run simulations

To use Icarus with the currently implemented topologies and models of caching policies and strategies you need to do the following.

First, create a configuration file with all the desired parameters of your
simulation. You can modify the file `config.py`, which is a well documented
example configuration. You can even use the configuration file as it is just
to get started. Alternatively, have a look at the `examples` folder which
contains examples of configuration files for various use cases.

Second, run Icarus by running the script `icarus.py` using the following syntax

    $ python icarus.py --results RESULTS_FILE CONF_FILE

where:

 * `RESULTS_FILE` is the [pickle](http://docs.python.org/3/library/pickle.html) file in which results will be saved,
 * `CONF_FILE` is the configuration file

Example usage could be:

    $ python icarus.py --results results.pickle config.py

After saveing the results in pickle format you can extract them in a human
readable format using the `printresults.py` script from the `scripts` folder. Example usage could be:

    $ python scripts/printresults.py results.pickle > results.txt

Icarus also provides a set of helper functions for plotting results. Have a look at the `examples`
folder for plot examples.

By executing the steps illustrated above it is possible to run simulations using the
topologies, cache policies, strategies and result collectors readily available on
Icarus. Icarus makes it easy to implement new models to use in simulations.

To implement new models, please refer to the description of the simulator provided in this paper:

L.Saino, I. Psaras and G. Pavlou, Icarus: a Caching Simulator for Information Centric
Networking (ICN), in Proc. of SIMUTOOLS'14, Lisbon, Portugal, March 2014.
\[[PDF](http://www.ee.ucl.ac.uk/~lsaino/publications/icarus-simutools14.pdf)\],
\[[Slides](http://www.ee.ucl.ac.uk/~lsaino/publications/icarus-simutools14-slides.pdf)\],
\[[BibTex](http://www.ee.ucl.ac.uk/~lsaino/publications/icarus-simutools14.bib)\]

Otherwise, please browse the source code. It is very well documented and easy to understand.

### Modelling tools
Icarus provides utilities for modelling the performance of caches and
work with traffic traces. The code is included in the `icarus.tools` package.
These tools are described in detail in [this paper](http://www.ee.ucl.ac.uk/~lsaino/publications/icarus-simutools14.pdf).

### Run tests
To run the unit test cases you can use the `test.py` script located in the directory of
this README file.

    $ python test.py

To run the test you need to have the Python [`nose`](https://nose.readthedocs.org/en/latest/) package. If you installed all
dependencies using the Ubuntu script, then you already have it installed. Otherwise you may need to install it using either `pip` or `easy_install`.

    $ pip install nose

or

    $ easy_install nose

### Build documentation from source
To build the documentation you can you the `Makefile` provided in the `doc` folder. This script provides targets for building
documentation in a number of formats. For example, to build HTML documentation, execute the following commands:

    $ cd <YOUR ICARUS FOLDER>
    $ cd doc
    $ make html

The built documentation will be put in the `doc/build` folder. The compiled HTML documentation is also available on the
[Icarus website](http://icarus-sim.github.io/doc/)

To build the documentation you need [Sphinx](http://sphinx-doc.org/). If you installed all dependencies using the Ubuntu script,
then you already have it installed. Otherwise you may need to install it using either `pip` or `easy_install`.

    $ pip install sphinx

or

    $ easy_install sphinx

## Citing

If you use Icarus for your paper, please refer to the following publication:

    @inproceedings{icarus-simutools14,
       author = {Saino, Lorenzo and Psaras, Ioannis and Pavlou, George},
       title = {Icarus: a Caching Simulator for Information Centric Networking (ICN)},
       booktitle = {Proceedings of the 7th International ICST Conference on Simulation Tools and Techniques},
       series = {SIMUTOOLS '14},
       year = {2014},
       location = {Lisbon, Portugal},
       numpages = {10},
       publisher = {ICST},
       address = {ICST, Brussels, Belgium, Belgium},
    }

## Documentation
If you desire further information about Icarus, you can find it in the following places:

 * In [this paper](http://www.ee.ucl.ac.uk/~lsaino/publications/icarus-simutools14.pdf), which describes the overall architecture of the Icarus simulator,
   the motivations for its design, the models implemented and shows some snippets of codes on how to use the modelling tools.
 * In the [API reference](http://icarus-sim.github.io/doc/), which documents all packages, modules, classes, methods
   and functions included in the Icarus simulator.
 * In the [source code](https://www.github.com/icarus-sim/icarus/), which is well organized and throughly documented.

## Reproduce results of previous papers

### Hash-routing schemes, ACM SIGCOMM ICN '13
The Icarus simulator can be used to reproduce the results and plot the graphs presented in the paper:

L.Saino, I. Psaras and G. Pavlou, Hash-routing Schemes for Information Centric Networking,
in *Proc. of the 3rd ACM SIGCOMM workshop on Information Centric Networking (ICN'13)*, Hong Kong, China, August 2013.
[\[PDF\]](http://www.ee.ucl.ac.uk/~lsaino/publications/hashrouting-icn13.pdf),
[\[BibTex\]](http://www.ee.ucl.ac.uk/~lsaino/publications/hashrouting-icn13.bib)

To do so, refer to the instructions reported in the  [icarus-sim/hashrouting-icn13-results](http://github.com/icarus-sim/hashrouting-icn13-results) repository.

## License
Icarus is licensed under the terms of the [GNU GPLv2 license](http://www.gnu.org/licenses/gpl-2.0.html).

## Contacts
For further information about the Icarus simulator, please contact
[Lorenzo Saino](http://www.ee.ucl.ac.uk/~lsaino)

## Acknowledgments
This work has been funded by the EU-Japan initiative, under EU FP7 grant agreement no. 608518 and NICT contract no. 167 ([GreenICN](http://www.greenicn.org/)), and by the EU FP7 grant agreement ICT-248784 ([COMET](http://www.comet-project.org/)).
