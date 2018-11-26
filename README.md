Currently only for one-cache scenarios! Changes are coming...
This is an extension of the original icarus project!

# Icarus ICN caching simulator
Icarus is a Python-based discrete-event simulator for evaluating caching performance in Information Centric Networks (ICN).

Icarus is not bound to any specific ICN architecture. Its design allows users to implement and evalute new caching policies or caching and routing strategy with few lines of code.

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

To pull in traces from elsewhere, such as Azure Storage Blobs, create a subscription, resource group and storage account first. Then create a container in the blob storage and put your traces there. The storage key will provide access to the files.


The old setup instructions have been removed because this repository was migrated to python3, thus invalidating the instructions.


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

## License
Icarus is licensed under the terms of the [GNU GPLv2 license](http://www.gnu.org/licenses/gpl-2.0.html).

## Contacts
For further information about this extended version of the Icarus simulator, please contact
[Roman Lutz](https://romanlutz.github.io)

## Acknowledgments
This extension heavily relies on [Lorenzo Saino's original work](https://github.com/icarus-sim/icarus/).
