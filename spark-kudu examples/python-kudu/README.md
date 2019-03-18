# Python & Kudu

Adapted from [here](https://kudu.apache.org/docs/developing.html#_kudu_python_client)

## Getting Started

To get started, some dependences must be fullfilled. The python interacts with the C++ API, therefore that API must be present. To install these dependencies, do the following for a CentOS, Fedora, or RHEL system.
```
sudo yum install kudu-client0
sudo yum install kudu-client-devel
```
For other systems, you can find the commands in the following locations: [SLES](https://kudu.apache.org/docs/installation.html#_install_on_sles_hosts), [Ubuntu/Debian](https://kudu.apache.org/docs/installation.html#_install_on_ubuntu_or_debian_hosts)

Before continuing, you must install `Cython` and `kudu-python`. A specific version of `kudu-python` must be installed (version 1.2.0) in order to be compatible.
```
sudo pip install Cython
sudo pip install kudu-python==1.2.0
```

Next, to begin just start up the pyspark shell through the following command:
```
$ pyspark
```