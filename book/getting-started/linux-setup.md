# Setting Up Your Ubuntu Environment for Wallaroo

These instructions have been tested for Ubuntu Artful, Bionic, Trusty, and Xenial releases.

There are a few applications/tools which are required to be installed before you can proceed with the setup of the Wallaroo environment.

## Memory requirements

In order to compile the Wallaroo example applications, your system will need to have approximately 3 GB working memory (this can be RAM or swap). If you don't have enough memory, you are likely to see that the compile process is `Killed` by the OS.

## Update apt-get

Ensures you'll have the latest available packages:

```bash
sudo apt-get update
```

## Installing git

If you do not already have Git installed, install it:

```bash
sudo apt-get install git
```

## Set up Environment for the Wallaroo Tutorial

If you haven't already done so, create a directory called `~/wallaroo-tutorial` and navigate there by running:

```bash
cd ~/
mkdir ~/wallaroo-tutorial
cd ~/wallaroo-tutorial
```

This will be our base directory in what follows. Download the Wallaroo sources (this will create a subdirectory called `wallaroo-{{ book.wallaroo_version }}`):

```bash
curl -L -o wallaroo-{{ book.wallaroo_version }}.tar.gz '{{ book.bintray_repo_url }}/wallaroo/{{ book.wallaroo_version }}/wallaroo-{{ book.wallaroo_version }}.tar.gz'
mkdir wallaroo-{{ book.wallaroo_version }}
tar -C wallaroo-{{ book.wallaroo_version }} --strip-components=1 -xzf wallaroo-{{ book.wallaroo_version }}.tar.gz
rm wallaroo-{{ book.wallaroo_version }}.tar.gz
cd wallaroo-{{ book.wallaroo_version }}
```

## Install make

```bash
sudo apt-get update
sudo apt-get install -y build-essential
```

## Install libssl-dev

```bash
sudo apt-get install -y libssl-dev
```

### Install GCC 4.7 or Higher

 If you have at least GCC 4.7 installed on your machine, you don't need to do anything. If you have gcc 4.6 or lower, you'll need to upgrade. You can check your `gcc` version by running:

```bash
gcc --version
```

To upgrade to GCC 5 if you don't have at least GCC 4.7:

```bash
sudo apt-get install software-properties-common
```

then

```bash
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
```

then

```bash
sudo apt-get update
sudo apt-get install -y gcc-5 g++-5
```

And set `gcc-5` as the default option:

```bash
sudo update-alternatives --install /usr/bin/gcc gcc \
  /usr/bin/gcc-5 60 --slave /usr/bin/g++ g++ /usr/bin/g++-5
```

## Add ponyc and pony-stable apt-key keyserver

In order to install `ponyc` and `pony-stable` via `apt-get` the following keyserver must be added to the APT key management utility.

```bash
sudo apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys E04F0923 B3B48BDA
```

The following packages need to be installed to allow `apt` to use a repository over HTTPS:

```bash
sudo apt-get install \
     apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common
```

### Installing ponyc

Now you need to install Pony compiler `ponyc`. Run:

```bash
sudo add-apt-repository "deb https://dl.bintray.com/pony-language/ponylang-debian  $(lsb_release -cs) main"
sudo apt-get update
sudo apt-get -V install ponyc={{ book.ponyc_version }}
```

## Installing pony-stable

Next, you need to install `pony-stable`, a Pony dependency management library. Navigate to a directory where you will put the `pony-stable` repo and execute the following commands:

```bash
sudo add-apt-repository "deb https://dl.bintray.com/pony-language/ponylang-debian  $(lsb_release -cs) main"
sudo apt-get update
sudo apt-get -V install pony-stable
```

## Install Compression Development Libraries

Wallaroo's Kakfa support requires `libsnappy` and `liblz` to be installed.

### Xenial Ubuntu and later:

```bash
sudo apt-get install -y libsnappy-dev liblz4-dev
```

### Trusty Ubuntu:

Install libsnappy:

```bash
sudo apt-get install -y libsnappy-dev
```

Trusty Ubuntu has an outdated `liblz4` package. You will need to install from source like this:

```bash
cd ~/
curl -L -o liblz4-1.7.5.tar.gz https://github.com/lz4/lz4/archive/v1.7.5.tar.gz
tar zxvf liblz4-1.7.5.tar.gz
cd lz4-1.7.5
make
sudo make install
```

## Install Python Development Libraries

```bash
sudo apt-get install -y python-dev
```

## Download and configure the Metrics UI

```bash
cd ~/wallaroo-tutorial/wallaroo-{{ book.wallaroo_version }}/
mkdir bin
curl -L -o Wallaroo_Metrics_UI-{{ book.wallaroo_version }}-x86_64.AppImage '{{ book.bintray_repo_url }}/wallaroo/{{ book.wallaroo_version }}/Wallaroo_Metrics_UI-{{ book.wallaroo_version }}-x86_64.AppImage'
chmod +x Wallaroo_Metrics_UI-{{ book.wallaroo_version }}-x86_64.AppImage
./Wallaroo_Metrics_UI-{{ book.wallaroo_version }}-x86_64.AppImage --appimage-extract
mv squashfs-root bin/metrics_ui
sed -i 's/sleep 4/sleep 0/' bin/metrics_ui/AppRun
rm Wallaroo_Metrics_UI-{{ book.wallaroo_version }}-x86_64.AppImage
ln -s metrics_ui/AppRun bin/metrics_reporter_ui
```

## Compiling Machida

Machida is the program that runs Wallaroo Python applications. Machida supports resiliency, but it comes with a performance hit. If you don't need resiliency then we recommend to build Machida without the flag. Here we build Machida once with resiliency and once without resiliency. Change to the Wallaroo directory. If you are using Python 2.7 you should run the following commands:

```bash
cd ~/wallaroo-tutorial/wallaroo-{{ book.wallaroo_version }}/
make build-machida-all resilience=on
cp machida/build/machida bin/machida-resilience
make clean-machida-all
make build-machida-all
cp machida/build/machida bin
cp -r machida/lib/ bin/pylib
```

If you are using Python 3.X you should run the following commands:

```bash
cd ~/wallaroo-tutorial/wallaroo-{{ book.wallaroo_version }}/
make build-machida3-all resilience=on
cp machida3/build/machida3 bin/machida3-resilience
make clean-machida3-all
make build-machida3-all
cp machida3/build/machida3 bin
cp -r machida/lib/ bin/pylib
```

## Compiling Giles Sender, Data Receiver, Cluster Shutdown, and Cluster Shrinker tools

Giles Sender is used to supply data to Wallaroo applications over TCP, and Data Receiver is used as a fast TCP Sink that optionally writes the messages it receives to the console. The two together are useful when developing and testing applications that use TCP Sources and a TCP Sink.

The Cluster Shutdown tool is used to instruct the cluster to shutdown cleanly, clearing away any resilience and recovery files it may have created.

The Cluster Shrinker tool is used to tell a running cluster to reduce the number of workers in the cluster and to query the cluster for information about how many workers are eligible for removal.

To compile all of the tools, change to the root Wallaroo directory:

```bash
cd ~/wallaroo-tutorial/wallaroo-{{ book.wallaroo_version }}/
make build-giles-sender-all build-utils-all
cp utils/data_receiver/data_receiver bin
cp utils/cluster_shrinker/cluster_shrinker bin
cp utils/cluster_shutdown/cluster_shutdown bin
cp giles/sender/sender bin
```

## Set up activate file for setting environment variables

The `activate` file sets up the environment for running Wallaroo examples when it is sourced

The following command copies it into the correct location:

```
cp misc/activate bin/
```

## Register

Register today and receive a Wallaroo T-shirt and a one-hour phone consultation with Sean, our V.P. of Engineering, to discuss your streaming data questions. Not sure if you have a streaming data problem? Not sure how to go about architecting a streaming data system? Looking to improve an existing system? Not sure how Wallaroo can help? Sean has extensive experience and is happy to help you work through your questions.

Please register here: [https://www.wallaroolabs.com/register](https://www.wallaroolabs.com/register).

Your email address will only be used to facilitate the above.

## Conclusion

Awesome! All set. Time to try running your first application.
