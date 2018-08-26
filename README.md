# MemcLoad
A script that concurrently load data to [memcache](https://memcached.org/)
 Data files:
 - https://cloud.mail.ru/public/2hZL/Ko9s8R9TA
 - https://cloud.mail.ru/public/DzSX/oj8RxGX1A
 - https://cloud.mail.ru/public/LoDo/SfsPEzoGc

## Install
0. Before running script you need to install packages:
```
sudo apt install memcached python-protobuf protobuf-compiler
```
1. Create virtual env
```
$ git clone https://github.com/ligain/12_concurrency
$ cd 12_concurrency/
$ python3.5 -m venv .env
$ . .env/bin/activate
```
2. Install requirements
```
$ pip install -r requirements.txt
```
## Run
```
$ python memc_load.py --pattern=/path/to/datafiles/*tsv.gz --dry
```
## Performance delta
**Single thread** version:
```
$ python memc_load.py --pattern=/path/to/datafiles/*tsv.gz --dry
...
[2018.08.26 16:22:54] I Acceptable error rate (0.0). Successfull load

real    31m24.540s
user    16m53.012s
sys 0m52.976s
```
**Multi processing** version:
```
$ python memc_load.py --pattern=/path/to/datafiles/*tsv.gz --dry
...
[MainProcess 2018.08.26 21:16:01] I Acceptable error rate (0.0). Successful load
[MainProcess 2018.08.26 21:16:01] I Script completed in: 599.1524565569998 sec

real	9m59.378s
user	18m12.064s
sys	1m49.956s
```

### Project Goals
The code is written for educational purposes.