# tair3-client
Java client for distributed K/V storage engine `tair` in version 3. 
Support sync/async invocations.

# Notes
## Expire-TTL
You can specify ttl for every key in second. 
The expire should be a non-negative value.
If the value is greater than or equal to last 30 days (now - 30 * 86400) it will be treated as an absolute time stamp or an offset from now.

Example:
Now is 2019-01-01 00:00:00(GMT+0) and the timestamp is 1546300800.  

Expire | Actually(GMT+0)
--- | ---
1 | 2019-01-01 00:00:01
10 | 2019-01-01 00:00:10
86400 | 2019-01-02 00:00:00
0 | no exprie
-1 | no exprie
-86400 | no expire
1543708800 | 2018-12-02 00:00:00
1546300800 | 2019-01-01 00:00:00

# Changelog
## 3.1.0
* Enlarge expire(TTL) to signed 64 bits. 
