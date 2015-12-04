## 1.0.2
 - Using filewatch >= 0.6.7, tail.quit closes files. Only one set of file
 handles are used if run is called more than once.

## 1.0.1
 - Force dependency on filewatch >= 0.6.5 that fixes a sincedb bug
 - Better documentation and error handling regarding the "sincedb_path" parameter
