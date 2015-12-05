## 2.1.0
 - Implement new config options: ignore_older and close_older.  When close_older is set, any buffered data will be flushed.
 - Fixes [#81](https://github.com/logstash-plugins/logstash-input-file/issues/81)
 - Fixes [#81](https://github.com/logstash-plugins/logstash-input-file/issues/89)
 - Fixes [#81](https://github.com/logstash-plugins/logstash-input-file/issues/90)

## 2.0.3
 - Implement Stream Identity mapping of codecs: distinct codecs will collect input per stream identity (filename)

## 2.0.2
 - Change LS core dependency version
 - Add CI badge

## 2.0.1
 - Change LS core dependency version

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully,
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

## 1.0.1
 - Force dependency on filewatch >= 0.6.5 that fixes a sincedb bug
 - Better documentation and error handling regarding the "sincedb_path" parameter
