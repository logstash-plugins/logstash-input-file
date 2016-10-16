## 4.0.0
  - Breaking: `ignore_older` settings is disabled by default. Previously if the file was older than 
    24 hours (the default for ignore_older), it would be ignored. This confused new users a lot, specially 
    when they were reading new files with Logstash (with `start_position => beginning`). This setting also 
    makes it consistent with Filebeat.

## 3.1.2
  - Adjust a few log call levels

## 3.1.1
  - Add host to @metadata

## 3.1.0
  - Breaking: Use native `--path.data` for Logstash 5.0 for sincedb files.

## 3.0.3
  - Relax constraint on logstash-core-plugin-api to >= 1.60 <= 2.99

## 3.0.2
  - relax constrains of `logstash-devutils` see https://github.com/elastic/logstash-devutils/issues/48

## 3.0.1
  - Republish all the gems under jruby.

## 3.0.0
  - Update the plugin to the version 2.0 of the plugin api, this change is required for Logstash 5.0 compatibility. See https://github.com/elastic/logstash/issues/5141

# 2.2.5
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

# 2.2.3
  - New dependency requirements for logstash-core for the 5.0 release

## 2.2.2
 - Fix for: Filewatch library complains if HOME or SINCEDB_PATH variables are unset.
   - [Issue #101](https://github.com/logstash-plugins/logstash-input-file/issues/101)
   - [PR, filewatch 78](https://github.com/jordansissel/ruby-filewatch/pull/78) introduces the fix
   - [Issue, filewatch 76](https://github.com/jordansissel/ruby-filewatch/issues/76)
 - Improve documentation on ignore_older and close_older options [#104](https://github.com/logstash-plugins/logstash-input-file/issues/104) Documentation

## 2.2.1
 - Fix spec failures on CI Linux builds (not seen on local OSX and Linux)

## 2.2.0
 - Use ruby-filewatch 0.8.0, major rework of filewatch. See [Pull Request 74](https://github.com/jordansissel/ruby-filewatch/pull/74)
 - add max_open_files config option, defaults to 4095, the input will process much more than this but have this number of files open at any time - files are closed based on the close_older setting, thereby making others openable.
 - Changes the close_older logic to measure the time since the file was last read internlly rather than using the file stat modified time.
 - Use logstash-codec-multiline 2.0.7, fixes a bug with auto_flush deadlocking when multiple file inputs are defined in the LS config.

## 2.1.3
 - Use ruby-filewatch 0.7.1, re-enable close after file is modified again

## 2.1.2
 - Isolate test helper class in their own namespace

## 2.1.1
 - Correct LS core dependency version

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
