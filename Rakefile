require 'logstash/devutils/rake'

desc "Compile and put filewatch jar into lib/jars"
task :vendor do
  sh('./gradlew --no-daemon clean jar')
end
