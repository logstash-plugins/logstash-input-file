@files=[]

task :default do
  system("rake -T")
end

require "logstash/devutils/rake"

desc "Compile and put filewatch jar into lib/jars"
task :vendor do
  exit(1) unless system './gradlew --no-daemon clean jar'
  puts "-------------------> built filewatch jar via rake"
end
