require 'logstash/devutils/rake'

desc "Compile and put filewatch jar into lib/jars"
task :vendor do
  sh('./gradlew --no-daemon clean jar')
end

task :test do
  require 'rspec'
  require 'rspec/core/runner'
  Rake::Task[:vendor].invoke
  exit(RSpec::Core::Runner.run(Rake::FileList['spec/**/*_spec.rb']))
end
