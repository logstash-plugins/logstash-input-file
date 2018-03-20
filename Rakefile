@files=[]

task :default do
  system("rake -T")
end

require "logstash/devutils/rake"

desc "Compile and put filewatch jar into lib/jars"
task :vendor do
  exit(1) unless system './gradlew clean jar'
  puts "-------------------> built filewatch jar via rake"
end

desc "Run full check with custom Logstash path"
task :custom_ls_check, :ls_dir do |task, args|
  ls_path = args[:ls_dir]
  system(custom_ls_path_shell_script(ls_path))
end

def custom_ls_path_shell_script(path)
  <<TXT
export LOGSTASH_PATH='#{path}'
export LOGSTASH_SOURCE=1
mv './Gemfile.lock' './Gemfile.lock.old'
bundle install
bundle exec rake vendor
bundle exec rspec spec
rm './Gemfile.lock'
mv './Gemfile.lock.old' './Gemfile.lock'
TXT
end
