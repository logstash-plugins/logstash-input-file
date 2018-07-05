while true
do
  LOG_AT=ERROR bundle exec rspec -fd --fail-fast --tag ~lsof ./spec || break
done
