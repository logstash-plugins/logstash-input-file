while true
do
  LOG_AT=TRACE bundle exec rspec --fail-fast -fd ./spec || break
done
