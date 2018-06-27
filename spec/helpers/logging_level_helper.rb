# encoding: utf-8

ENV["LOG_AT"].tap do |level|
  if !level.nil?
    LogStash::Logging::Logger::configure_logging(level)
    LOG_AT_HANDLED = true
  end
end
