# encoding: utf-8

module RSpec
  module Wait
    module Handler
      def handle_matcher(target, *args, &block)
        # there is a similar patch in the rspec-wait repo since Nov, 19 2017
        # it does not look like the author is interested in the change.
        # - do not use Ruby Timeout
        count = RSpec.configuration.wait_timeout.fdiv(RSpec.configuration.wait_delay).ceil
        failure = nil
        count.times do
          begin
            actual = target.respond_to?(:call) ? target.call : target
            super(actual, *args, &block)
            failure = nil
          rescue RSpec::Expectations::ExpectationNotMetError => failure
            sleep RSpec.configuration.wait_delay
          end
          break if failure.nil?
        end
        raise failure unless failure.nil?
      end
    end

    # From: https://github.com/rspec/rspec-expectations/blob/v3.0.0/lib/rspec/expectations/handler.rb#L44-L63
    class PositiveHandler < RSpec::Expectations::PositiveExpectationHandler
      extend Handler
    end

    # From: https://github.com/rspec/rspec-expectations/blob/v3.0.0/lib/rspec/expectations/handler.rb#L66-L93
    class NegativeHandler < RSpec::Expectations::NegativeExpectationHandler
      extend Handler
    end
  end
end

RSPEC_WAIT_HANDLER_PATCHED = true
