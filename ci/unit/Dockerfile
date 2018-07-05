ARG ELASTIC_STACK_VERSION
FROM docker.elastic.co/logstash/logstash:$ELASTIC_STACK_VERSION
WORKDIR /usr/share/logstash/logstash-core
RUN cp versions-gem-copy.yml ../logstash-core-plugin-api/versions-gem-copy.yml
COPY --chown=logstash:logstash . /usr/share/plugins/this
WORKDIR /usr/share/plugins/this
ENV PATH=/usr/share/logstash/vendor/jruby/bin:${PATH}
ENV LOGSTASH_SOURCE 1
RUN jruby -S gem install bundler
RUN jruby -S bundle install --jobs=3 --retry=3
RUN jruby -S bundle exec rake vendor
