warn 'ASQLConnector is only for use with JRuby' if RUBY_ENGINE != 'jruby'

require 'asqlconnector/version'

module Jdbc
  module SQLite3
    def self.driver_jar
      "asqlconnector/asqlconnector-#{DM-A-SQLConnector::MAVEN_VERSION}.jar"
    end

    def self.load_driver(method = :load)
      send method, driver_jar
    end

    def self.driver_name
      'org.sqlite.JDBC'
    end

    if defined?(JRUBY_VERSION) && # enable backwards-compat behavior :
          (Java::JavaLang::Boolean.get_boolean('jdbc.driver.autoload') ||
            Java::JavaLang::Boolean.get_boolean('jdbc.sqlite3.autoload'))
      warn "autoloading JDBC driver on require 'jdbc/sqlite3'" if $VERBOSE
      load_driver :require
    end
  end
end