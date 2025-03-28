module DM-A-SQLConnector
  pom_locations = [
      File.expand_path('../META-INF/fr.distrimind.oss.asqlconnector/asqlconnector/pom.xml', __dir__),
      File.expand_path('../../pom.xml', __dir__),
  ]
  MAVEN_VERSION = File.read(pom_locations.find { |f| File.exist? f })[%r{(?<=<version>)([0-9a-zA-Z.-]*)(?=</version>)}]
  VERSION = Gem::Version.new(MAVEN_VERSION.gsub('-SNAPSHOT', '')).send(:_segments).join('.')
end
