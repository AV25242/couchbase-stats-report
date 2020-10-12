require "couchbase"
require "yaml"
class Storage
  def self.instance
    @instance ||= Storage.new
  end

  def cluster
    @cluster
  end

  def collection
    @collection
  end

  def bucket
    @bucket
  end

  def initialize
    # load configuration
    config = YAML.load(File.read("#{__dir__}/config.yml"))

    # establish database connection
    options = Couchbase::Cluster::ClusterOptions.new
    options.authenticate(config["username"], config["password"])
    @cluster = Couchbase::Cluster.connect(config["address"], options)
    @collection = @cluster.bucket(config["bucket"]).default_collection
    @bucket = config["bucket"]
  end

end
